# 使用 rgb-daemon 进行 L1 和 L2 转账

本文是一份流程手册，说明上层钱包、signer、LN node 如何配合 `rgb-service-daemon` 完成 RGB20 的 L1 direct send 和 L2/LN RGB 转账。

接口字段的完整定义见 [API.zh-CN.md](API.zh-CN.md)。本文重点讲调用顺序、职责边界和当前限制。

## 当前支持范围

当前版本暂时只支持同一个 `rgb-service-daemon` 内部的 account 之间转账：

- L1 direct send：sender account 和 receiver account 都在同一个 daemon 的 `service.data_dir` 下。
- L2/LN RGB：channel funding、commitment、closing、claim 状态都由同一个 daemon 保存和恢复。
- `/v1/transfers/commit` 会把 consignment 直接 staged 到同一个 daemon 里的 receiver account stock。
- 普通 L1 `/v1/recover` 不对客户端开放，daemon 后台 scanner 自动 promote staged stock。

扩展到多个 daemon 很直接，但不是当前默认流程。需要补上的主要是跨 daemon transport：

- sender daemon 导出或发送 consignment
- receiver daemon 接收并 stage consignment
- 双方约定 account/address、asset、txid/outpoint 和确认状态同步
- 多 daemon 间的 auth、anti-replay、重试和幂等语义

也就是说，RGB 状态机本身已经按 sender/receiver、consignment、staged stock 分层；当前只是把 transport 固定在同一个 daemon 内部，后续可以把这层替换成远程 daemon handoff。

## 角色边界

调用 daemon 的最小依赖很少：HTTP + JSON + signer 输出的签名。客户端不需要链接
RGB crate，不需要本地 RGB stock，也不需要跑 zust-console。

真正的额外依赖来自业务本身：

- L1 转账业务需要外部 BTC 钱包能力，用来选 UTXO、构造/签名/广播 PSBT。
- L2/LN 转账业务需要外部 LN node 能力，用来处理 peer、channel、HTLC、commitment 和 LN 签名协议。
- signer 可以是 signer-app，也可以是能产出 daemon 接受的 ECDSA/legacy `bip322` envelope 的钱包组件。

`rgb-service-daemon` 负责：

- 保存 RGB stock、合约、allocation 和 consignment 状态
- 为 L1 anchor PSBT 写入 RGB commitment
- 在 L1 commit 后把 consignment staged 到接收方 account
- 后台扫描 staged stock 并自动 promote
- 为 L2/LN funding、commitment、closing、claim 交易组合 RGB transition
- 保存 LN/RGB channel、compose、payment claim 状态

外部钱包或 LN node 负责：

- 选择 BTC UTXO
- 构造 BTC PSBT 或 unsigned transaction
- 持有 BTC 私钥并完成 BTC 签名
- 广播 BTC 交易
- 创建 signer 签名和 asset spend authorization
- 处理 LN peer、channel、invoice、route、HTLC 和 commitment 签名协议

daemon 不负责：

- BTC 私钥托管
- BTC 交易广播
- LN peer 连接和路由
- BOLT11 invoice 创建或支付
- 普通 L1 `/v1/recover` 客户端入口

## 通用签名要求

除 `GET /v1/tokens/list` 外，HTTP 请求都包在 `SignedRequest<T>` 中：

```json
{
  "payload": {},
  "signature": {}
}
```

会移动或锁定 RGB 资产的请求还需要 `asset_authorization`。这层授权和 HTTP 外层签名不是一回事：

- HTTP 外层签名证明这个 API request 是 account 合法调用方发起的。
- `asset_authorization` 证明用户明确授权某个 asset、amount、purpose、recipient 和 anchor。

常见 `asset_authorization.purpose`：

```text
l1_transfer
l2_reserve
l2_settle
channel_deposit
channel_withdraw
```

## L1 Direct Send 转账

L1 转账是 sender account 直接把 RGB consignment staged 给 receiver account。当前 sender 和 receiver 必须属于同一个 `rgb-service-daemon`。

### 前置条件

1. sender 已经在 daemon 里有该 RGB asset 的可用 allocation。
2. receiver 有同一 daemon 内的 account id，当前约定通常是 BTC address。
3. 外部 BTC 钱包可以选择 sender UTXO，并构造 anchor PSBT。
4. anchor PSBT 能把 RGB amount 分配到 `recipient_vout`。

### 步骤 1：查询资产和余额

```text
GET  /v1/tokens/list
POST /v1/assets/list
POST /v1/balance
POST /v1/balance/breakdown
```

这些查询使用 daemon 已维护的 `account_utxos`，客户端不要再传任意 `tracked_utxos`。

### 步骤 2：钱包构造 unsigned anchor PSBT

外部钱包选择 BTC UTXO，构造 unsigned anchor PSBT，并确定：

- `change_vout`：RGB change 回到 sender 的输出
- `recipient_vout`：RGB amount 给 receiver 的输出
- `fee_rate_sat_vb`：可选 fee rate

BTC 输入、找零、最终 BTC 签名和广播都由外部钱包处理。

### 步骤 3：生成 L1 asset authorization

授权内容至少表达：

```json
{
  "asset_id": "rgb:...",
  "amount": 100,
  "purpose": "l1_transfer",
  "recipient": "receiver-account-id",
  "anchor_psbt": "unsigned-anchor-psbt-hex",
  "expires_at_ms": 1760000300000
}
```

返回的 signature 放入 `asset_authorization.signature`。

### 步骤 4：调用 prepare

```text
POST /v1/transfers/prepare
```

payload 核心字段：

```json
{
  "account_id": "sender-account-id",
  "asset_id": "rgb:...",
  "amount": 100,
  "recipient": "receiver-account-id",
  "fee_rate_sat_vb": 1,
  "unsigned_anchor_psbt": "hex-psbt",
  "change_vout": 1,
  "recipient_vout": 0,
  "asset_authorization": {}
}
```

daemon 会：

- 扣 `transfer_fee`
- 用 sender stock 给 PSBT 写入 RGB commitment
- 保存 prepared transfer record
- 用 `asset_authorization.signature.nonce` 作为 `transfer_id`
- 失败时自动 refund 已扣 RNA

响应：

```json
{
  "transfer_id": "...",
  "operation_id": "prepare_transfer",
  "anchor_psbt": "prepared-hex-psbt"
}
```

### 步骤 5：钱包签名并广播 BTC anchor transaction

外部钱包拿 `anchor_psbt` 完成 BTC 签名并广播。广播成功后得到：

- `txid`
- receiver outpoint 信息
- signed anchor PSBT，可选

### 步骤 6：调用 commit

```text
POST /v1/transfers/commit
```

payload 核心字段：

```json
{
  "account_id": "sender-account-id",
  "transfer_id": "...",
  "txid": "bitcoin-txid",
  "signed_anchor_psbt": "optional-signed-psbt",
  "utxos": [
    {
      "outpoint": "bitcoin-txid:0",
      "address": "receiver-btc-address",
      "confirmed": false
    }
  ],
  "asset_authorization": {}
}
```

daemon 会：

- 找回 prepare 阶段保存的 RGB fascia
- stage sender fascia
- 构造 RGB consignment
- 将 consignment staged 到同一 daemon 内 receiver account 的 stock
- 将 receiver outpoint 写入 receiver 的 `account_utxos`
- 返回 `committed`

响应：

```json
{
  "transfer_id": "...",
  "operation_id": "bitcoin-txid",
  "status": "committed"
}
```

### 步骤 7：等待 daemon 后台 scanner 自动 promote

daemon 定时扫描：

```text
kv/rgb_pending_ops + kv/rgb_pending_status（按 account_id 前缀隔离）
```

满足链上条件后自动 promote。客户端不要调用 `/v1/recover`，普通 L1 recover HTTP API 已经移除。

用余额接口确认最终状态：

```text
POST /v1/balance
POST /v1/balance/breakdown
```

### 取消 L1 转账

如果 anchor transaction 还没有广播，可以调用：

```text
POST /v1/transfers/cancel
```

已经广播进入链上确认流程后，不要依赖 cancel 静默撤销。

## L2/LN RGB 转账

这里的 L2 指 RGB-aware Lightning 流程。`rgb-service-daemon` 提供 RGB 状态组合和恢复接口，但不是完整 LN node。

LN node 或 `ln-rgb-lightning` 仍然负责：

- peer 连接
- channel open/close 协议
- commitment 轮换
- HTLC、invoice、route、payment preimage
- BTC/LN 交易签名和广播

daemon 负责：

- 把 RGB state 绑定到 LN funding output
- 为 commitment/closing/claim transaction 组合 RGB transition
- 保存 channel funding ref、compose record、payment claim record
- 在重启后通过 `/v1/ln/recover` 返回 LN/RGB 状态

当前 L2/LN RGB 状态也默认属于同一个 daemon。也就是 channel funding、commitment compose、closing compose、recover 都在同一个 daemon 的 KV 和 stock 中完成。

### L2 流程总览

```text
1. LN node 准备 channel funding PSBT。
2. 调用 POST /v1/ln/channels/open/prepare 绑定 RGB funding。
3. 钱包/LN node 签名并广播 funding transaction。
4. 后续每次 commitment 更新，调用 POST /v1/ln/commitments/compose。
5. LN node 按正常 LN 协议交换/签署 commitment。
6. RGB payment settle 后，调用 POST /v1/ln/payments/claim 记录 payment claim。
7. cooperative/force close 时，调用 POST /v1/ln/closing/compose。
8. 需要扫 commitment/HTLC output 时，调用 POST /v1/ln/onchain-claims/compose。
9. daemon 或 LN node 重启后，调用 POST /v1/ln/recover 恢复 LN/RGB 状态。
```

### 步骤 1：打开 RGB-funded channel

LN node 构造 channel funding PSBT，要求：

- 有 funding output
- 有 OP_RETURN carrier output
- 确定 `funding_vout`
- 确定 `change_vout`
- 确定 `funding_rgb`
- `funding_rgb == to_local_rgb + to_remote_rgb`

生成 `asset_authorization`：

```json
{
  "asset_id": "rgb:...",
  "amount": 1000,
  "purpose": "channel_deposit",
  "recipient": "channel-id",
  "anchor_psbt": "funding-psbt-hex",
  "expires_at_ms": 1760000300000
}
```

调用：

```text
POST /v1/ln/channels/open/prepare
```

payload 核心字段：

```json
{
  "account_id": "account-id",
  "channel_id": "channel-id",
  "contract_id": "rgb:...",
  "unsigned_anchor_psbt": "funding-psbt-hex",
  "change_vout": 1,
  "funding_vout": 0,
  "funding_rgb": 1000,
  "to_local_rgb": 600,
  "to_remote_rgb": 400,
  "asset_authorization": {}
}
```

daemon 返回 `funding_ref`、`funding_outpoint` 和写入 RGB commitment 后的 `anchor_psbt`。之后 LN node 或钱包签名并广播 funding transaction。

`funding_ref` 要保存；如果上层丢失，可以用：

```text
POST /v1/ln/channels/funding-ref
```

按 `channel_id` 找回。

### 步骤 2：L2 channel 内 RGB 转账

L2 转账体现为 commitment transaction 的 RGB distribution 更新。

每次 LN commitment 更新时，上层 LN/RGB 状态机计算：

- `to_local_rgb`
- `to_local_vout`
- `to_remote_rgb`
- `to_remote_vout`
- 可选 HTLC RGB outputs：`htlcs`
- `change_vout`
- `unsigned_tx_hex`

当前 daemon 对 LN compose 强校验：

- `asset_authorization.asset_id == contract_id`
- `asset_authorization.amount == to_local_rgb + to_remote_rgb + sum(htlcs.amount_rgb)`

调用：

```text
POST /v1/ln/commitments/compose
```

daemon 返回带 RGB state 的 `tx_hex` 和 `rgb_state_ref`。之后由 LN node 按正常 LN 协议处理 commitment 签名、撤销旧 state、交换新 state。daemon 不替 LN node 做这些协议动作。

### 步骤 3：记录 L2 payment claim

当上层确认某个 RGB payment 已 settle，可以调用：

```text
POST /v1/ln/payments/claim
```

payload：

```json
{
  "account_id": "account-id",
  "channel_id": "channel-id",
  "payment_hash": "hex-payment-hash",
  "contract_id": "rgb:...",
  "amount_msat": 100000,
  "rgb_amount": 100
}
```

daemon 会记录 payment，并返回 `settled`。

### 步骤 4：关闭 channel

cooperative close 或 force close 需要把 channel 内 RGB amount 分配到 closing transaction outputs。

常见 authorization purpose：

```text
channel_withdraw
```

调用：

```text
POST /v1/ln/closing/compose
```

daemon 返回 `tx_hex` 和 `rgb_state_ref`。LN node 继续负责签名、广播和正常 LN close 协议。

### 步骤 5：on-chain claim

如果需要扫 commitment output 或 HTLC output，LN node 构造 claim transaction，然后调用：

```text
POST /v1/ln/onchain-claims/compose
```

daemon 会在已保存的 compose record 中查找 `commitment_txid + vout` 对应的 RGB assignment：

- 找到：把 RGB amount 转移到 claim transaction 的唯一输出 `vout = 0`，返回新的 `tx_hex` 和 `rgb_state_ref`。
- 找不到：原样返回 `unsigned_tx_hex`，`rgb_state_ref = null`。

### 步骤 6：恢复 L2/LN RGB 状态

daemon 或上层 LN node 重启后，调用：

```text
POST /v1/ln/recover
```

恢复某个 channel：

```json
{
  "account_id": "account-id",
  "channel_id": "channel-id"
}
```

恢复 account 下所有 channel：

```json
{
  "account_id": "account-id",
  "channel_id": null
}
```

daemon 返回：

- `channels`：已保存的 RGB channel funding 状态
- `composes`：已保存的 commitment/closing/claim compose 状态

注意：`/v1/ln/recover` 是 LN/RGB 状态恢复接口；普通 L1 `/v1/recover` 不存在。

## 多 daemon 扩展方向

当前同 daemon direct send 的关键简化是：`/v1/transfers/commit` 可以直接访问 receiver stock，把 consignment staged 到 receiver account。

扩展到多个 daemon 时，流程可以自然拆成：

```text
sender daemon:
  prepare -> commit -> build/export consignment

transport:
  deliver consignment + txid + receiver outpoint metadata

receiver daemon:
  verify/import/stage consignment -> scanner promote -> balance visible
```

需要新增或恢复的能力会集中在 transport/API 层，而不是改 RGB transfer 核心：

- remote receiver discovery
- consignment push/pull
- receiver daemon import/stage endpoint
- idempotency key 和 retry
- cross-daemon auth 和 anti-replay
- failure/refund/timeout 策略

所以“多个 daemon”是协议和 API 编排扩展，不是资产模型重写。

## 常见错误和排查

### `/v1/recover` 返回 404

这是预期行为。普通 L1 recover 是 daemon 后台 scanner 的职责，不暴露 HTTP API。L2/LN 使用 `/v1/ln/recover`。

### `RGB LN funding PSBT must include an OP_RETURN carrier output`

LN funding PSBT 没有 OP_RETURN carrier output。打开 RGB-funded channel 时必须提供。

### `to_local_rgb + to_remote_rgb must equal funding_rgb`

channel open 的 RGB 初始分配不守恒。

### `asset authorization amount ... does not match LN RGB amount`

LN commitment/closing compose 中，`asset_authorization.amount` 必须等于所有 RGB output assignment 的总和。

## 最小流程对照

L1 direct send：

```text
/v1/transfers/prepare
wallet signs and broadcasts anchor tx
/v1/transfers/commit
daemon background scanner promotes staged stock
/v1/balance or /v1/balance/breakdown
```

L2/LN RGB：

```text
/v1/ln/channels/open/prepare
LN wallet signs and broadcasts funding tx
/v1/ln/commitments/compose
LN protocol signs/exchanges commitment state
/v1/ln/payments/claim
/v1/ln/closing/compose
/v1/ln/onchain-claims/compose
/v1/ln/recover
```
