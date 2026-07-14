# RGB Service API 中文说明

本文档描述 `rgb-service-daemon` 当前公开给客户端的 HTTP API 边界。

L1/L2 转账流程手册见 [TRANSFER.zh-CN.md](TRANSFER.zh-CN.md)。

当前 L1 direct send 和 L2/LN RGB 状态默认只支持同一个
`rgb-service-daemon` 内部的 account/channel；扩展到多个 daemon 时，主要增加
consignment transport、receiver-side staging 和跨 daemon auth/幂等处理。

## 服务边界

调用 `rgb-service-daemon` 本身不需要依赖 Rust SDK、RGB 本地库、本地 RGB stock
或 zust-console。普通客户端只需要：

- 能发 HTTP request
- 能编码/解析 JSON
- 能拿到 signer 生成的 request signature
- 在花费 RGB 资产时，能拿到 signer 生成的 `asset_authorization`

不同业务场景会有自己的外部能力，但那不是调用 daemon 的 SDK 依赖：

- 只查公开 catalog：只需要 HTTP GET。
- 查余额、发行、转账：需要 signed request。
- L1 转账：外部 BTC 钱包需要会选 UTXO、构造/签名/广播 PSBT。
- L2/LN 转账：外部 LN node 需要会处理 peer、channel、HTLC、commitment 和签名协议。

RGB Service 负责托管服务端 RGB 状态：

- RGB stock、合约和资产状态
- account 下的 RGB allocation
- direct send transfer prepare/commit/cancel
- pending/recovery 状态的 daemon 内部推进
- RGB-aware LN 状态转换接口
- 内部 RNA credit 余额和扣费

客户端或外部钱包仍然负责：

- BTC UTXO 选择
- BTC 私钥和 PSBT 签名
- BTC 交易广播
- 用户资产授权签名

普通 L1 RGB pending/recovery 不作为客户端能力暴露。daemon 后台 scanner 会自动扫描 staged stock 并推进状态，所以客户端不能调用 `/v1/pending/list` 或 `/v1/recover`。

## 公开路由

```text
POST /v1/rna/balance
POST /v1/assets/issue
POST /v1/assets/list
GET  /v1/tokens/list
POST /v1/balance
POST /v1/balance/breakdown
POST /v1/transfers/prepare
POST /v1/transfers/commit
POST /v1/transfers/cancel
POST /v1/ln/channels/open/prepare
POST /v1/ln/channels/funding-ref
POST /v1/ln/commitments/compose
POST /v1/ln/closing/compose
POST /v1/ln/onchain-claims/compose
POST /v1/ln/payments/claim
POST /v1/ln/recover
POST /v1/test/rgb
```

不公开：

```text
POST /v1/pending/list
POST /v1/recover
raw fascia download
raw consignment download
RGB stock import/export
BTC private key custody
BTC transaction broadcast
```

## 签名模型

除 `GET /v1/tokens/list` 外，请求都使用 `SignedRequest<T>`：

```json
{
  "payload": {
    "account_id": "bc1..."
  },
  "signature": {
    "signer_id": "bc1...",
    "public_key": "...",
    "scheme": "ecdsa",
    "nonce": "...",
    "timestamp_ms": 1760000000000,
    "signature": "..."
  }
}
```

ECDSA 请求会按 daemon 的域分隔消息做真实签名验证。Schnorr 和 Ed25519 当前不接受。

移动或锁定 RGB value 的接口还需要 `asset_authorization`，由用户钱包或 signer 对 asset、amount、purpose、recipient、anchor_psbt、expires_at_ms 做授权。

## RNA

`RNA` 是 daemon 内部 service credit，不是 RGB 资产。新 profile 会获得配置的 `new_profile_grant`。

```text
POST /v1/rna/balance
```

用途：

- 查询调用方 RNA 余额
- 返回当前 fee policy
- 如果 profile 不存在则创建 profile 并发放初始 RNA

该接口需要 signed request，但不扣 RNA。

## 资产发行

```text
POST /v1/assets/issue
```

用途：

- 发行 RGB20 资产
- 将发行 allocation 绑定到指定 BTC outpoint
- 把相关 UTXO 写入 daemon 维护的 `account_utxos`

请求 payload 核心字段：

```json
{
  "account_id": "bc1...",
  "ticker": "ASSET",
  "name": "Asset Name",
  "precision": 0,
  "supply": 1000000,
  "allocation_outpoint": "txid:vout",
  "utxos": [
    {
      "outpoint": "txid:vout",
      "address": "bc1...",
      "confirmed": true
    }
  ]
}
```

发行失败时，如果 RNA 已经扣除，daemon 会按失败路径自动 refund。

## 资产和余额查询

```text
POST /v1/assets/list
GET  /v1/tokens/list
POST /v1/balance
POST /v1/balance/breakdown
```

说明：

- `/v1/tokens/list` 是公开 catalog，不需要签名，不扣 RNA。
- 其他查询需要签名，并按 query fee 扣 RNA。
- daemon 使用本地 `account_utxos` 查询 RGB allocation，不接受客户端传入任意 `tracked_utxos`。
- 查询过程中发现某个已知 UTXO 不再持有 RGB allocation，会从 daemon 状态中清理。

## L1 Direct Send Transfer

### Prepare

```text
POST /v1/transfers/prepare
```

用途：

- 外部钱包提供 unsigned anchor PSBT
- daemon 写入 RGB commitment
- daemon 保存 transfer state
- 返回可交给钱包签名和广播的 anchor PSBT

该接口需要 outer signed request 和 `asset_authorization`。

### Commit

```text
POST /v1/transfers/commit
```

用途：

- 钱包广播 anchor transaction 后提交 `txid`
- daemon 根据 `transfer_id` 找回 prepare 阶段状态
- daemon 构造 consignment
- daemon 把 consignment 暂存到接收方 account
- daemon 后台 scanner 后续自动 promote staged stock

commit payload 可以附带广播后产生的新 UTXO 信息，daemon 会维护到 `account_utxos`。

### Cancel

```text
POST /v1/transfers/cancel
```

用途：

- 取消尚未完成且仍可安全回滚的 transfer
- 已广播进入链上确认流程的交易不能靠 cancel 静默撤销

## L1 Recovery

普通客户端没有 L1 recovery API。

daemon 启动后会按配置 `recovery_scan_interval_secs` 定时扫描：

- 统一 Fjall 数据库中的 `rgb_pending_ops` / `rgb_pending_status`
- 已满足条件的账户级 pending operation
- 可 promote 的 confirmed RGB 状态

scanner 日志示例：

```text
rgb pending recovery scanner enabled interval_secs=60
rgb pending recovery scan accounts=2 scanned=2 promoted=2 pending=0 skipped=0 failed=0
```

如果需要历史 backfill 或运维修复，先停止 daemon，再使用 daemon 自己的离线修复命令：

```bash
target/release/rgb-service repair-daemon-account-utxos \
  ./rgb-service.toml <account_id> <txid:vout>...
```

`zust-console` 不再直接打开或修改 daemon 的 Fjall 数据库；该命令也不是公开 HTTP 权限入口。

## RGB-aware LN 路由

这组接口由 `rgb-service-daemon` 直接提供，是 daemon 公开 HTTP API 的一部分，不是 zust-console 本地能力。它们用于 `ln-rgb-lightning` 这类上层 LN/RGB 状态机，把 RGB 状态绑定到 LN channel funding、commitment、closing、claim 和 payment claim 流程。

```text
POST /v1/ln/channels/open/prepare
POST /v1/ln/channels/funding-ref
POST /v1/ln/commitments/compose
POST /v1/ln/closing/compose
POST /v1/ln/onchain-claims/compose
POST /v1/ln/payments/claim
POST /v1/ln/recover
```

所有 LN 路由都需要 `SignedRequest<T>`。其中会移动或锁定 RGB value 的接口还需要 `asset_authorization`：

- `/v1/ln/channels/open/prepare`
- `/v1/ln/commitments/compose`
- `/v1/ln/closing/compose`

以下接口不额外要求 `asset_authorization`；它们要么查询/恢复 daemon 已有状态，要么基于 daemon 已保存的 RGB assignment 继续 compose：

- `/v1/ln/channels/funding-ref`
- `/v1/ln/onchain-claims/compose`
- `/v1/ln/payments/claim`
- `/v1/ln/recover`

daemon 会把 LN 相关状态写入本地 KV：

- `ln_channels`：channel funding 和 opening RGB state
- `ln_composes`：commitment/closing/claim compose 结果
- `ln_payments`：payment claim 记录

LN `asset_authorization` 当前强校验：

- `asset_id` 必须等于请求里的 `contract_id`
- `amount` 必须等于本次 LN/RGB 状态转换的 RGB 总量
- channel open 的总量是 `funding_rgb`
- commitment/closing compose 的总量是 `to_local_rgb + to_remote_rgb + htlcs.amount_rgb...`

### `POST /v1/ln/channels/open/prepare`

用途：

- 为 RGB-aware LN channel open 准备 funding anchor PSBT
- 检查 `funding_rgb == to_local_rgb + to_remote_rgb`
- 要求 funding PSBT 包含 OP_RETURN carrier output
- 将 `funding_rgb` 绑定到 `funding_vout`
- 保存 channel funding 记录，返回 `funding_ref`

权限 purpose：

```text
ln_channel_open_prepare
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id",
  "contract_id": "rgb:...",
  "unsigned_anchor_psbt": "hex-psbt",
  "change_vout": 1,
  "funding_vout": 0,
  "funding_rgb": 1000,
  "to_local_rgb": 600,
  "to_remote_rgb": 400,
  "asset_authorization": {
    "asset_id": "rgb:...",
    "amount": 1000,
    "purpose": "channel_deposit",
    "recipient": "channel-id",
    "anchor_psbt": "hex-psbt",
    "expires_at_ms": 1760000300000,
    "signature": {}
  }
}
```

响应：

```json
{
  "funding_ref": {
    "transfer_id": "ln-open:nonce-or-operation-id",
    "operation_id": "nonce-or-operation-id",
    "channel_id": "channel-id"
  },
  "operation_id": "nonce-or-operation-id",
  "funding_outpoint": "txid:0",
  "anchor_psbt": "hex-psbt"
}
```

### `POST /v1/ln/channels/funding-ref`

用途：

- 根据 `channel_id` 查询 daemon 已保存的 RGB funding reference
- 给后续 commitment/closing compose 传入 `funding_ref`

权限 purpose：

```text
ln_channel_funding_ref
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id"
}
```

响应：

```json
{
  "funding_ref": {
    "transfer_id": "ln-open:...",
    "operation_id": "...",
    "channel_id": "channel-id"
  }
}
```

没有记录时：

```json
{
  "funding_ref": null
}
```

### `POST /v1/ln/commitments/compose`

用途：

- 为 LN commitment transaction 组合 RGB transition
- 把 RGB amount 分配到本地输出、远端输出和 HTLC 输出
- 返回带 RGB state 的 transaction hex 和 `rgb_state_ref`
- 保存 compose 记录，供 on-chain claim 和 recover 使用

权限 purpose：

```text
ln_commitment_compose
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id",
  "funding_ref": {
    "transfer_id": "ln-open:...",
    "operation_id": "...",
    "channel_id": "channel-id"
  },
  "unsigned_tx_hex": "bitcoin-tx-hex",
  "funding_outpoint": "funding-txid:vout",
  "contract_id": "rgb:...",
  "to_local_rgb": 600,
  "to_local_vout": 0,
  "to_remote_rgb": 300,
  "to_remote_vout": 1,
  "htlcs": [
    {
      "vout": 2,
      "amount_rgb": 100
    }
  ],
  "change_vout": 3,
  "asset_authorization": {
    "asset_id": "rgb:...",
    "amount": 1000,
    "purpose": "channel_deposit",
    "recipient": "channel-id",
    "anchor_psbt": "bitcoin-tx-hex",
    "expires_at_ms": 1760000300000,
    "signature": {}
  }
}
```

响应：

```json
{
  "operation_id": "nonce-or-operation-id",
  "tx_hex": "bitcoin-tx-hex",
  "rgb_state_ref": "ln:channel-id:operation-id"
}
```

说明：

- `to_local_rgb > 0` 时必须提供 `to_local_vout`。
- `to_remote_rgb > 0` 时必须提供 `to_remote_vout`。
- `htlcs` 为空数组表示没有 RGB HTLC output。

### `POST /v1/ln/closing/compose`

用途：

- 为 LN cooperative/force closing transaction 组合 RGB transition
- 把 channel 内 RGB amount 分配到 closing transaction 的本地/远端输出
- 保存 compose 记录，供 recover 使用

权限 purpose：

```text
ln_closing_compose
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id",
  "funding_ref": {
    "transfer_id": "ln-open:...",
    "operation_id": "...",
    "channel_id": "channel-id"
  },
  "unsigned_tx_hex": "bitcoin-tx-hex",
  "funding_outpoint": "funding-txid:vout",
  "contract_id": "rgb:...",
  "to_local_rgb": 600,
  "to_local_vout": 0,
  "to_remote_rgb": 400,
  "to_remote_vout": 1,
  "change_vout": 2,
  "asset_authorization": {
    "asset_id": "rgb:...",
    "amount": 1000,
    "purpose": "channel_withdraw",
    "recipient": "bc1...",
    "anchor_psbt": "bitcoin-tx-hex",
    "expires_at_ms": 1760000300000,
    "signature": {}
  }
}
```

响应同 commitment compose：

```json
{
  "operation_id": "nonce-or-operation-id",
  "tx_hex": "bitcoin-tx-hex",
  "rgb_state_ref": "ln:channel-id:operation-id"
}
```

### `POST /v1/ln/onchain-claims/compose`

用途：

- 为 commitment output 或 HTLC output 的 on-chain claim transaction 组合 RGB transition
- daemon 会根据 `commitment_txid + vout` 在已保存 compose 记录中找原 RGB output assignment
- 找到后，把该 RGB amount 转移到 claim transaction 的唯一输出 `vout = 0`
- 找不到 assignment 时原样返回 unsigned tx，并且 `rgb_state_ref = null`

权限 purpose：

```text
ln_onchain_claim_compose
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id",
  "commitment_txid": "commitment-txid",
  "vout": 2,
  "unsigned_tx_hex": "claim-tx-hex"
}
```

响应：

```json
{
  "operation_id": "ln-claim:commitment-txid:2:claim-txid",
  "tx_hex": "claim-tx-hex",
  "rgb_state_ref": "ln:channel-id:ln-claim:..."
}
```

说明：如果找到了 RGB assignment，claim transaction 必须只有一个输出；daemon 会把 RGB amount 放到该输出。

### `POST /v1/ln/payments/claim`

用途：

- 记录 LN payment 对应的 RGB claim 状态
- 以 `payment_hash` 生成 operation id
- 当前实现会把该 payment 标记为 `settled`

权限 purpose：

```text
ln_payment_claim
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id",
  "payment_hash": "hex-payment-hash",
  "contract_id": "rgb:...",
  "amount_msat": 100000,
  "rgb_amount": 100
}
```

响应：

```json
{
  "operation_id": "ln-payment:hex-payment-hash",
  "status": "settled",
  "rgb_state_ref": "ln:channel-id:ln-payment:hex-payment-hash"
}
```

`channel_id` 可以为 `null`；这种情况下 `rgb_state_ref` 也会是 `null`。

### `POST /v1/ln/recover`

用途：

- 从 daemon 本地 KV 读取 LN channel、compose 记录
- 返回某个 account 下全部 channel 的 LN/RGB 状态
- 如果传 `channel_id`，只返回该 channel 的状态
- 给 LN/RGB 上层服务恢复内存状态用

权限 purpose：

```text
ln_recover
```

请求 payload：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id"
}
```

恢复 account 下全部 channel：

```json
{
  "account_id": "bc1...",
  "channel_id": null
}
```

响应：

```json
{
  "account_id": "bc1...",
  "channel_id": "channel-id",
  "channels": [
    {
      "channel_id": "channel-id",
      "contract_id": "rgb:...",
      "funding_outpoint": "funding-txid:vout",
      "funding_rgb": 1000,
      "to_local_rgb": 600,
      "to_remote_rgb": 400,
      "funding_ref": {
        "transfer_id": "ln-open:...",
        "operation_id": "...",
        "channel_id": "channel-id"
      },
      "created_at_ms": 1760000000000
    }
  ],
  "composes": [
    {
      "channel_id": "channel-id",
      "operation_id": "...",
      "route": "/v1/ln/commitments/compose",
      "contract_id": "rgb:...",
      "txid": "txid",
      "tx_hex": "bitcoin-tx-hex",
      "rgb_state_ref": "ln:channel-id:operation-id",
      "fascia_len": 1234,
      "funding_ref": {
        "transfer_id": "ln-open:...",
        "operation_id": "...",
        "channel_id": "channel-id"
      },
      "created_at_ms": 1760000000000
    }
  ]
}
```

普通 L1 的 `/v1/recover` 已经移除；`/v1/ln/recover` 只用于 LN/RGB 服务状态恢复，不是给客户端推进普通 L1 pending stock 的入口。

## 测试路由

```text
POST /v1/test/rgb
```

用途：

- 受控环境下跑 RGB lifecycle 测试
- 验证 daemon、stock、issue、transfer 等路径

该接口不应该暴露给普通公网调用方，应该只给 admin/test signer 权限。

## 推荐 L1 转账流程

```text
1. 外部 BTC 钱包选择 UTXO，构造 unsigned anchor PSBT。
2. 钱包对 HTTP SignedRequest 签名。
3. 钱包对 asset_authorization 签名，授权 asset_id、amount、purpose、recipient。
4. 钱包调用 POST /v1/transfers/prepare。
5. RGB Service 写入 RGB commitment，保存 sender 状态，返回 anchor PSBT。
6. 外部 BTC 钱包签名并广播 anchor PSBT。
7. 钱包拿到 txid 后调用 POST /v1/transfers/commit。
8. RGB Service 构造 consignment，并暂存到 recipient 对应的 account。
9. daemon 后台 scanner 自动扫描 staged stock 并推进最终状态。
```

这个流程中，客户端不需要也不能调用 recover。
