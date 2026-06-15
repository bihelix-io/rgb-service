# RGB Service API 中文说明

本文档描述 `rgb-service-daemon` 当前公开给客户端的 HTTP API 边界。

## 服务边界

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
- signer App 生命周期

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
    "scheme": "bip322",
    "nonce": "...",
    "timestamp_ms": 1760000000000,
    "signature": "..."
  }
}
```

当前 daemon 为了兼容 signer-app 的 legacy `bip322` envelope，会校验：

- `signer_id == account_id`
- `signature` 非空
- `nonce` 非空
- `timestamp_ms` 在允许窗口内

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

- `accounts/<account_id>/rgb-stock_pending`
- 已满足条件的 staged stock
- 可 promote 的 confirmed RGB 状态

scanner 日志示例：

```text
rgb pending recovery scanner enabled interval_secs=60
rgb pending recovery scan accounts=2 scanned=2 promoted=2 pending=0 skipped=0 failed=0
```

如果需要历史 backfill 或运维修复，使用 SSH 到 daemon 所在机器上运行 console 运维函数，例如：

```zust
rgb::scan_utxos("bc1...")
```

这个函数直接维护 daemon 本地 `account_utxos`，不是公开 HTTP 权限入口。

## RGB-aware LN 路由

```text
POST /v1/ln/channels/open/prepare
POST /v1/ln/channels/funding-ref
POST /v1/ln/commitments/compose
POST /v1/ln/closing/compose
POST /v1/ln/onchain-claims/compose
POST /v1/ln/payments/claim
POST /v1/ln/recover
```

这些接口是 `ln-rgb-lightning` 使用的 service-owned state transition API。它们需要 signed request，涉及移动或锁定 RGB value 的路径还需要 `asset_authorization`。

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
