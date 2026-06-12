# rgb-service

BiHelix RGB service workspace.

This repository contains the RGB contract and asset libraries extracted from
`btc-local-wallet`. It intentionally excludes Lightning Network node code.

## Crates

- `rgb-aluvm`
- `rgb-api`
- `rgb-coloring`
- `rgb-consensus`
- `rgb-ops`
- `rgb-service-api`
- `rgb-service-daemon`
- `rgb-service-local`
- `rgb-schemas`

Internal RGB workspace crates are kept where required:

- `rgb-api/cli`
- `rgb-api/psbt`
- `rgb-ops/invoice`

## Service Boundary

`rgb-service-api` defines the public service boundary for:

- asset and contract management
- balance and allocation queries
- RGB invoice creation
- transfer prepare/commit/cancel
- pending operation and recovery workflows
- admin-triggered RGB test workflows

HTTP support is available behind the `axum` feature. Mutating endpoints accept
signed requests. Operations that move or lock RGB value require an additional
asset spend authorization, intended to be signed by the user wallet or signer.
The `/v1/test/rgb` endpoint is intended for privileged integration testing and
requires a signed request with the test permission.

## Start The Daemon

Create a config file. A local example is available at `examples/rgb-service.toml`:

```toml
[service]
bind = "127.0.0.1:8787"
network = "regtest"
data_dir = "/tmp/bihelix-rgb-service"
esplora_url = "http://127.0.0.1:3002"

[iroh]
secret_key_hex = ""
```



Daemon logs are written to:

```text
<service.data_dir>/rgb-service.log
```

When `[iroh]` is configured, the daemon derives the iroh `node_id` from
`secret_key_hex` and writes both `node_id` and the current endpoint address to
this log file at startup.

Run the HTTP service:

```bash
cargo run -p rgb-service-daemon -- examples/rgb-service.toml
```

The daemon fails loudly when required config is missing. It does not infer
`network`, `data_dir`, or `esplora_url` from legacy wallet files.

## Data Layout

The daemon owns RGB contract and asset state under `service.data_dir`.

```text
<data_dir>/
|-- kv/                         # service KV: accounts, prepared transfers
`-- accounts/
    `-- <account_id>/
        |-- rgb-stock/          # RGB stock / contracts / asset state
        `-- rgb-stock_pending/  # pending promotion markers
```

Accounts are API/KV concepts, not static config sections. Requests carry
`account_id`, and the service uses it as the namespace for RGB state.

## Public HTTP API 中文说明

当前 daemon 公开的 HTTP API 只面向 RGB 合约和资产服务。外部钱包仍然负责
BTC UTXO 选择、BTC 私钥签名、PSBT 最终签名和交易广播。RGB Service 负责
托管 RGB stock、合约状态、资产分配、pending operation 和 recovery 状态。

Public daemon routes:

```text
POST /v1/iroh-nodes/register
POST /v1/iroh-nodes/lookup
POST /v1/assets/issue
POST /v1/assets/list
POST /v1/balance
POST /v1/balance/breakdown
POST /v1/invoices/create
POST /v1/transfers/prepare
POST /v1/transfers/commit
POST /v1/consignments/send
POST /v1/consignments/receive
POST /v1/transfers/cancel
POST /v1/pending/list
POST /v1/recover
POST /v1/test/rgb
```

Import/export、raw fascia 暂时不作为 public API 暴露。consignment 不作为任意文件下载接口暴露，
但 RGB 转账必须支持受控传输：发送方通过 `/v1/consignments/send` 交付 consignment，
接收方通过 `/v1/consignments/receive` 接收外部 consignment。当前支持 service 内部 inbox 和显式 inline 传输；inline 只用于 SDK、调试或跨服务调用方自己负责转发的场景。

### 通用请求格式

所有 HTTP 请求都使用 `SignedRequest<T>` 包装：

```json
{
  "payload": {
    "account_id": "alice"
  },
  "signature": {
    "signer_id": "alice-wallet",
    "public_key": "...",
    "scheme": "bip322",
    "nonce": "random-nonce",
    "timestamp_ms": 1760000000000,
    "signature": "..."
  }
}
```

`signature.scheme` 可选值：

```text
bip322
schnorr
ecdsa
ed25519
```

权限由服务端 `AuthVerifier` 判断。签名必须绑定请求 payload、account、nonce、时间戳和权限语义。
缺少签名、签名错误、权限不匹配、account 不匹配都应该直接失败，不做静默 fallback。

### 资产花费授权

会移动或锁定 RGB 资产的接口还需要 `asset_authorization`。目前包括：

- `POST /v1/transfers/prepare`
- `POST /v1/transfers/commit`

字段结构：

```json
{
  "asset_id": "rgb:...",
  "amount": 1000,
  "purpose": "l1_transfer",
  "recipient": "rgb invoice or recipient seal",
  "anchor_psbt": "optional psbt hex/base64 according to caller convention",
  "expires_at_ms": 1760000000000,
  "signature": {
    "signer_id": "alice-wallet",
    "public_key": "...",
    "scheme": "bip322",
    "nonce": "asset-spend-nonce",
    "timestamp_ms": 1760000000000,
    "signature": "..."
  }
}
```

`purpose` 可选值：

```text
l1_transfer
l2_reserve
l2_settle
channel_deposit
channel_withdraw
```

这层签名表示用户钱包明确授权某个资产、金额、用途和接收方。HTTP 外层签名表示
这个 API 请求本身由该 account 的合法调用方发起。两层签名不要合并，因为它们表达的
安全语义不同。

## Route Details

### `POST /v1/iroh-nodes/register`

用途：注册 BTC 地址到 iroh node 的绑定，用于后续通过 BTC 地址找到接收方 iroh 节点。

权限：`register_iroh_node`

请求 payload：

```json
{
  "account_id": "alice",
  "btc_address": "bcrt1...",
  "iroh_node_id": "iroh-node-id",
  "label": "alice mobile signer"
}
```

响应：

```json
{
  "binding": {
    "account_id": "alice",
    "btc_address": "bcrt1...",
    "iroh_node_id": "iroh-node-id",
    "label": "alice mobile signer",
    "updated_at_ms": 1760000000000
  }
}
```

说明：BTC 地址是目录 key。注册必须签名，避免第三方替地址写入错误 node_id。

### `POST /v1/iroh-nodes/lookup`

用途：调用方用自己的 `account_id + 签名` 进行访问控制，然后根据任意 BTC 地址查询已注册的 iroh node_id。

权限：`lookup_iroh_node`

说明：`account_id` 是查询调用方身份，不要求等于被查询地址的 owner。这样可以查别人的地址，同时要求签名以降低公开目录被滥用和 DoS 的风险。

请求 payload：

```json
{
  "account_id": "caller-account",
  "btc_address": "bcrt1..."
}
```

响应：

```json
{
  "binding": {
    "account_id": "alice",
    "btc_address": "bcrt1...",
    "iroh_node_id": "iroh-node-id",
    "label": "alice mobile signer",
    "updated_at_ms": 1760000000000
  }
}
```

如果没有绑定：

```json
{
  "binding": null
}
```

### `POST /v1/assets/issue`

用途：发行 RGB20 资产，并把初始供应量分配到指定 Bitcoin outpoint。

权限：`issue_asset`

请求 payload：

```json
{
  "account_id": "alice",
  "ticker": "USDA",
  "name": "Alice USD Asset",
  "precision": 2,
  "supply": 1000000,
  "allocation_outpoint": "txid:vout"
}
```

响应：

```json
{
  "contract_id": "...",
  "asset_id": "...",
  "allocation_outpoint": "txid:vout"
}
```

说明：`allocation_outpoint` 必须由外部 BTC 钱包控制。RGB Service 不持有 BTC 私钥。

### `POST /v1/assets/list`

用途：列出 account 命名空间下已知 RGB 资产。

权限：`read_assets`

请求 payload：

```json
{
  "account_id": "alice",
  "tracked_utxos": [
    {
      "outpoint": "txid:vout",
      "address": "bcrt1...",
      "confirmed": true
    }
  ]
}
```

响应：

```json
{
  "assets": [
    {
      "asset_id": "...",
      "contract_id": "...",
      "ticker": "USDA",
      "name": "Alice USD Asset",
      "precision": 2
    }
  ]
}
```

说明：`tracked_utxos` 由外部钱包提供，用于限定或辅助查询钱包正在跟踪的 UTXO。

### `POST /v1/balance`

用途：查询某个 RGB asset 的汇总余额。

权限：`read_assets`

请求 payload：

```json
{
  "account_id": "alice",
  "asset_id": "...",
  "scope": "all",
  "tracked_utxos": []
}
```

`scope` 可选值：

```text
all
l1
l2
account
{ "channel": "channel-id" }
```

响应：

```json
{
  "asset_id": "...",
  "total": 1000,
  "l1_available": 1000,
  "l1_pending_in": 0,
  "l1_pending_out": 0,
  "l2_available": 0,
  "l2_locked": 0,
  "l2_pending_in": 0,
  "l2_pending_out": 0,
  "reserved": 0,
  "settling": 0
}
```

说明：余额模型同时预留 L1/L2 字段。即使当前没有接入 LN node，API 也可以表达
未来 L2 reserve、settle、locked 等状态。

### `POST /v1/balance/breakdown`

用途：查询余额明细，包括 allocation 和 pending operation。

权限：`read_assets`

请求 payload：

```json
{
  "account_id": "alice",
  "asset_id": "...",
  "tracked_utxos": []
}
```

响应：

```json
{
  "summary": {
    "asset_id": "...",
    "total": 1000,
    "l1_available": 1000,
    "l1_pending_in": 0,
    "l1_pending_out": 0,
    "l2_available": 0,
    "l2_locked": 0,
    "l2_pending_in": 0,
    "l2_pending_out": 0,
    "reserved": 0,
    "settling": 0
  },
  "allocations": [
    {
      "asset_id": "...",
      "outpoint": "txid:vout",
      "amount": 1000,
      "layer": "l1",
      "status": "available"
    }
  ],
  "pending_ops": []
}
```

`layer` 可选值：`l1`, `l2`。

`status` 可选值：`available`, `reserved`, `pending_in`, `pending_out`, `settling`, `locked`。

### `POST /v1/invoices/create`

用途：创建 RGB 收款 invoice。

权限：`create_invoice`

请求 payload：

```json
{
  "account_id": "alice",
  "asset_id": "...",
  "amount": 1000,
  "expiry_seconds": 3600,
  "transport_hints": ["iroh:..."]
}
```

响应：

```json
{
  "invoice_id": "...",
  "invoice": "rgb:...",
  "blinded_seal": "...",
  "expires_at_ms": 1760000000000
}
```

说明：`amount` 可以为空，用于生成不固定金额 invoice。`transport_hints` 用于放入
接收方希望使用的传输方式，例如未来的 iroh endpoint。

### `POST /v1/transfers/prepare`

用途：准备 RGB 转账。调用方传入未签名 BTC anchor PSBT，RGB Service 生成 RGB commitment，
返回更新后的 anchor PSBT，并把内部 fascia/consignment 状态保存到 service KV。

权限：`prepare_transfer`

额外要求：`asset_authorization`

请求 payload：

```json
{
  "account_id": "alice",
  "asset_id": "...",
  "amount": 1000,
  "recipient": "rgb invoice or blinded seal",
  "fee_rate_sat_vb": 2,
  "unsigned_anchor_psbt": "...",
  "change_vout": 1,
  "recipient_vout": 0,
  "asset_authorization": {
    "asset_id": "...",
    "amount": 1000,
    "purpose": "l1_transfer",
    "recipient": "rgb invoice or blinded seal",
    "anchor_psbt": "...",
    "expires_at_ms": 1760000000000,
    "signature": {
      "signer_id": "alice-wallet",
      "public_key": "...",
      "scheme": "bip322",
      "nonce": "asset-spend-nonce",
      "timestamp_ms": 1760000000000,
      "signature": "..."
    }
  }
}
```

响应：

```json
{
  "transfer_id": "...",
  "operation_id": "...",
  "anchor_psbt": "..."
}
```

说明：响应不返回 fascia 或 consignment。外部钱包拿到 `anchor_psbt` 后，只负责 BTC 签名和广播。

### `POST /v1/transfers/commit`

用途：外部钱包完成 BTC 签名和广播后，通知 RGB Service 这笔 RGB 转账对应的 anchor txid。
RGB Service 根据 `transfer_id` 找回内部保存的 RGB 状态，并把 operation 标记为 pending/committed 流程的一部分。

权限：`commit_transfer`

额外要求：`asset_authorization`

请求 payload：

```json
{
  "account_id": "alice",
  "transfer_id": "...",
  "txid": "bitcoin-txid",
  "signed_anchor_psbt": "...",
  "asset_authorization": {
    "asset_id": "...",
    "amount": 1000,
    "purpose": "l1_transfer",
    "recipient": "rgb invoice or blinded seal",
    "anchor_psbt": "...",
    "expires_at_ms": 1760000000000,
    "signature": {
      "signer_id": "alice-wallet",
      "public_key": "...",
      "scheme": "bip322",
      "nonce": "asset-spend-nonce",
      "timestamp_ms": 1760000000000,
      "signature": "..."
    }
  }
}
```

响应：

```json
{
  "transfer_id": "...",
  "operation_id": "...",
  "status": "pending"
}
```

说明：`signed_anchor_psbt` 是可选字段。当前核心确认依据是 `txid` 和 service 内部保存的 pending RGB 状态。

### `POST /v1/consignments/send`

用途：根据 `transfer_id`、anchor `txid` 和 service 内部保存的 sender fascia 构建 RGB transfer consignment，
并把 consignment 交付给接收方。

权限：`send_consignment`

额外要求：`asset_authorization`

请求 payload：

```json
{
  "account_id": "alice",
  "transfer_id": "...",
  "asset_id": "...",
  "txid": "bitcoin-txid",
  "recipient_vout": 0,
  "transport": {
    "kind": "service_inbox",
    "account_id": "bob"
  },
  "asset_authorization": {
    "asset_id": "...",
    "amount": 1000,
    "purpose": "l1_transfer",
    "recipient": "rgb invoice or blinded seal",
    "anchor_psbt": "...",
    "expires_at_ms": 1760000000000,
    "signature": {
      "signer_id": "alice-wallet",
      "public_key": "...",
      "scheme": "bip322",
      "nonce": "asset-spend-nonce",
      "timestamp_ms": 1760000000000,
      "signature": "..."
    }
  }
}
```

`transport` 当前可选：

```json
{ "kind": "service_inbox", "account_id": "bob" }
```

```json
{ "kind": "iroh", "node_id": "receiver-node-id", "topic": "optional-account-id", "timeout_ms": 30000 }
```

```json
{ "kind": "inline" }
```

响应，service inbox：

```json
{
  "transfer_id": "...",
  "operation_id": "bitcoin-txid",
  "status": "pending",
  "delivery": {
    "kind": "service_inbox",
    "account_id": "bob"
  }
}
```

响应，iroh：

```json
{
  "transfer_id": "...",
  "operation_id": "bitcoin-txid",
  "status": "pending",
  "delivery": {
    "kind": "iroh",
    "node_id": "receiver-node-id",
    "delivery_id": "iroh:transfer-id:bitcoin-txid",
    "status": "pending"
  }
}
```

响应，inline：

```json
{
  "transfer_id": "...",
  "operation_id": "bitcoin-txid",
  "status": "pending",
  "delivery": {
    "kind": "inline",
    "consignment_hex": "..."
  }
}
```

说明：`service_inbox` 用于同一个 RGB Service 内部账户之间交付，不把 consignment 返回给调用方。
`iroh` 用于点对点 consignment 传输。daemon 配置只需要 `[iroh].secret_key_hex`，`node_id` 由 secret key 自动派生；如果没有配置 `[iroh]`，请求 iroh transport 会直接失败，不会 fallback 到 inline。调用方只需要传接收方 `node_id`，daemon 会使用 iroh discovery/relay/address lookup 解析连接地址；如果解析不到就失败。接收端必须从 assignment `topic` 读取 account_id，topic 为空会拒收。
`inline` 会返回编码后的 consignment，适合 SDK 或跨服务调用方自己转发；它不是任意历史 consignment 下载接口，
只能基于当前 `transfer_id` 和已授权资产花费生成。

### `POST /v1/consignments/receive`

用途：接收外部 RGB consignment，把它放入接收方 account 的 receiver pending 状态，之后通过 `/v1/recover`
在 anchor tx 满足条件后推进到正式 RGB stock。

权限：`receive_consignment`

请求 payload：

```json
{
  "account_id": "bob",
  "txid": "bitcoin-txid",
  "consignment_hex": "...",
  "source_transfer_id": "optional-transfer-id"
}
```

响应：

```json
{
  "operation_id": "bitcoin-txid",
  "status": "pending"
}
```

说明：这个接口是“接收转账证明”，不是通用导入 RGB stock。服务会把 consignment 暂存为 receiver pending，
再由 recovery/confirmation 流程验证和接收。

### `POST /v1/transfers/cancel`

用途：取消尚未完成的 transfer reservation/pending 准备状态。

权限：`cancel_transfer`

请求 payload：

```json
{
  "account_id": "alice",
  "transfer_id": "...",
  "reason": "user_cancelled"
}
```

响应：

```json
{
  "transfer_id": "...",
  "status": "cancelled"
}
```

说明：只能取消仍可安全回滚的状态。已经广播并进入链上确认流程的交易不能靠 API 静默撤销。

### `POST /v1/pending/list`

用途：列出 account 下还在 pending、settling、recovery required 等状态的 operation。

权限：`manage_pending`

请求 payload：

```json
{
  "account_id": "alice"
}
```

响应：

```json
{
  "pending": [
    {
      "operation_id": "...",
      "asset_id": "...",
      "amount": 1000,
      "status": "pending",
      "layer": "l1",
      "related_txid": "bitcoin-txid",
      "related_l2_ref": null
    }
  ]
}
```

`operation.status` 可选值：

```text
prepared
reserved
pending
committed
settled
cancelled
failed
recovery_required
```

### `POST /v1/recover`

用途：扫描并恢复 pending operation，把已经满足条件的 RGB 状态推进到 settled/committed，或者报告失败项。

权限：`recover`

请求 payload：

```json
{
  "account_id": "alice",
  "operation_id": null
}
```

响应：

```json
{
  "scanned": 1,
  "recovered": 1,
  "failed": 0,
  "actions": [
    {
      "operation_id": "...",
      "action": "promote_pending",
      "message": "operation promoted"
    }
  ]
}
```

说明：`operation_id` 为空表示扫描该 account 下所有 pending operation；非空表示只恢复指定 operation。

### `POST /v1/test/rgb`

用途：触发服务端 RGB 集成测试路径，用于本地、CI 或受控测试环境验证完整 RGB20 生命周期。

权限：`run_test`

请求 payload：

```json
{
  "account_id": "alice",
  "scenario": "full_rgb20_lifecycle"
}
```

响应：

```json
{
  "scenario": "full_rgb20_lifecycle",
  "passed": true,
  "steps": [
    {
      "name": "issue_asset",
      "passed": true,
      "message": null
    }
  ]
}
```

说明：这个接口不应该暴露给普通公网调用方，建议只给 admin/test signer 权限。

## Recommended L1 Transfer Flow

```text
1. 外部 BTC 钱包选择 UTXO，构造 unsigned anchor PSBT。
2. 钱包对 HTTP SignedRequest 签名。
3. 钱包对 asset_authorization 签名，明确授权 asset_id、amount、purpose、recipient。
4. 钱包调用 POST /v1/transfers/prepare。
5. RGB Service 写入 RGB commitment，内部保存 sender fascia 状态，返回 anchor_psbt。
6. 外部 BTC 钱包签名并广播 anchor_psbt。
7. 钱包拿到 txid 后调用 POST /v1/transfers/commit。
8. 钱包或业务服务调用 POST /v1/consignments/send，把 consignment 交付给接收方。
9. 外部接收方如果不在同一个 service inbox 内，通过 POST /v1/consignments/receive 提交 consignment。
10. RGB Service 根据 transfer_id / txid 找回内部 RGB 状态并进入 pending/recovery 流程。
11. 调用方或后台任务调用 POST /v1/recover 推进最终状态。
```

这个流程里，BTC 私钥、BTC 签名和广播都在外部钱包；RGB 合约状态、资产分配、pending 状态
都在 RGB Service。这样 L1 钱包、未来 L2/LN sidecar、以及其他业务入口可以共享同一个 RGB 服务。

## Signing Flow

Every HTTP request is wrapped in `SignedRequest<T>`. Mutating asset operations
also require `AssetSpendAuthorization`.

For an L1 transfer, the external wallet remains responsible for BTC ownership:

```text
1. External wallet builds an unsigned anchor PSBT and chooses recipient/change vouts.
2. Wallet calls /v1/transfers/prepare with signed request + asset spend authorization.
3. RGB Service updates the PSBT with RGB commitments and stores internal fascia in KV.
4. Wallet signs and broadcasts the returned PSBT.
5. Wallet calls /v1/transfers/commit with transfer_id + txid.
6. Wallet or business service calls /v1/consignments/send to deliver the transfer consignment.
7. Receiver calls /v1/consignments/receive when the consignment comes from another service.
8. RGB Service marks the RGB operation pending and later /v1/recover promotes it.
```

The external caller never receives raw fascia in this public flow. Consignment
is transmitted only through controlled send/receive endpoints.
