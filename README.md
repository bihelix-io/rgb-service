# rgb-service

BiHelix RGB service workspace.

This repository contains the RGB contract and asset libraries extracted from
`btc-local-wallet`. It intentionally excludes Lightning Network node code.

中文 API 说明见 [docs/API.zh-CN.md](docs/API.zh-CN.md)。
L1/L2 转账流程手册见 [docs/TRANSFER.zh-CN.md](docs/TRANSFER.zh-CN.md)。
`1.0.10-prod` 升级和 wallet-v2 质押迁移手册见
[docs/UPGRADE-1.0.10-prod.zh-CN.md](docs/UPGRADE-1.0.10-prod.zh-CN.md)。

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
- transfer prepare/commit/cancel
- service-owned pending staging and background promotion
- admin-triggered RGB test workflows

HTTP support is available behind the `axum` feature. Mutating endpoints accept
signed requests. Operations that move or lock RGB value require an additional
asset spend authorization, intended to be signed by the user wallet.
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
```



Daemon logs are written to:

```text
<service.data_dir>/rgb-service.log
```

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
|-- kv/                         # single Fjall DB: all account and wallet state
`-- rgb-service.log             # runtime log; not migration state
```

Accounts are API/KV concepts, not static config sections. Requests carry
`account_id`, and the service uses it as the key prefix for RGB stock and
pending state inside the shared database. On the first upgraded startup the
daemon imports old `accounts/` and `legacy-wallets/` data into `kv/`
idempotently. After the migration markers are persisted, those old directories
are no longer read during normal operation and may be archived or removed.

## Public HTTP API 中文说明

当前 daemon 公开的 HTTP API 只面向 RGB 合约和资产服务。外部钱包仍然负责
BTC UTXO 选择、BTC 私钥签名、PSBT 最终签名和交易广播。RGB Service 负责
托管 RGB stock、合约状态、资产分配和 pending/recovery 状态推进。
pending/recovery 是 daemon 内部状态机和后台 scanner 的职责，不作为客户端
HTTP 能力暴露。

Public daemon routes:

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

Import/export、raw fascia 暂时不作为 public API 暴露。consignment 不作为任意文件下载接口暴露，
RGB 转账采用 direct send：`/v1/transfers/commit` 会根据 prepare 阶段保存的接收方 account_id，
直接把 consignment 暂存到接收方 service inbox。

### 通用请求格式

除公开 catalog 外，HTTP 请求都使用 `SignedRequest<T>` 包装：

```json
{
  "payload": {
    "account_id": "alice"
  },
  "signature": {
    "signer_id": "alice-wallet",
    "public_key": "...",
    "scheme": "ecdsa",
    "nonce": "random-nonce",
    "timestamp_ms": 1760000000000,
    "signature": "..."
  }
}
```

`signature.scheme` 可选值：

```text
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
  "recipient": "receiver account_id",
  "anchor_psbt": "optional psbt hex/base64 according to caller convention",
  "expires_at_ms": 1760000000000,
  "signature": {
    "signer_id": "alice-wallet",
    "public_key": "...",
    "scheme": "ecdsa",
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
  "account_id": "alice"
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
  ],
  "utxo_assets": {
    "txid:vout": [
      {
        "asset_id": "...",
        "outpoint": "txid:vout",
        "amount": 1000,
        "layer": "l1",
        "status": "available"
      }
    ]
  }
}
```

说明：daemon 维护 account/address 下的 UTXO 集合。查询时会返回 `utxo_assets`，
并自动移除没有 RGB 资产的 UTXO。

### `GET /v1/tokens/list`

用途：公开列出 daemon 已知的所有 RGB20 合约和资产元信息。

说明：这是公开 catalog 接口，不需要签名、不需要 account_id、不扣 RNA。

响应：

```json
{
  "contracts": [
    {
      "contract_id": "...",
      "schema": "rgb20",
      "asset_id": "...",
      "ticker": "RGB",
      "name": "RGB Token",
      "precision": 8
    }
  ],
  "assets": [
    {
      "asset_id": "...",
      "contract_id": "...",
      "ticker": "RGB",
      "name": "RGB Token",
      "precision": 8
    }
  ]
}
```

### `POST /v1/balance`

用途：查询某个 RGB asset 的汇总余额。

权限：`read_assets`

请求 payload：

```json
{
  "account_id": "alice",
  "asset_id": "...",
  "scope": "all"
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
  "asset_id": "..."
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
  "utxo_assets": {
    "txid:vout": [
      {
        "asset_id": "...",
        "outpoint": "txid:vout",
        "amount": 1000,
        "layer": "l1",
        "status": "available"
      }
    ]
  },
  "pending_ops": []
}
```

`layer` 可选值：`l1`, `l2`。

`status` 可选值：`available`, `reserved`, `pending_in`, `pending_out`, `settling`, `locked`。

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
  "recipient": "bob",
  "fee_rate_sat_vb": 2,
  "unsigned_anchor_psbt": "...",
  "change_vout": 1,
  "recipient_vout": 0,
  "asset_authorization": {
    "asset_id": "...",
    "amount": 1000,
    "purpose": "l1_transfer",
    "recipient": "bob",
    "anchor_psbt": "...",
    "expires_at_ms": 1760000000000,
    "signature": {
      "signer_id": "alice-wallet",
      "public_key": "...",
      "scheme": "ecdsa",
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
  "utxos": [
    {
      "outpoint": "bitcoin-txid:0",
      "address": "bc1...",
      "confirmed": true
    }
  ],
  "asset_authorization": {
    "asset_id": "...",
    "amount": 1000,
    "purpose": "l1_transfer",
    "recipient": "bob",
    "anchor_psbt": "...",
    "expires_at_ms": 1760000000000,
    "signature": {
      "signer_id": "alice-wallet",
      "public_key": "...",
      "scheme": "ecdsa",
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
  "status": "committed"
}
```

说明：`signed_anchor_psbt` 是可选字段。当前核心确认依据是 `txid` 和 service 内部保存的 pending RGB 状态。
`commit` 会根据 prepare 阶段保存的 `recipient` account_id 构造 consignment，并直接暂存到接收方 stock pending。
之后由 recovery/confirmation 流程推进最终状态。

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
8. RGB Service 构造 consignment，并直接暂存到 `recipient` 对应的接收方 account。
9. RGB Service 根据 transfer_id / txid 进入 pending/recovery 流程。
10. daemon 后台 scanner 自动扫描 staged stock 并推进最终状态；客户端没有 recover API。
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
6. RGB Service builds the consignment and stages it directly for the recipient account.
7. The daemon background scanner promotes staged RGB stock when chain conditions are met.
```

The external caller never receives raw fascia in this public flow. Consignment
is delivered internally by `/v1/transfers/commit`. The current direct-send flow
is for accounts on the same `rgb-service-daemon`; extending it to multiple
daemons mainly requires a remote consignment transport and receiver-side staging
API.
