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
├── kv/                         # service KV: accounts, prepared transfers
└── accounts/
    └── <account_id>/
        ├── rgb-stock/          # RGB stock / contracts / asset state
        └── rgb-stock_pending/  # pending promotion markers
```

Accounts are API/KV concepts, not static config sections. Requests carry
`account_id`, and the service uses it as the namespace for RGB state.

## Public HTTP API

Current public daemon routes:

```text
POST /v1/assets/issue
POST /v1/assets/list
POST /v1/balance
POST /v1/balance/breakdown
POST /v1/invoices/create
POST /v1/transfers/prepare
POST /v1/transfers/commit
POST /v1/transfers/cancel
POST /v1/pending/list
POST /v1/recover
POST /v1/test/rgb
```

Import/export and raw consignment access are intentionally not exposed in the
public daemon API right now. In this model, RGB binary state stays inside the
service.

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
6. RGB Service marks the RGB operation pending and later /v1/recover promotes it.
```

The external caller never receives fascia or consignment data in this public
flow. Those are service-internal state artifacts.
