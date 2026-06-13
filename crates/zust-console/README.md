# zust-console

`zust-console` is the command-line REPL/admin console for the RGB service stack.
It loads a `.zs` startup script, registers native Zust modules, and then keeps a
REPL open for debugging and operations.

```bash
cargo run -p zust-console -- crates/zust-console/start.zs
cargo run -p zust-console -- -e 'ln::status()'
```

Runtime config is written by the startup script:

```zs
root::add("local/rgb-service", "http://3.1.207.115:8091");
root::add("local/btc-addr", "bc1q5nqave6m673q4g704r4ppzwacur3d67amp3f8c");
root::add("local/signer-node", "417c33530ab6097e5e2538ffd16e833ee4337318015f201452a540c49adb4158");
```

`local/btc-addr` is the default L1 BTC/RGB account. `local/signer-node` is the
signer App iroh node id. `local/rgb-service` is the RGB daemon HTTP URL.

Useful REPL commands:

```text
:load <path>
:reset
:quit
```

## Signer App Protocol

Signer requests are sent over iroh using Dynamic msgpack:

```text
[path, body]
```

The console currently calls these signer paths:

```text
/v1/signer/request-signature
/v1/signer/asset-authorization
/v1/signer/psbt/sign
/v1/signer/address/batch
```

Address batch request:

```json
{
  "account_id": "bc1q...",
  "network": "bitcoin",
  "purpose": "low_water_refill",
  "count": 20,
  "timestamp_ms": 1780000000000
}
```

Address batch response:

```json
{
  "account_id": "bc1q...",
  "network": "bitcoin",
  "count": 20,
  "addresses": [
    {
      "address": "bc1...",
      "derivation_path": "m/84'/0'/0'/0/12",
      "index": 12,
      "script_pubkey": "0014..."
    }
  ]
}
```

PSBT signing request:

```json
{
  "account_id": "bc1q...",
  "ident": "alice",
  "domain": "bihelix-btc-wallet",
  "psbt": "...",
  "signing_accounts": [
    {
      "kind": "derived",
      "ident": "alice",
      "account_id": "bc1q...",
      "address": "bc1q...",
      "source": "local_address_pool",
      "derivation_path": "m/84'/0'/0'/0/12",
      "index": 12,
      "script_pubkey": "0014...",
      "signer_response": {}
    }
  ],
  "policy": {},
  "expires_at_ms": 1780000300000,
  "timestamp_ms": 1780000000000
}
```

For `ident == ""`, `signing_accounts[0].kind` is `"default"` and `address` is
`local/btc-addr`.

## Local State

Fjall local store lives under `.zust-console/local-store`. It is local console
state, not RGB stock and not the LN channel database.

Stored partitions:

| Partition | Purpose |
| --- | --- |
| `ident_btc_address` | `ident -> btc_address` mapping for assigned deposit addresses. |
| `wallet_btc_address` | Default wallet address cache. |
| `btc_deposit_records` | Scanned BTC deposit records keyed by `txid:vout`. |
| `btc_address_pool_available` | Unused signer-derived address pool records. |
| `btc_address_pool_used` | Assigned signer-derived address records with signer metadata. |
| `ident_ln_invoice` | Reserved legacy/local LN invoice mapping. |
| `ln_payment_hash_ident` | Reserved legacy/local LN payment mapping. |

LN hot wallet state is separate:

```text
.zust-console/ln-node.json
.zust-console/lightning/
.zust-console/lightning/ldk/
```

## Module Overview

Native modules currently registered:

```text
env
bdk
btc
rgb
ln
ln_rgb
```

All native functions return a Dynamic value. Most object responses include
`ok: true`; errors are returned as `{ ok: false, error: "..." }`.

## env

| Function | Description |
| --- | --- |
| `env::get(name)` | Returns an environment variable as string, or `""`. |

## bdk

The `bdk` module is a thin compatibility/admin surface for external BTC wallet
coordination. It does not own the production signer key.

| Function | Description |
| --- | --- |
| `bdk::wallet({ network?, data_dir? })` | Returns an external BTC wallet descriptor object. Defaults `network` to `regtest`. |
| `bdk::sign_request(payload)` | Builds a signed body and sends `/v1/signer/request-signature`. |
| `bdk::asset_authorization({ asset_id, amount, purpose?, recipient?, anchor_psbt?, expires_at_ms? })` | Requests `/v1/signer/asset-authorization`. |
| `bdk::external_anchor({ unsigned_anchor_psbt?, signed_anchor_psbt?, txid?, change_vout?, recipient_vout? })` | Packages external BTC anchor fields for RGB prepare/commit flows. |

## btc

BTC address-selecting calls take an `ident` string:

```text
""      -> default local/btc-addr
"alice" -> signer-derived address assigned by btc::get_deposit_address("alice")
```

Current BTC functions:

| Function | Description |
| --- | --- |
| `btc::get_wallet_address(ident)` | Returns the selected BTC address and account metadata. |
| `btc::balance(ident)` | Reads selected address balance and UTXOs from Esplora. |
| `btc::status(ident)` | Returns selected BTC balance plus `btc::assets(ident)` result. |
| `btc::utxos(ident)` | Lists selected address UTXOs from Esplora. |
| `btc::assets(ident)` | Sends selected address tracked UTXOs to RGB daemon `/v1/assets/list`. |
| `btc::get_deposit_address(ident)` | Returns existing assigned address or consumes one local pooled signer-derived address. `""` returns default account. |
| `btc::lookup_address_ident(address)` | Local reverse lookup from assigned address to ident. |
| `btc::scan_deposits(ident)` | Scans selected default/ident address through Esplora and persists deposit records. |
| `btc::scan_ident_deposits(ident)` | Legacy alias-style ident scan; requires non-empty ident. |
| `btc::address_pool_status()` | Returns available/used address pool counts and available address records. |
| `btc::refill_address_pool(count)` | Requests `count` new addresses from signer App and stores them in the available pool. |
| `btc::sign_psbt(psbt, ident)` | Sends PSBT and selected signing account metadata to `/v1/signer/psbt/sign`. |
| `btc::broadcast(tx_hex)` | Broadcasts raw transaction hex through Esplora `/tx`. |
| `btc::tx_status(txid)` | Reads Esplora transaction confirmation status. |

Examples:

```zs
btc::get_wallet_address("")
btc::get_deposit_address("alice")
btc::balance("alice")
btc::utxos("alice")
btc::scan_deposits("alice")
btc::sign_psbt(psbt, "alice")
```

The current scanner is small-scale: it scans one selected address per call. It
does not implement a production block indexer.

## rgb

The `rgb` module talks to `local/rgb-service`. Most calls wrap the payload with
`signed_request(...)`, so the signer App is used automatically unless the input
already contains a `signature` field.

| Function | Route / Description |
| --- | --- |
| `rgb::signed(payload)` | Returns `{ payload, signature }` for an arbitrary payload. |
| `rgb::rna_balance()` | POST `/v1/rna/balance`. No arguments. |
| `rgb::request_signature(payload)` | Direct signer request through `/v1/signer/request-signature`. |
| `rgb::asset_authorization({ asset_id, amount, purpose?, recipient?, anchor_psbt?, expires_at_ms? })` | Direct signer asset authorization request. |
| `rgb::request({ route, ...payload })` | POST an arbitrary RGB daemon route. |
| `rgb::issue(payload)` | POST `/v1/assets/issue`. |
| `rgb::assets()` | POST `/v1/assets/list` for default account. |
| `rgb::token_list()` | GET `/v1/tokens/list`; public token/contract list. |
| `rgb::balance(payload)` | POST `/v1/balance`. Adds `scope: "all"` and tracked default UTXOs if omitted. |
| `rgb::balance_breakdown(payload)` | POST `/v1/balance/breakdown`. Adds tracked default UTXOs if omitted. |
| `rgb::prepare_transfer(payload)` | POST `/v1/transfers/prepare`. |
| `rgb::commit_transfer(payload)` | POST `/v1/transfers/commit`. |
| `rgb::pending(payload)` | POST `/v1/pending/list`. |
| `rgb::recover(payload)` | POST `/v1/recover`. |
| `rgb::test(payload)` | POST `/v1/test/rgb`. |

Examples:

```zs
rgb::rna_balance()
rgb::token_list()
rgb::balance({ asset_id: "..." })
rgb::prepare_transfer({
  asset_id: "...",
  amount: 100,
  recipient: "bc1q...",
})
```

RGB consignment storage/transport is handled by `rgb-service-daemon`, not by
local console RGB stock.

## ln

The `ln` module controls the local hot-wallet LN node. It uses
`LnRgbBtcLnBackend` with `ln-rgb-lightning`.

Create/load the hot wallet node:

```zs
let lightning = ln::node_address({
  network: "bitcoin",
  low_water_sats: 100000,
  data_dir: ".zust-console/lightning",
  ldk_data_dir: ".zust-console/lightning/ldk",
  listen: "0.0.0.0:9736",
  chain_source: {
    kind: "esplora",
    url: "https://blockstream.info/api",
  },
});
root::add("local/lightning", lightning);
ln::start()
```

Current LN functions:

| Function | Description |
| --- | --- |
| `ln::node_address(config)` | Creates/loads local LN hot wallet, persists mnemonic, returns redacted node object. |
| `ln::start()` | Starts the LN runtime from `local/lightning`. |
| `ln::stop()` | Stops the LN runtime. |
| `ln::status()` | Runtime status, balances, peer/channel counts. |
| `ln::scanner_status()` | BTC address pool/scanner state. |
| `ln::spawn_scanner({ interval_ms? })` | Starts the address-pool refill scanner thread. |
| `ln::events()` | Drains up to 100 pending LN debug events. |
| `ln::get_node_id()` | Returns LN node id. |
| `ln::get_addr()` | Returns LN hot-wallet L1 deposit address. |
| `ln::get_peers()` | Returns connected/persisted peers. |
| `ln::get_channels()` | Returns channel snapshots. |
| `ln::connect({ node_id, address, persist? })` | Connects to a peer. `address` is LDK socket address string. |
| `ln::open_channel({ node_id, address, amount_sats, push_msat? })` | Opens a BTC LN channel. |
| `ln::close_channel({ channel_id, counterparty_node_id|node_id, force?, reason? })` | Closes a BTC LN channel. |
| `ln::invoice({ amount_msat, description?, expiry_secs? })` | Creates a BOLT11 invoice. |
| `ln::pay(invoice)` | Pays a BOLT11 invoice string. |
| `ln::token_list()` | Calls RGB daemon token list through the LN RGB service client. |
| `ln::rgb_channel_context({ contract_id, amount, outbound? })` | Builds RGB channel context for a contract/amount. |

Examples:

```zs
ln::status()
ln::get_addr()
ln::invoice({ amount_msat: 1000, description: "test" })
ln::connect({ node_id: "...", address: "1.2.3.4:9735" })
```

## ln_rgb

`ln_rgb` is the RGB-over-Lightning facade. Basic BTC LN functions mostly forward
to the same runtime as `ln`.

| Function | Description |
| --- | --- |
| `ln_rgb::start()` | Same as `ln::start()`. |
| `ln_rgb::stop()` | Same as `ln::stop()`. |
| `ln_rgb::status()` | Same as `ln::status()`. |
| `ln_rgb::get_node_id()` | Returns LN node id. |
| `ln_rgb::get_addr()` | Returns LN hot-wallet L1 deposit address. |
| `ln_rgb::amount()` | Spendable onchain + LN balance snapshot in sats. |
| `ln_rgb::btc_amount()` | Spendable onchain balance snapshot in sats. |
| `ln_rgb::ln_amount()` | Lightning balance snapshot in sats. |
| `ln_rgb::get_peers()` | Same as `ln::get_peers()`. |
| `ln_rgb::get_channels()` | Same as `ln::get_channels()`. |
| `ln_rgb::connect(payload)` | Same as `ln::connect(payload)`. |
| `ln_rgb::open_channel(payload)` | Same as `ln::open_channel(payload)`. |
| `ln_rgb::close_channel(payload)` | Same as `ln::close_channel(payload)`. |
| `ln_rgb::invoice(payload)` | Same as `ln::invoice(payload)`. |
| `ln_rgb::pay(invoice)` | Same as `ln::pay(invoice)`. |
| `ln_rgb::events()` | Same as `ln::events()`. |
| `ln_rgb::get_info()` | RGB LN runtime info, balances, peer/channel counts. |
| `ln_rgb::rgb_channel_context({ contract_id|asset_id, amount|rgb_amount|funding_rgb, outbound? })` | Same context helper as `ln::rgb_channel_context`. |
| `ln_rgb::open_rgb_channel({ node_id, address?, capacity_sat|amount_sats, push_msat?, user_channel_id?, contract_id|asset_id, amount|rgb_amount|funding_rgb })` | Opens an RGB-funded channel. |
| `ln_rgb::send_rgb_payment({ node_id|recipient_node_id, amount_msat, payment_id?, contract_id|asset_id, amount|rgb_amount|funding_rgb })` | Sends an RGB spontaneous payment. |

Examples:

```zs
ln_rgb::get_info()
ln_rgb::open_rgb_channel({
  node_id: "...",
  address: "1.2.3.4:9735",
  capacity_sat: 100000,
  contract_id: "...",
  amount: 100,
})
ln_rgb::send_rgb_payment({
  recipient_node_id: "...",
  amount_msat: 1000,
  contract_id: "...",
  amount: 1,
})
```

## Current Operational Notes

- `btc::get_deposit_address(ident)` consumes local pooled signer-derived
  addresses; it only asks signer App when the pool is below low water.
- `btc::scan_deposits(ident)` is suitable for small-scale/admin flows. For large
  custody wallets, replace it with a block/indexer-based scanner.
- `btc::sign_psbt(psbt, ident)` does not hold private keys locally. It sends the
  PSBT plus address derivation metadata to signer App.
- `rgb::*` no longer uses local RGB stock or Iroh consignment transport.
  Consignment management belongs to `rgb-service-daemon`.
- LN uses a local hot wallet. It is separate from `local/btc-addr` and must be
  funded independently for channel operations.
