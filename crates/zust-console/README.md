# zust-console

Zust console for driving `rgb-service` daemon flows.

This crate registers three Zust module namespaces:

- `bdk`: external BTC wallet side helpers for request signatures, asset authorizations, and anchor PSBT fields.
- `rgb`: wrapped `rgb-service-daemon` calls. Scripts call `rgb::issue`, `rgb::test`, etc. directly.
- `ln`: registered but disabled for now; LN integration is intentionally deferred.

`zust-console` is a pure daemon client. Startup requires the daemon connection config; scripts should not carry RGB stock paths, iroh secret keys, or daemon-side runtime settings.

```bash
cargo run -p zust-console -- crates/zust-console/examples/rgb-service-flow.zs '{"daemon_url":"http://127.0.0.1:8787","btc_addr":"bcrt1..."}'
```

`rgb::*` functions wrap daemon HTTP calls internally. For example:

```zs
let response = rgb::test({
  account_id: arg.btc_addr,
  scenario: "full_rgb20_lifecycle",
});
```

BTC modules should use BTC addresses as the identity handle. Frontend code only needs `btc_addr`; it should not call `rgb::lookup_iroh_node` directly. When a request needs a real signature, the `rgb` Zust module resolves `btc_addr -> iroh_node_id` through the daemon, caches the binding inside the module process, returns a `waiting_for_signer_app` state, and then the signer App can be contacted over iroh. `zust-console` is only the script runtime; it is not a background service.

Example signing discovery call from a BTC module:

```zs
let wait = rgb::request_signature({
  account_id: arg.btc_addr,
  btc_addr: "bcrt1target...",
});
```

Example RGB asset authorization request:

```zs
let wait = rgb::asset_authorization({
  account_id: arg.btc_addr,
  btc_addr: arg.btc_addr,
  asset_id: asset_id,
  amount: 1000,
});
```

The RGB helpers wrap payloads in `SignedRequest<T>`. The current console signer is a development signer for local integration testing only; production wallets should replace it with the iroh signer App flow.


Identity rule:

```text
account_id = caller BTC address
profile id = BTC address
```

Registering an iroh signer requires `account_id == btc_address`. Looking up another address is allowed, but the lookup is signed and charged to the caller `account_id`.

RNA is an internal service credit, not an RGB asset. New profiles receive the configured grant, and resource APIs charge the configured RNA fees from the caller profile.


RNA balance can be queried through the RGB module:

```zs
let rna = rgb::rna_balance({
  account_id: arg.btc_addr,
});
```
