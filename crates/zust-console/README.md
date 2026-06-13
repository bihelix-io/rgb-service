# zust-console

Zust console for driving `rgb-service` daemon flows.

Startup opens a command-line REPL. Passing a `.zs` script runs it first as the
session initializer and then drops into the REPL.

```bash
cargo run -p zust-console -- crates/zust-console/start.zs
```

For one-shot execution:

```bash
cargo run -p zust-console -- --once crates/zust-console/start.zs
cargo run -p zust-console -- -e 'ln::status()'
```

Runtime config belongs in the script through `root::add`:

```zs
root::add("local/rgb-service", "http://3.1.207.115:8091");
root::add("local/btc-addr", "bc1q5nqave6m673q4g704r4ppzwacur3d67amp3f8c");
root::add("local/signer-node", "417c33530ab6097e5e2538ffd16e833ee4337318015f201452a540c49adb4158");
```

`rgb::*` functions read the BTC address, daemon URL, and signer node from `local/*`.
For example, RNA balance is called without arguments:

```zs
rgb::rna_balance()
```

Signer requests are sent to the configured signer App over iroh as Dynamic msgpack:

```text
[path, body]
```

BTC deposit address pool uses this signer request:

```text
path = "/v1/signer/address/batch"
body = {
  account_id: "<wallet btc address>",
  network: "bitcoin",
  purpose: "low_water_refill" | "manual_refill",
  count: 20,
  timestamp_ms: 1780000000000,
}
```

The signer response must be:

```text
{
  account_id: "<wallet btc address>",
  network: "bitcoin",
  count: 20,
  addresses: [
    {
      address: "bc1...",
      derivation_path: "m/84'/0'/0'/0/12",
      index: 12,
      script_pubkey: "...",
    },
  ],
}
```

`addresses.length` must equal `count`, and every item must contain a non-empty
`address`. `zust-console` persists these records in the local address pool, and
`btc::get_deposit_address("...")` consumes one local pooled address
without asking the signer again. The scanner refills when the pool drops below
the low-water mark.

Inside the REPL, admin/debug code is executed directly:

```zs
btc::get_wallet_address()
btc::balance()
btc::utxos()
btc::assets()
btc::status()
btc::address_pool_status()
btc::refill_address_pool(20)
btc::get_deposit_address("alice")
btc::scan_deposits()

rgb::assets()
rgb::token_list()

ln::node_address({
  network: "bitcoin",
  low_water_sats: 100000,
})

ln::spawn_scanner({
  interval_ms: 30000,
})
```

`ln::node_address` loads `.zust-console/ln-node.json` if it exists. If it does
not exist, it creates a local LN hot-wallet mnemonic, derives a spendable BDK
wallet address, and persists the node state locally with file mode `0600`.
Console output redacts private key, seed, mnemonic, WIF, and xprv fields.

`start.zs` stores the returned node object in `local/lightning` and starts LN:

```zs
let lightning = ln::node_address({
  network: "bitcoin",
  low_water_sats: 100000,
});
root::add("local/lightning", lightning);
ln::start()
```

The scanner thread maintains the BTC deposit address pool and scans stored
deposit addresses through Esplora, persisting discovered outpoints locally.

BTC module scope:

```zs
btc::balance()
btc::utxos()
btc::assets()
btc::address_pool_status()
btc::refill_address_pool(20)
btc::get_deposit_address("alice")
btc::scan_deposits()
btc::tx_status("...")
btc::sign_psbt("...")
btc::broadcast("...")
```

`btc::assets()` and `rgb::assets()` read `local/btc-addr`, fetch its current
L1 UTXOs from Esplora, and send those outpoints to the RGB daemon. Direct BTC
send is not exposed until the signer App implements `/v1/signer/psbt/sign`;
`btc::sign_psbt()` calls that path directly and returns the real signer result.

Useful REPL commands:

```text
:load <path>
:reset
:quit
```
