# zust-console

Zust console for driving `rgb-service` daemon flows.

Startup takes one argument: the `.zs` script path.

```bash
cargo run -p zust-console -- crates/zust-console/start.zs
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
