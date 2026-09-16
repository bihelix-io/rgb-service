# UTEXO signet integration environment

This is an isolated integration deployment on `bihelix-aws`. It does not use the
existing public-signet Bitcoin Core container or any mainnet stock.

## Build and deploy

Build the implementation branch in a separate checkout, with a separate target
directory and `CARGO_BUILD_JOBS=2`. Pin source revision in the deployment record.

```sh
cargo build --locked --release -p rgb-service-daemon --bin rgb-service --example signet_test_signer
```

On the server use `/home/ubuntu/utexo-integration/{bin,service-data,signer}`.
Install `rgb-service` into `bin/`, copy `examples/utexo-signet.toml` to
`rgb-service.toml`, and install the adjacent systemd unit. The `signer` directory
must have mode 0700 and is outside the daemon's writable data directory.
The unit binds only to loopback; no public ingress is required for local probes.

The height-100 checkpoint in `probe.py` was observed from the official UTEXO
Esplora on 2026-09-16. It prevents accidental use of public signet; it is not an
independent verification of signet consensus or the endpoint's trustworthiness.

```sh
python3 deploy/utexo-signet/probe.py
```

## Test signer and faucet

The example signer is a single P2WPKH test key, not a production/HD wallet or an
UTEXO SDK wallet. It is sufficient for requesting test BTC and signed API smoke
checks. Its key must later be used explicitly by the integration signing flow.
It refuses to overwrite an existing key. Back up the private WIF outside Git;
do not print it or include it in reports. The daemon never reads this key.

```sh
signet_test_signer init /home/ubuntu/utexo-integration/signer/alice.wif
signet_test_signer rna-request /home/ubuntu/utexo-integration/signer/alice.wif |
  curl --fail-with-body -sS -H 'Content-Type: application/json' \
    --data-binary @- http://127.0.0.1:18787/v1/rna/balance
```

The [UTEXO RN sandbox](https://github.com/UTEXO-Protocol/rgb-sdk-rn-sandbox)
documents Telegram `@Utexo_RLN_bot` with `/getbtc <address>`. In the observed
bot flow, this command asks for the address again; reply with the address alone.
Submit once, retain
the reply, and query the address on the UTEXO Esplora until funded. Ordinary
signet faucets fund a different chain. The alternate automated faucet requires
a configured URL and bearer token; do not guess credentials.

```sh
python3 deploy/utexo-signet/probe.py --address '<public test address>'
```

Success criteria: valid signed API call; unsigned call rejected; catalog
reachable; key file mode 0600; daemon restart preserves the profile; faucet UTXO
appears on the correct chain. These checks do not certify RGB interoperability.

Stop only `rgb-service-utexo-signet.service` to pause this environment. Preserve
the database and signer backups. Do not stop existing Bitcoin or application
containers.

互操作测试进度与 USDT Faucet 错误详见 [实测记录](INTEROP-REPORT.zh-CN.md)；可复用对照工具见 [reference harness](../../integration/utexo-reference/README.md)。
