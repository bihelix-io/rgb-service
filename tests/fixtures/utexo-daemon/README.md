# Official test USDT daemon HTTP fixtures

Captured on UTEXO custom signet on 2026-09-17. The asset is official Faucet test
USDT, not evidence of mainnet backing or Mint redemption.

The three consignments follow UTEXO reference → daemon A (500,000 raw units),
A → daemon B (200,000, blinded reception), B → reference (100,000, witness).
Precision is 6. The daemon handles RGB state and proof transport through its
public HTTP API; test helpers only build BTC carriers and sign wallet requests.

`manifest.json` records hashes, chain confirmations and proxy ACKs. Transaction
JSON files come from the official Esplora. `before-final-restart.json` and
`after-final-restart.json` retain public operation/asset responses and process IDs.
`roundtrip-public-evidence.json` records reference balances, idempotency checks,
the forced-kill recovery journal and reserved input observation.
`deployed-source-sha256.json` binds the tested daemon build to its source files.

No wallet keys, blind receive secrets or databases are included. The blind invoice
contains only its concealed beneficiary. Historical proof data is necessary to
reproduce wire compatibility and must not be confused with a wallet backup.

Run `cargo test --locked -p rgb-service-local --test utexo_fixtures` for offline
decoding/encoding, history and blind terminal checks. These tests do not query the
chain or replace the live acceptance evidence in the
[HTTP report](../../../deploy/utexo-signet/HTTP-INTEROP-REPORT.zh-CN.md).
