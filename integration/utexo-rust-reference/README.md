# Isolated UTEXO Rust reference client

Uses the official `rgb-lib` Rust tag `v0.3.0-beta.20`, pinned to
`b5c2fb6e53bc8bce2c72e5e9ee50412d2a2f16a4`. This revision recognizes the IFA
schema delivered by the official test faucet. It is a test reference client,
not a daemon API, and it only connects to the UTEXO indexer. RGB validation
uses `BitcoinNetwork::Signet`, matching the original SDK mapping of `utexo`
and the faucet contract; the newer `SignetCustom` enum is not interchangeable.

The existing Node SDK wallet is watch-only internally. This client preserves
that model when loading a copied wallet; private keys are only used in a
separate offline signing wallet when explicitly preparing a send. Never run
the old and new clients concurrently against copies of the same wallet lineage.
Freeze the old runner, ensure its RGB runtime lock is absent, then copy the
entire wallet directory with mode 0700 and keys mode 0600. Preserve the old
copy as a rollback artifact; after any new broadcast it is stale and must not
be resumed for spending.

```sh
umask 077
cargo build --locked --release
./target/release/utexo-rust-reference /isolated-copy/rgb /isolated-copy/keys.json
```

A fourth argument names an operation JSON file. Each operation requires a new
`output` directory and writes an intent before performing the operation. An
existing directory blocks retries, including after ambiguous failures. Inspect
on-chain state and saved transfer state before any recovery.

- `witness`: `asset_id`, positive raw `amount`, `output`; creates a 24-hour
  witness invoice for the contract, using the official proxy.
- `prepare-send`: `invoice`, `output`; checks network, expiry, fungible amount
  and official endpoint, prepares a donation transfer with 5000 sats to the
  witness recipient and fee rate 2 sat/vB, then signs offline. Saves unsigned
  and signed PSBTs plus operation details. Does not broadcast.
- `complete-send`: `signed_psbt_path`, `output`; uploads proof and broadcasts
  the previously reviewed signed PSBT. Only call after inspecting amounts,
  fee, outputs, and the intended contract/recipient. Do not retry on timeout.

Status prints the complete per-transfer `refresh` result, including schema
errors that the older Node SDK wrapper discarded. It also prints assets and
transfers, never mnemonic/key material. A proxy ACK is not sufficient evidence
of a settled wallet balance.

All private state, operation files, and secrets belong outside this repository.
This tool does not establish production recovery, concurrency, or API guarantees.

## Migration observations

The beta.13 BDK watch-only chain cache is not binary-compatible with this
version (`bincode ... enum ... found 10`). In the isolated copy only, archive
`rgb/<fingerprint>/bdk_db_watch_only` before opening. Keep the RGB stock, SQLite
database, transfer files and receive secrets intact. The consistency check
performs a full scan of both keychains and must remain enabled. This procedure
was exercised on this small test wallet, not established as a general migration
for production wallets; preserve address derivation history before general use.

Explicitly enumerate supported schemas. Despite a comment saying an empty
list means all schemas, this revision returns `NoSupportedSchemas` for it.
The client's allocation limit and vanilla keychain match the original SDK
(1 allocation per UTXO, vanilla keychain 0).

The live candidate is `/home/ubuntu/utexo-integration/reference-rust-signet-state`.
The old Node runner is frozen with a `runner.lock` marker. An earlier
`reference-rust-state` experiment used the wrong RGB network enum and recorded
a failed receive; it is forensic state, not the active wallet. Never resume
spending from either stale copy.
