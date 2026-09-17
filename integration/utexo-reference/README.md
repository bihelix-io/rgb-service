# UTEXO reference wallet and BiHelix compatibility harness

> 2026-09-17: This Node reference wallet is frozen. Its native beta.13 cannot
> recognize the current faucet IFA schema. The active test wallet uses the
> [isolated Rust reference client](../utexo-rust-reference/README.md). Do not
> resume spending from the old wallet or a local snapshot after migration.

This is an isolated **UTEXO signet** integration harness, not a new daemon HTTP
API or a production wallet. The BiHelix side uses `rgb-service-local` directly;
its stock must be separate from any running daemon database. The reference SDK
and BiHelix must never write the same wallet state.

## Version and installation

- Node 22.x (tested 22.23.2). Node 26 fails to compile the published SWIG binding.
- `@utexo/rgb-sdk` 1.0.0-beta.8; `@utexo/rgb-lib` 0.3.0-beta.13.
- Install with `npm ci` using Node 22. Native modules require a C++ compiler,
  Python and the platform linker. Explicitly verify the platform addon loads;
  npm can suppress optional-addon build failures.
- `node reference.mjs init` creates private `state/reference-a/keys.json` with
  mode 0600 and prints only the deposit address.
- The recorded live wallet resides on bihelix-aws in
  `/home/ubuntu/utexo-integration/reference`; its local pre-migration snapshot
  is obsolete and must not be used for further spending.

Every command holds a wallet runner lock. If a process is interrupted, inspect
its status and wallet logs before recovering any stale lock. Never copy a live
wallet directory or operate two copies of one wallet.

## Reference commands

```sh
node reference.mjs init
node reference.mjs status
node reference.mjs utxos reference-a 8 1000 true
node reference.mjs receive reference-a
node reference.mjs witness reference-a '<contract-id>' 40
node reference.mjs issue-test reference-a
node reference.mjs send reference-a '<invoice>' '<contract-id>' 100 5000
```

`receive` uses a blind beneficiary and defaults to 1,000,000 raw units, matching
the observed faucet amount. `witness` and `receive` take optional contract and
raw amount arguments. `send` takes witness sats as its final argument and uses
`donation=true`; it is for the witness test flow shown here. `issue-test` issues
only `BHXTEST / BiHelix Interop Test Only`, never a token named USDT.

One-shot commands refuse to overwrite a previous result. A send writes
`send-pending.json` before execution; on an ambiguous error, inspect the SDK
transfer state, proxy and chain before archiving that marker. Do not repeat a
payment merely because a process timed out. Results contain public transaction
information; `keys.json` and the wallet state remain outside Git.

## BiHelix commands

```sh
cargo build --locked --release -p rgb-service-local --example utexo_compat
cargo test --locked --release -p rgb-service-local --test utexo_fixtures
```

The `utexo_compat` example supports:

- `invoice <file>`: decode an invoice using BiHelix's existing core.
- `contract <file>`: decode a contract and report its ID/schema.
- `consignment <file> [esplora-url]`: decode and optionally validate on chain.
- `make-invoice <output-file> <signet-address> <contract-id> <raw-amount>`:
  make a 24-hour witness invoice with the official UTEXO proxy.
- `accept <consignment> <isolated-stock-dir> <recipient-outpoint>`:
  validate/import a test proof and report allocations at that outpoint.
- `prepare-return <spec.json>`: prepare the test return PSBT/fascia/consignment.
- `settle-return <fascia.bin> <isolated-stock-dir> <txid>`: stage the sender
  transition, check chain confirmation and report the change allocation.

The test return specification has `stock`, `invoice`, `change_address`,
`inputs: [{outpoint, sats}]` and a new `output_dir`. All inputs must belong to the
same dedicated P2WPKH test key. The harness uses 2,000 sats for the recipient and
500 sats for the fee. Output order is OP_RETURN (0), recipient (1), change (2).
Verify input values/ownership against Esplora before signing. The test signer
accepts `sign-test-psbt <wif-path> <unsigned.psbt>` and outputs signed transaction
JSON without broadcasting. Keep its private WIF separate from the daemon.

`deploy/utexo-signet/proxy.py get <recipient-id> --output <file>` downloads proof
bytes from the fixed official endpoint; it does not validate them. ACK must only
be sent after RGB validation and durable acceptance. Upload proof before
broadcast in the tested donation flow; retain proof, fascia, signed transaction,
proxy response, expected txid and final wallet balances together.

## Compatibility constraint found in the live test

UTEXO's deployed validator rejects an `opret1st` proof if a Taproot output occurs
before OP_RETURN. BiHelix has an existing compatibility relaxation accepting
that ordering. Do not change that consensus policy as an incidental integration
fix. The new `prepare_rgb20_external_psbt` wrapper rejects incompatible carriers
before preparing them; callers must supply correct output indexes/order.

A captured rejected carrier is included in the offline regression tests.
Invoice/consignment parsing tests alone are insufficient: real reverse transfers
and the receiver's settled balance are required for interoperability acceptance.
