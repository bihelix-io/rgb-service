# rgb-service-daemon

`rgb-service-daemon` is the HTTP daemon for BiHelix RGB service APIs. It exposes the `rgb-service-api` Axum router and stores local service state in `fjall` under `service.data_dir`.

The daemon is intentionally explicit: missing required configuration fails at startup. Do not rely on silent defaults for service bind address, Bitcoin network, data directory, chain backend, or RNA fees.

## What the daemon owns

The daemon owns service-side state, not user private keys.

```text
rgb-service-daemon
  - RGB stock / account state
  - BTC address profile state
  - internal RNA credit balance
  - internal usage logs
  - pending RGB transfer state
```

The daemon does not own:

```text
BTC private keys
RGB asset signing keys
wallet seed phrases
frontend user sessions
LN node state
```

## Identity model

First version rule:

```text
account_id = caller BTC address
profile id = BTC address
```

So a request like this:

```json
{
  "account_id": "bc1pcaller...",
  "btc_address": "bc1ptarget..."
}
```

means:

```text
bc1pcaller... signs and pays for the request
bc1ptarget... is the address being queried or operated on
```

## RNA credits

`RNA` is an internal service credit, not an RGB asset.

It exists only inside the daemon profile:

```text
profiles/{btc_addr} -> Dynamic MsgPack
```

New profiles start with `0` RNA. The daemon no longer grants RNA automatically.

Default fee policy:

```text
issue asset:       1000 RNA
transfer prepare:   100 RNA
query:                1 RNA
```

The daemon writes internal usage logs, but does not expose per-user transaction history in the public API.

## External L1 RGB transfers

The opt-in `service.external_rgb` module receives witness/blinded RGB invoices,
prepares externally addressed payments and broadcasts only finalized, authorized
carriers. Private keys stay with the wallet. Its account-scoped operation API
exposes delivery, broadcast, confirmation and recovery state for these transfers.
See the [API and configuration](../../docs/EXTERNAL-RGB-API.zh-CN.md) and
[test client](../../integration/utexo-daemon/README.md).

External accounts use persistent input reservations. Asset lists and balance
breakdowns exclude reserved or quarantined allocations from available funds.
Queries of pending operations include external tasks. The first version requires
`legacy.enabled=false` and fences external accounts from old issue/direct-send/LN
write paths; other accounts retain the existing API. Reorgs freeze affected
accounts for review rather than automatically rebuilding their state.

Back up the entire service database and account stocks together, including receive
secrets. Never restore a pre-payment snapshot to an active wallet after broadcast.
Disabling new work does not justify deleting in-flight operations or their proofs.

DAEMON_RNA metering is temporarily disabled. The configured fee schedule and
balance/credit APIs remain available, but issue, query, L1 transfer prepare and
LN channel-open prepare do not debit or refund DAEMON_RNA while metering is
paused. This does not disable the separately configured legacy RGB RNA transfer
fee.

## Configuration

Example regtest configuration:

```toml
[service]
bind = "127.0.0.1:8787"
network = "regtest"
data_dir = "/tmp/bihelix-rgb-service"
esplora_url = "http://127.0.0.1:3002"

[rna]
issue_fee = 1000
transfer_fee = 100
query_fee = 1

[legacy]
enabled = false
allow_loopback = true
allowed_ips = []
sync_timeout_secs = 30
reveal_address_count = 20
rgb_fee_enabled = false
rgb_fee_collector_address = "bc1q42qxdzefsaqks40j7qtk2u8r00pzxt44nu3qzj"
rgb_fee_contract_id = "rgb:nykNCHhT-BgKdtCi-ilF89kf-JilBhg0-JfInd9k-7MyyYOE"
rgb_fee_amount = 1054000
```

Example mainnet server configuration:

```toml
[service]
bind = "0.0.0.0:8787"
network = "mainnet"
data_dir = "/home/ubuntu/rgb-service-data"
esplora_url = "https://mempool.space/api"

[rna]
issue_fee = 1000
transfer_fee = 100
query_fee = 1
```

`esplora_url` is the Bitcoin chain backend. It is used to check anchor transactions, outpoints, confirmations, and recovery-related chain state. It is not an RGB data source and it does not hold BTC keys.

## Run locally

```bash
cargo run -p rgb-service-daemon -- examples/rgb-service.toml
```

## One-time wallet-v2 stake redeem import

The importer accepts a Navicat PostgreSQL `.sql` dump or a `.sql.zip` archive.
It projects the legacy `redeem`, `transfer`, and `rgb_assign` rows into the
daemon's Fjall `stake_redeems` keyspace. It does not import the old full PSBT or
fascia payloads. Stop any daemon already using the same `service.data_dir`
before running the standalone import command.

Run a non-writing precheck first:

```bash
cargo run -p rgb-service-daemon -- \
  import-wallet-v2-stake-redeems \
  examples/rgb-service.toml \
  /secure-backup/wallet-v2.sql.zip \
  --report /secure-backup/stake-import-dry-run.json \
  --dry-run
```

The precheck validates every legacy relation and field used by redeem, then
checks every `status=0` stake outpoint against the configured Bitcoin backend:
confirmation, output index, amount, P2WSH stake script, confirmation height,
and unspent state. Any mismatch aborts the import.

After reviewing the report, import once:

```bash
cargo run -p rgb-service-daemon -- \
  import-wallet-v2-stake-redeems \
  examples/rgb-service.toml \
  /secure-backup/wallet-v2.sql.zip \
  --report /secure-backup/stake-import.json
```

The Fjall records, public-key secondary indexes, and migration marker are
committed in one transaction and synchronously persisted. The daemon then reads
all imported records and indexes back, verifies their immutable digest, and
writes the final JSON report.

The same import may instead run before the HTTP listener starts:

```bash
cargo run -p rgb-service-daemon -- \
  examples/rgb-service.toml \
  --import-wallet-v2-stake-redeems /secure-backup/wallet-v2.sql.zip \
  --stake-import-report /secure-backup/stake-import.json
```

Idempotency is enforced by the `wallet_v2_stake_redeems_v1` marker in the
`schema_migrations` keyspace. Re-running with the same source SHA-256 performs
only a read-back verification and reports `already-applied`; it does not import
again or repeat chain queries. Re-running with a different source SHA-256 is
refused. Remove the one-time startup flags after the first successful start,
and keep the successful JSON report with the database backup.

Daemon logs are written to:

```text
<service.data_dir>/rgb-service.log
```

## Run on server with screen

Clone or update the repository:

```bash
git clone https://github.com/bihelix-io/rgb-service.git ~/rgb-service
cd ~/rgb-service
```

Start in a detached screen session:

```bash
screen -dmS rgb-service bash -lc 'cd ~/rgb-service && cargo run -p rgb-service-daemon -- examples/rgb-service.toml'
```

Attach to the session:

```bash
screen -r rgb-service
```

Detach from the session:

```text
Ctrl-a d
```

Stop the session:

```bash
screen -S rgb-service -X quit
```

Check screen sessions:

```bash
screen -list
```

Check daemon log:

```bash
tail -100 ~/rgb-service-data/rgb-service.log
```

Check port listening:

```bash
ss -ltnp | grep 8787
```

Expected mainnet startup log:

```text
starting rgb-service on 0.0.0.0:8787 for mainnet with data_dir /home/ubuntu/rgb-service-data
INFO rna issue_fee=1000 transfer_fee=100 query_fee=1
```

## Public HTTP API

中文 API 说明和 LN route 字段示例见仓库根目录
`docs/API.zh-CN.md`。

Current public daemon routes:

```text
POST /v1/rna/balance            # Query caller internal RNA balance and current fee policy
POST /v1/assets/issue           # Issue RGB20 asset; DAEMON_RNA debit temporarily disabled
POST /v1/assets/list            # Query account asset list; DAEMON_RNA debit temporarily disabled
POST /v1/assets/by-utxo         # Public read-only query for verified RGB allocations at one L1 outpoint; no signature or RNA fee
GET  /v1/tokens/list            # Public RGB20 contract/asset catalog, no signature required
POST /v1/balance                # Query asset balance summary; DAEMON_RNA debit temporarily disabled
POST /v1/balance/breakdown      # Query allocations and pending detail; DAEMON_RNA debit temporarily disabled
POST /v1/transfers/prepare      # Prepare RGB transfer; DAEMON_RNA debit temporarily disabled
POST /v1/transfers/commit       # Commit txid and stage consignment for recipient
POST /v1/transfers/cancel       # Cancel unfinished transfer
POST /v1/ln/channels/open/prepare # Prepare RGB-aware LN channel open
POST /v1/ln/channels/funding-ref  # Register/query LN funding reference
POST /v1/ln/commitments/compose   # Compose RGB-aware LN commitment transition
POST /v1/ln/closing/compose       # Compose RGB-aware LN closing transition
POST /v1/ln/onchain-claims/compose # Compose RGB-aware LN on-chain claim
POST /v1/ln/payments/claim        # Claim RGB-aware LN payment state
POST /v1/ln/recover               # Recover RGB-aware LN service state
POST /v1/test/rgb               # Controlled RGB lifecycle test route
```

## Internal Legacy Compatibility

The wallet-service-v2 compatible routes are **not public API**. They are kept as
an internal compatibility experiment and are disabled by default. They are only
mounted when `[legacy].enabled = true` in the daemon config and should be bound
to loopback or a trusted internal network during development.

```text
PUT  /account/create
GET  /asset/list
GET  /asset
GET  /utxo
POST /asset/internal/issue
GET  /estimate/gas
GET  /get_fee
GET  /stake/address
POST /stake/split
GET  /redeem/list
POST /redeem/psbt
POST /redeem/callback
POST /transfer/psbt
POST /transfer/callback
POST /transfer/cancel
```

The legacy `/transfer/psbt` route opens a watch-only descriptor wallet, syncs
with Esplora, builds the BTC PSBT with BDK, selects required RGB UTXOs from the
daemon stock, writes the RGB commitment, and returns the unsigned PSBT for
external signing. The daemon still does not hold BTC private keys.

Legacy RGB RNA fee collection is implemented but disabled by default. When
`[legacy].rgb_fee_enabled = true`, `/transfer/psbt` appends a dust output to
`rgb_fee_collector_address`, assigns the configured RNA fee to that output, and
prepares all requested RGB assignments in one RGB PSBT. Leave it disabled until
the BitPocket routing path has been verified.

Current gaps before exposing this beyond trusted local testing:

- Legacy allowlist is IP-based only. Empty `allowed_ips` means no IP
  restriction; once compatibility is verified, set exact caller IPs here.
- `/transfer/psbt` supports multiple RGB assignments, including optional RNA
  RGB fee collection when `[legacy].rgb_fee_enabled = true`.
- `/transfer/callback` accepts a signed raw transaction. For RGB transfers it
  infers the prepared transfer from the transaction id; for BTC-only transfers
  no prepared RGB state is required. The callback broadcasts the transaction.
- Pending UTXO reservation is minimal; production use needs stronger
  double-spend/pending-transfer guards.
- Legacy `/asset/list` returns daemon catalog metadata but not historical total
  supply.
- Legacy `/asset` maps `address` directly to daemon `account_id`; descriptor
  accounts use an internal `legacy-desc:<sha256(desc)>` id.
- `GET /stake/address`, `POST /stake/split`, and the three `/redeem/*` routes
  preserve the wallet-service-v2 request/response shapes. Redeem PSBTs are
  rebuilt from the funding transaction fetched through the configured chain
  backend; the old full PSBT row is not required.
- Redeem state is stored in Fjall keyspaces `stake_redeems` (by stake outpoint)
  and `stake_redeems_by_public_key`. A successful redeem broadcast updates the
  record idempotently. Historical records still need to be imported before
  these endpoints are enabled for production traffic.
- Stateful `/stake/coloring` and `/stake/callback`, stock backup/upload,
  foreign transfer, retry/rescan and tx detail routes are not implemented in
  this compatibility layer.
- Error bodies are compatibility-shaped enough for debugging, but not a full
  wallet-service-v2 error-code clone.

L1 pending/recovery is daemon-owned background work. The public HTTP API does
not expose `/v1/pending/list` or `/v1/recover` to clients.


LN compose routes are service-owned state-transition APIs for `ln-rgb-lightning`. They require both the outer request signature and `asset_authorization`. Until the daemon RGB-LN state machine is wired to `rgb-service-local`, these routes fail loudly with HTTP 501 instead of falling back to local LN RGB state.

All non-public requests use `SignedRequest<T>`. `/v1/assets/by-utxo` is a read-only public
query and accepts its payload directly without a signer request.

`/v1/assets/by-utxo` payload:

```json
{
  "account_id": "<custody account>",
  "outpoint": "<txid>:<vout>",
  "address": "bc1q...",
  "confirmed": true
}
```

The response returns `assets` and `allocations`. Allocations come exclusively from the
account RGB stock after consignment acceptance; an ordinary BTC UTXO with no RGB assignment
returns empty arrays.

The route also accepts a JSON array of the same payload. An array request returns an array of
responses in the same order; a single-object request continues to return a single object.

`prepare` and `commit` also require `AssetSpendAuthorization`.

The service uses direct send: sender prepares and commits the transfer, and
`/v1/transfers/commit` builds the consignment and stages it directly for the
recipient account saved in `prepare.recipient`.

## Storage layout

Under `service.data_dir`:

```text
rgb-service.log          # daemon log
kv/                      # single fjall database for all daemon state
```

Important fjall keyspaces:

```text
profiles                 # btc_addr profile, stored as Zust Dynamic MsgPack
usage_logs               # internal RNA debit logs
prepared_transfers       # pending prepared transfer state
account_utxos            # daemon-maintained account UTXOs used for RGB allocation lookup
rgb_stock                # all account RGB stocks, keyed by account prefix
rgb_pending_ops          # all pending RGB operations, keyed by account + txid
rgb_pending_status       # pending/confirmed promotion state
legacy_wallets           # wallet-service-v2-compatible BDK wallet state
schema_migrations        # completed on-disk-to-database migrations
```

On the first upgraded startup, legacy `accounts/<account_id>/rgb-stock` and
`legacy-wallets/<descriptor_hash>/bdk_wallet` data are imported into `kv/`.
The import is idempotent and records migration markers only after every source
entry succeeds. Normal operation does not read or write those directories.

## RGB UTXO ownership

RGB assets are bound to BTC UTXOs. The daemon maintains each account's known
RGB-relevant UTXOs in `account_utxos`.

The public HTTP API does not provide an ops/admin UTXO registration endpoint,
and asset/balance queries do not scan Esplora as a fallback. Issue and transfer
commit requests add the involved UTXOs to daemon state; queries remove known
UTXOs that no longer carry RGB allocations.

For historical backfill or operational recovery, stop the daemon and use its
offline repair command with explicit outpoints:

```bash
target/release/rgb-service repair-daemon-account-utxos \
  ./rgb-service.toml <account_id> <txid:vout>...
```

The command is daemon-owned and writes `account_utxos` through the daemon storage
implementation. `zust-console` must not open or modify the daemon Fjall database.

## Deployment note

For public mainnet deployment, bind to:

```toml
bind = "0.0.0.0:8787"
network = "mainnet"
esplora_url = "https://mempool.space/api"
```

Make sure the server firewall/security group allows inbound TCP `8787` only from the intended clients if the service should not be public.

## RNA balance request

Querying RNA balance is signed but does not charge RNA. If the caller profile does not exist yet, the daemon creates it with `0` RNA.

```http
POST /v1/rna/balance
```

Payload inside `SignedRequest<T>`:

```json
{
  "account_id": "bc1pcaller..."
}
```

Response:

```json
{
  "account_id": "bc1pcaller...",
  "rna_balance": 0,
  "new_profile_grant": 0,
  "issue_fee": 1000,
  "transfer_fee": 100,
  "query_fee": 1
}
```
