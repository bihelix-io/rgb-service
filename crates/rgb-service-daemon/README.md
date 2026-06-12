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

A new profile receives the configured `new_profile_grant`.

Default fee policy:

```text
new profile grant: 10000 RNA
issue asset:       1000 RNA
transfer prepare:   100 RNA
query:                1 RNA
```

The daemon writes internal usage logs, but does not expose per-user transaction history in the public API.

## Configuration

Example regtest configuration:

```toml
[service]
bind = "127.0.0.1:8787"
network = "regtest"
data_dir = "/tmp/bihelix-rgb-service"
esplora_url = "http://127.0.0.1:3002"

[rna]
new_profile_grant = 10000
issue_fee = 1000
transfer_fee = 100
query_fee = 1
```

Example mainnet server configuration:

```toml
[service]
bind = "0.0.0.0:8787"
network = "mainnet"
data_dir = "/home/ubuntu/rgb-service-data"
esplora_url = "https://mempool.space/api"

[rna]
new_profile_grant = 10000
issue_fee = 1000
transfer_fee = 100
query_fee = 1
```

`esplora_url` is the Bitcoin chain backend. It is used to check anchor transactions, outpoints, confirmations, and recovery-related chain state. It is not an RGB data source and it does not hold BTC keys.

## Run locally

```bash
cargo run -p rgb-service-daemon -- examples/rgb-service.toml
```

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
INFO rna new_profile_grant=10000 issue_fee=1000 transfer_fee=100 query_fee=1
```

## Public HTTP API

Current public daemon routes:

```text
POST /v1/rna/balance            # Query caller internal RNA balance and current fee policy
POST /v1/assets/issue           # Issue RGB20 asset, charges issue_fee
POST /v1/assets/list            # Query account asset list, charges query_fee
POST /v1/balance                # Query asset balance summary, charges query_fee
POST /v1/balance/breakdown      # Query allocations and pending detail, charges query_fee
POST /v1/invoices/create        # Create RGB receive invoice
POST /v1/transfers/prepare      # Prepare RGB transfer, charges transfer_fee
POST /v1/transfers/commit       # Commit txid after BTC broadcast
POST /v1/consignments/send      # Build and transmit RGB consignment
POST /v1/consignments/receive   # Receive external RGB consignment
POST /v1/transfers/cancel       # Cancel unfinished transfer
POST /v1/pending/list           # List pending operations
POST /v1/recover                # Promote/recover pending operations
POST /v1/test/rgb               # Controlled RGB lifecycle test route
```


LN compose routes are service-owned state-transition APIs for `ln-rgb-lightning`. They require both the outer request signature and `asset_authorization`. Until the daemon RGB-LN state machine is wired to `rgb-service-local`, these routes fail loudly with HTTP 501 instead of falling back to local LN RGB state.

All requests use `SignedRequest<T>`.

`prepare`, `commit`, and `send_consignment` also require `AssetSpendAuthorization`.

The public daemon does not expose raw fascia download or arbitrary raw consignment download. Consignments move through controlled APIs:

```text
/v1/consignments/send
/v1/consignments/receive
```

`/v1/consignments/send` supports:

```text
inline
service_inbox
```

## Storage layout

Under `service.data_dir`:

```text
rgb-service.log          # daemon log
kv/                      # fjall database
accounts/<btc_addr>/     # RGB stock/account state
```

Important fjall keyspaces:

```text
profiles                 # btc_addr profile, stored as Zust Dynamic MsgPack
usage_logs               # internal RNA debit logs
prepared_transfers       # pending prepared transfer state
```

## Deployment note

For public mainnet deployment, bind to:

```toml
bind = "0.0.0.0:8787"
network = "mainnet"
esplora_url = "https://mempool.space/api"
```

Make sure the server firewall/security group allows inbound TCP `8787` only from the intended clients if the service should not be public.

## RNA balance request

Querying RNA balance is signed but does not charge RNA. If the caller profile does not exist yet, the daemon creates it and grants `new_profile_grant`.

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
  "rna_balance": 10000,
  "new_profile_grant": 10000,
  "issue_fee": 1000,
  "transfer_fee": 100,
  "query_fee": 1
}
```
