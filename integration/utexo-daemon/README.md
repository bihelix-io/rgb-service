# UTEXO signet daemon HTTP integration client

These helpers only build carriers and sign test-key requests. The daemon performs
RGB preparation, proof validation, proxy delivery and broadcasting. Wallet keys
stay outside the repository and outside the daemon.

Build `signet_test_signer` and `external_test_carrier` from `rgb-service-daemon`.
Run `request.py SIGNER TEST_WIF API_BASE PAYLOAD_JSON`. Never reconstruct a new
payment after a timeout: query/retry the original request ID or operation ID.

Request envelope before signing:

```json
{
  "account_id": "<test P2WPKH address>",
  "action": {
    "type": "receive",
    "request_id": "receive-001",
    "asset_id": "<complete RGB contract ID>",
    "amount": 500000,
    "expires_at": 1789700000,
    "blind_outpoint": null
  }
}
```

Set expiry relative to the current time (at most seven days). `blind_outpoint`
selects blinded reception on an unallocated confirmed UTXO owned by the account;
null creates a witness invoice. A witness recipient is not reused because the
proxy slot is keyed by its address-derived ID.

For sending, first build a BTC carrier with `external_test_carrier SPEC_JSON`:

```json
{
  "account_id": "<sender address>",
  "invoice": "<recipient invoice>",
  "inputs": [{"outpoint": "<txid>:<vout>", "sats": 5000}],
  "recipient_sats": 2000,
  "fee": 500
}
```

Construct `action.type=prepare` using the returned `invoice`,
`unsigned_anchor_psbt`, `recipient_vout`, `change_vout`, and a new `request_id`.
Include `asset_authorization` with the same asset ID/amount, `purpose=l1_transfer`,
`recipient` equal to the canonical invoice, `anchor_psbt` equal to the exact
unsigned PSBT, and `expires_at_ms` set in the near future. Its `signature` object
must have the RequestSignature fields; the test signer replaces it with the real
signature. The outer signature is then generated over the typed request.

Review the returned RGB PSBT before signing. Write its decoded bytes to a file
and invoke `signet_test_signer sign-test-psbt TEST_WIF FILE`. This prints the raw
transaction, txid, fee and finalized `signed_psbt` (test fee ceiling: 2000 sats).
Submit `action.type=finalize`, `request_id`, `operation_id`,
`signed_anchor_psbt`, and a new asset authorization bound to the daemon's returned
RGB `anchor_psbt`. Finalize saves a durable intent; it does not promise immediate
broadcast or settlement.

Query using `action={"type":"get","operation_id":"..."}`. List with
`action={"type":"list","after":null,"limit":20}`. Refresh queues a scan;
it does not replace the signed transaction. The daemon's worker performs retries.

Store payloads, responses and operation IDs in a private integration directory.
Do not commit WIFs, raw wallet databases or receive seal secrets. See
`docs/EXTERNAL-RGB-API.zh-CN.md` for protocol and recovery semantics.

The live test used 0.5 USDT reference → A, 0.2 USDT A → B (blind), and
0.1 USDT B → reference. It also killed the daemon after finalize persisted the
signed intent and verified recovery of the same operation/txid. See the
[HTTP acceptance report](../../deploy/utexo-signet/HTTP-INTEROP-REPORT.zh-CN.md)
and public fixtures in `tests/fixtures/utexo-daemon/`. Never replay the captured
payments or use their expired invoices to start new tests.
