# Official faucet test USDT fixtures

Public transfer proofs from the 2026-09-17 UTEXO signet interoperability run.
`manifest.json` records provenance, exact implementation revision, transactions,
raw amounts, bundle counts and SHA256 hashes. Precision is 6.

The official faucet delivered 100 USDT to the isolated reference wallet; it
sent 1 USDT to BiHelix Carol, which returned 0.4 USDT. These files contain no
private keys, mnemonic or wallet database. RGB proofs disclose transfer history;
they are deliberately retained here as public test-network fixtures.

The offline fixture test checks contract/schema identity, historical bundle
counts and byte-exact decode/encode. It does not substitute for chain validation
or settled balances; see the live evidence and integration report for those.
