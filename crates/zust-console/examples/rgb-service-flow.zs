// Minimal RGB service flow sketch for zust-console.
// Fill real outpoints, PSBTs, and txids from the external BTC wallet side.

let account_id = arg.btc_addr;
let signer_id = "zust-console";

let issue = rgb::issue({
  signer_id: signer_id,
  account_id: account_id,
  ticker: "ZUSD",
  name: "Zust Console USD",
  precision: 2,
  supply: 1000000,
  allocation_outpoint: arg.allocation_outpoint,
});

let assets = rgb::assets({
  signer_id: signer_id,
  account_id: account_id,
  tracked_utxos: arg.tracked_utxos,
});

let daemon_test = rgb::test({
  signer_id: signer_id,
  account_id: account_id,
  scenario: "full_rgb20_lifecycle",
});

{
  issue: issue,
  assets: assets,
  daemon_test: daemon_test,
  ln: ln::status(),
}
