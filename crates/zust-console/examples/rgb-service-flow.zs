// Minimal RGB service flow sketch for zust-console.
// Fill real outpoints, PSBTs, and txids from the external BTC wallet side.

let issue = rgb::issue("ZUSD", "Zust Console USD", 2, 1000000, arg.allocation_outpoint, |result| {
  root::add("local/rgb/flow/issue", result);
  result
});

let assets = rgb::assets(|result| {
  root::add("local/rgb/flow/assets", result);
  result
});

let daemon_test = rgb::test("full_rgb20_lifecycle", |result| {
  root::add("local/rgb/flow/test", result);
  result
});

{
  issue: issue,
  assets: assets,
  daemon_test: daemon_test,
  ln: ln_rgb::status(),
}
