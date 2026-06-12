root::add("local/rgb-service", "http://3.1.207.115:8091");
root::add("local/btc-addr", "bc1q5nqave6m673q4g704r4ppzwacur3d67amp3f8c");
root::add("local/signer-node", "417c33530ab6097e5e2538ffd16e833ee4337318015f201452a540c49adb4158");

{
  btc_wallet: bdk::wallet({
    network: "bitcoin",
  }),
  btc_anchor: bdk::external_anchor({
    unsigned_anchor_psbt: "",
    signed_anchor_psbt: "",
    txid: "",
  }),
  btc_signature: bdk::sign_request({
    operation: "btc",
  }),
  ln: ln::status({}),
}
