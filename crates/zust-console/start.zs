root::add("local/rgb-service", "http://3.1.207.115:8091");
root::add("local/rgb-service-data", "/home/ubuntu/rgb-service-data");
root::add("local/btc-addr", "bc1q5nqave6m673q4g704r4ppzwacur3d67amp3f8c");
root::add("local/signer-node", "417c33530ab6097e5e2538ffd16e833ee4337318015f201452a540c49adb4158");
root::add("local/signer-request", {
  attempts: 60,
  retry_interval_ms: 0,
  timeout_ms: 60000,
});

let ln_node_config = {
  network: "bitcoin",
  chain_source: {
    kind: "esplora",
    url: "https://blockstream.info/api"
  },
  data_dir: ".zust-console/lightning",
  ldk_data_dir: ".zust-console/lightning/ldk",
  ln_backend: "ln-rgb",
  listen: "0.0.0.0:9736",
  peers: [],
  trusted_peers_0conf: [],
  accept_inbound_channels: true,
  low_water_sats: 100000,
  interval_ms: 30000,
};

let lightning = ln_rgb::node_address(ln_node_config);
root::add("local/lightning", lightning);
root::add("local/lightning/node", lightning.config);

{
  lightning: root::get("local/lightning"),
  ln_rgb_start: ln_rgb::start(),
  ln_rgb_scanner: ln_rgb::spawn_scanner(120000),
  ln_rgb: ln_rgb::status(),
}
