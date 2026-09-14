use super::*;
use std::net::TcpListener;
use std::sync::atomic::AtomicUsize;

struct MockElectrum {
    url: String,
    requests: Arc<AtomicUsize>,
    active: Arc<AtomicUsize>,
    peak: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    server: Option<thread::JoinHandle<()>>,
}

impl MockElectrum {
    fn start(fail: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("electrum://{}", listener.local_addr().unwrap());
        listener.set_nonblocking(true).unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let active = Arc::new(AtomicUsize::new(0));
        let peak = Arc::new(AtomicUsize::new(0));
        let stop = Arc::new(AtomicBool::new(false));
        let state = (requests.clone(), active.clone(), peak.clone(), stop.clone());
        let server = thread::spawn(move || {
            let (requests, active, peak, stop) = state;
            let mut connections = Vec::new();
            while !stop.load(Ordering::SeqCst) {
                match listener.accept() {
                    Ok((mut socket, _)) => {
                        let requests = requests.clone();
                        let active = active.clone();
                        let peak = peak.clone();
                        connections.push(thread::spawn(move || {
                            socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
                            socket.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
                            let mut line = String::new();
                            BufReader::new(socket.try_clone().unwrap()).read_line(&mut line).unwrap();
                            let request: Value = serde_json::from_str(&line).unwrap();
                            assert_eq!(request["method"], "blockchain.scripthash.listunspent");
                            requests.fetch_add(1, Ordering::SeqCst);
                            let current = active.fetch_add(1, Ordering::SeqCst) + 1;
                            peak.fetch_max(current, Ordering::SeqCst);
                            thread::sleep(Duration::from_millis(80));
                            let response = if fail {
                                json!({"id": request["id"], "error": {"code": -1, "message": "mock unavailable"}})
                            } else {
                                json!({"id": request["id"], "result": [
                                    {"tx_hash": "11".repeat(32), "tx_pos": 0, "value": 4000, "height": 800000},
                                    {"tx_hash": "22".repeat(32), "tx_pos": 1, "value": 5000, "height": 0}
                                ]})
                            };
                            active.fetch_sub(1, Ordering::SeqCst);
                            writeln!(socket, "{response}").unwrap();
                        }));
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(error) => panic!("mock accept failed: {error}"),
                }
            }
            for connection in connections { connection.join().unwrap(); }
        });
        Self { url, requests, active, peak, stop, server: Some(server) }
    }
}

impl Drop for MockElectrum {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        if let Some(server) = self.server.take() { server.join().unwrap(); }
    }
}

fn inputs(count: usize) -> Vec<Value> {
    let secp = bitcoin::secp256k1::Secp256k1::new();
    (1..=count).map(|index| {
        let secret = bitcoin::secp256k1::SecretKey::from_slice(&[index as u8; 32]).unwrap();
        let public = PublicKey::from_secret_key(&secp, &secret);
        let address = Address::p2wpkh(&CompressedPublicKey(public), Network::Bitcoin);
        json!({"address": address.to_string()})
    }).collect()
}

#[test]
fn consolidation_point_lookup_preserves_used_precedence_and_missing() {
    let id = BTC_CONSOLIDATION_TRACE_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!(
        "btc-consolidation-point-{}-{}-{id}", std::process::id(), now_ms()
    ));
    let store = LocalNodeStore::open(&path).unwrap();
    store.put_btc_address_pool_record("available", &json!({"index": 1})).unwrap();
    store.put_btc_address_pool_record("both", &json!({"index": 2})).unwrap();
    store.put_used_btc_address_pool_record("both", &json!({"index": 3})).unwrap();
    assert_eq!(btc_address_pool_record_for(&store, "available").unwrap(), Some(json!({"index": 1})));
    assert_eq!(btc_address_pool_record_for(&store, "both").unwrap(), Some(json!({"index": 3})));
    assert_eq!(btc_address_pool_record_for(&store, "missing").unwrap(), None);
}

#[test]
fn consolidation_chain_deduplicates_addresses_and_caps_concurrency() {
    let server = MockElectrum::start(false);
    let mut items = inputs(11);
    items.push(items[0].clone());
    let result = consolidation_live_utxos(&items, &[server.url.clone()]).unwrap();
    assert_eq!(result.len(), 11);
    assert_eq!(server.requests.load(Ordering::SeqCst), 11);
    assert!(server.peak.load(Ordering::SeqCst) > 1);
    assert!(server.peak.load(Ordering::SeqCst) <= BTC_CONSOLIDATION_CHAIN_CONCURRENCY);
    assert_eq!(server.active.load(Ordering::SeqCst), 0);
    for utxos in result.values() {
        assert_eq!(utxos.len(), 1, "unconfirmed outputs must not be selected");
        assert_eq!(utxos.get(&format!("{}:0", "11".repeat(32))), Some(&4000));
    }
}

#[test]
fn consolidation_chain_failure_joins_workers_and_stops_later_batches() {
    let server = MockElectrum::start(true);
    let result = consolidation_live_utxos(&inputs(11), &[server.url.clone()]);
    assert!(result.is_err(), "node failure must not become an empty UTXO set");
    assert_eq!(server.requests.load(Ordering::SeqCst), BTC_CONSOLIDATION_CHAIN_CONCURRENCY);
    assert_eq!(server.active.load(Ordering::SeqCst), 0);
}

#[test]
fn consolidation_chain_uses_configured_fallback() {
    let failed = MockElectrum::start(true);
    let healthy = MockElectrum::start(false);
    let result = consolidation_live_utxos(&inputs(2), &[failed.url.clone(), healthy.url.clone()]).unwrap();
    assert_eq!(result.len(), 2);
    assert_eq!(failed.requests.load(Ordering::SeqCst), 2);
    assert_eq!(healthy.requests.load(Ordering::SeqCst), 2);
}

#[test]
fn consolidation_chain_invalid_address_performs_no_network_io() {
    let server = MockElectrum::start(false);
    let result = consolidation_live_utxos(&[json!({"address": "invalid"})], &[server.url.clone()]);
    assert!(result.is_err());
    assert_eq!(server.requests.load(Ordering::SeqCst), 0);
}
