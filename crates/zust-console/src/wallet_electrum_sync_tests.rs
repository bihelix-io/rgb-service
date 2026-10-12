use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::net::TcpListener;

const MNEMONIC: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

struct Node {
    config: ElectrumConfig,
    txs: Arc<Mutex<Vec<(Transaction, i64)>>>,
    calls: Arc<Mutex<Vec<String>>>,
    fail: Arc<AtomicBool>,
    limit: Arc<AtomicUsize>,
    stop: Arc<AtomicBool>,
    join: Option<std::thread::JoinHandle<()>>,
}
impl Node {
    fn new(txs: Vec<(Transaction, i64)>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let config = ElectrumConfig::new(format!("electrum://{}", listener.local_addr().unwrap()));
        let txs = Arc::new(Mutex::new(txs));
        let calls = Arc::new(Mutex::new(Vec::new()));
        let fail = Arc::new(AtomicBool::new(false));
        let limit = Arc::new(AtomicUsize::new(0));
        let limited = limit.clone();
        let stop = Arc::new(AtomicBool::new(false));
        let (data, recorded, failing, stopping) = (txs.clone(), calls.clone(), fail.clone(), stop.clone());
        let join = std::thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                let (stream, _) = match listener.accept() {
                    Ok(pair) => pair,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(2)); continue;
                    }
                    Err(e) => panic!("accept: {e}"),
                };
                stream.set_nonblocking(false).unwrap();
                stream.set_read_timeout(Some(Duration::from_millis(20))).unwrap();
                let mut reader = BufReader::new(stream);
                let mut subscribed = HashSet::new();
                while !stopping.load(Ordering::Relaxed) {
                    let mut line = String::new();
                    match reader.read_line(&mut line) {
                        Ok(0) => break,
                        Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut) => continue,
                        Err(_) => break,
                        Ok(_) => (),
                    }
                    let request: Value = serde_json::from_str(&line).unwrap();
                    let method = request["method"].as_str().unwrap();
                    recorded.lock().unwrap().push(method.to_string());
                    let hash = request["params"][0].as_str().unwrap_or_default().to_owned();
                    if method == "blockchain.scripthash.subscribe" { subscribed.insert(hash.clone()); }
                    if method == "blockchain.scripthash.get_history" {
                        assert!(subscribed.contains(&hash), "history queried without subscription on this connection");
                    }
                    if method == "blockchain.scripthash.unsubscribe" { subscribed.remove(&hash); }
                    let txs = data.lock().unwrap();

                    let header = bytes_to_hex(&encode::serialize(&bdk_wallet::bitcoin::blockdata::constants::genesis_block(Network::Regtest).header));
                    let result = match method {
                        "blockchain.scripthash.subscribe" => if txs.is_empty() { Value::Null } else {
                            json!(sha256::Hash::hash(txs.iter().map(|(tx, h)| format!("{}:{h}:", tx.compute_txid())).collect::<String>().as_bytes()).to_string())
                        },
                        "blockchain.scripthash.unsubscribe" => json!(true),
                        "blockchain.scripthash.get_history" => json!(txs.iter().map(|(tx, height)| json!({"tx_hash":tx.compute_txid().to_string(), "height":height})).collect::<Vec<_>>()),
                        "blockchain.scripthash.listunspent" => json!(txs.iter().map(|(tx, height)| json!({"tx_hash":tx.compute_txid().to_string(), "height":height, "tx_pos":0, "value":tx.output[0].value.to_sat()})).collect::<Vec<_>>()),
                        "blockchain.transaction.get" => {

                            let id = request["params"][0].as_str().unwrap();
                            json!(bytes_to_hex(&encode::serialize(&txs.iter().find(|(tx, _)| tx.compute_txid().to_string() == id).unwrap().0)))
                        },
                        "blockchain.block.header" => json!(header),
                        "blockchain.headers.subscribe" => json!({"height":200,"hex":header}),
                        other => panic!("unexpected or mutating RPC: {other}"),
                    };
                    let mode = limited.load(Ordering::Relaxed);
                    let response = if (mode == 2 && method == "blockchain.scripthash.subscribe")
                        || (mode != 0 && method == "blockchain.scripthash.get_history") {
                        json!({"id":request["id"],"error":{"code":-1,"message":"too many history entries"}})
                    } else if mode == 3 && method == "blockchain.scripthash.listunspent" {
                        json!({"id":request["id"],"result":{}})
                    } else if failing.load(Ordering::Relaxed) && method == "blockchain.transaction.get" {

                        json!({"id":request["id"],"error":{"code":-1,"message":"injected transaction read failure"}})
                    } else { json!({"id":request["id"],"result":result}) };
                    if writeln!(reader.get_mut(), "{response}").is_err() { break; }
                }
            }
        });
        Self { config, txs, calls, fail, limit, stop, join: Some(join) }
    }
    fn count(&self, method: &str) -> usize {
        self.calls.lock().unwrap().iter().filter(|m| m.as_str() == method).count()
    }
}
impl Drop for Node {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        self.join.take().unwrap().join().unwrap();
    }
}

struct Fixture { dir: PathBuf, local: Option<LocalWallet>, script: ScriptBuf }
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let dir = std::env::temp_dir().join(format!("incremental-electrum-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let mnemonic = bdk_wallet::bip39::Mnemonic::parse(MNEMONIC).unwrap();
        let mut local = LocalWallet::open_with_mnemonic(&dir, Network::Regtest, &mnemonic).unwrap();
        let script = local.wallet.reveal_next_address(KeychainKind::External).address.script_pubkey();
        local.persist().unwrap();
        Self { dir, local: Some(local), script }
    }
    fn tx(&self, amount: u64, previous_output: OutPoint) -> Transaction {
        Transaction {
            version: bdk_wallet::bitcoin::transaction::Version::TWO,
            lock_time: bdk_wallet::bitcoin::absolute::LockTime::ZERO,
            input: vec![bdk_wallet::bitcoin::TxIn { previous_output, ..Default::default() }],
            output: vec![TxOut { value: Amount::from_sat(amount), script_pubkey: self.script.clone() }],
        }
    }
    fn run(&mut self, node: &Node) -> Result<()> {
        let local = self.local.as_mut().unwrap();
        sync(local, &node.config)?;
        local.persist()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { self.local.take(); let _ = fs::remove_dir_all(&self.dir); }
}

#[test]
fn unchanged_history_and_existing_transactions_are_reused_then_spend_is_detected() {
    let mut fixture = Fixture::new();
    let first = fixture.tx(310_001, OutPoint { txid: Txid::from_str(&"11".repeat(32)).unwrap(), vout: 0 });
    let first_id = first.compute_txid();
    let node = Node::new(vec![(first.clone(), 1)]);
    fixture.run(&node).unwrap();
    assert_eq!(node.count("blockchain.transaction.get"), 1);
    assert_eq!(node.count("blockchain.scripthash.get_history"), 1);
    fixture.run(&node).unwrap();
    assert_eq!(node.count("blockchain.transaction.get"), 1);
    assert_eq!(node.count("blockchain.scripthash.get_history"), 1);
    // Reopen from the persisted BDK store. No in-memory wallet is required.
    fixture.local.take();
    let mnemonic = bdk_wallet::bip39::Mnemonic::parse(MNEMONIC).unwrap();
    fixture.local = Some(LocalWallet::open_with_mnemonic(&fixture.dir, Network::Regtest, &mnemonic).unwrap());
    let spent = fixture.tx(300_001, OutPoint { txid: first_id, vout: 0 });
    let spent_id = spent.compute_txid();
    *node.txs.lock().unwrap() = vec![(first, 1), (spent, 1)];
    fixture.run(&node).unwrap();
    assert_eq!(node.count("blockchain.transaction.get"), 2);
    assert_eq!(node.count("blockchain.scripthash.get_history"), 2);
    assert_eq!(node.count("blockchain.scripthash.subscribe"), node.count("blockchain.scripthash.unsubscribe"));
    let utxos = fixture.local.as_ref().unwrap().wallet.list_unspent().map(|u| u.outpoint.txid).collect::<Vec<_>>();
    assert!(!utxos.contains(&first_id));
    assert!(utxos.contains(&spent_id));
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}

#[test]
fn failed_transaction_batch_never_applies_partial_wallet_update_or_cache_marker() {
    let mut fixture = Fixture::new();
    let tx = fixture.tx(410_001, OutPoint { txid: Txid::from_str(&"22".repeat(32)).unwrap(), vout: 0 });
    let id = tx.compute_txid();
    let node = Node::new(vec![(tx, 1)]);
    node.fail.store(true, Ordering::Relaxed);
    assert!(fixture.run(&node).is_err());
    assert!(fixture.local.as_ref().unwrap().wallet.tx_graph().get_tx(id).is_none());
    assert!(!fixture.dir.join(BDK_SYNC_CACHE_FILE).exists());
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
    node.fail.store(false, Ordering::Relaxed);
    fixture.run(&node).unwrap();
    assert!(fixture.local.as_ref().unwrap().wallet.tx_graph().get_tx(id).is_some());
}

#[test]
fn history_digest_change_and_wrong_transaction_identity_are_rejected() {
    let id = Txid::from_str(&"33".repeat(32)).unwrap();
    let value = json!([{"tx_hash":id.to_string(),"height":-1}]);
    let digest = sha256::Hash::hash(format!("{id}:-1:").as_bytes()).to_string();
    assert_eq!(parse_history(&value, &digest).unwrap()[0].height, -1);
    assert!(parse_history(&value, &"00".repeat(32)).is_err());
    assert!(parse_history(&json!({}), &digest).is_err());
    assert!(parse_history(&json!([{"tx_hash":id.to_string(),"height":-2}]), &digest).is_err());
    let fixture = Fixture::new();
    let tx = fixture.tx(510_001, OutPoint { txid: id, vout: 0 });
    assert!(decode_transaction(&json!(bytes_to_hex(&encode::serialize(&tx))), id).is_err());
}

#[test]
fn historical_snapshot_cache_has_bounded_keys_and_entries() {
    let mut cache = Cache::default();
    let entries = Arc::new(vec![Entry { txid: Txid::from_str(&"44".repeat(32)).unwrap(), height: 1 }]);
    for index in 0..MAX_HISTORY_KEYS + 10 {
        cache.remember_history(("endpoint".into(), index.to_string()), "digest".into(), entries.clone());
        assert!(cache.histories.len() <= MAX_HISTORY_KEYS);
        assert!(cache.history_entries <= MAX_HISTORY_ENTRIES);
    }
    cache.remember_history(("endpoint".into(), "large".into()), "digest".into(),
        Arc::new(vec![entries[0].clone(); MAX_HISTORY_ENTRIES + 1]));
    assert!(!cache.histories.contains_key(&("endpoint".into(), "large".into())));
}

#[test]
fn history_limit_fallback_is_refetched_and_never_cached_as_complete() {
    let mut fixture = Fixture::new();
    let tx = fixture.tx(610_001, OutPoint { txid: Txid::from_str(&"55".repeat(32)).unwrap(), vout: 0 });
    let node = Node::new(vec![(tx, 1)]);
    for mode in [1, 2] {
        node.limit.store(mode, Ordering::Relaxed);
        fixture.run(&node).unwrap();
        assert!(cache().histories.keys().all(|(url, _)| url != &node.config.url));
    }
    assert_eq!(node.count("blockchain.scripthash.listunspent"), 2);
    assert_eq!(node.count("blockchain.transaction.get"), 1);
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}

#[test]
fn malformed_history_limit_fallback_fails_without_applying_or_broadcasting() {
    let mut fixture = Fixture::new();
    let tx = fixture.tx(710_001, OutPoint { txid: Txid::from_str(&"66".repeat(32)).unwrap(), vout: 0 });
    let id = tx.compute_txid();
    let node = Node::new(vec![(tx, 1)]);
    node.limit.store(3, Ordering::Relaxed);
    assert!(fixture.run(&node).is_err());
    assert!(fixture.local.as_ref().unwrap().wallet.tx_graph().get_tx(id).is_none());
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}
