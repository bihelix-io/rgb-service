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
    header_time: Arc<AtomicUsize>,
    header_delay_ms: Arc<AtomicUsize>,
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
        let header_time = Arc::new(AtomicUsize::new(0));
        let header_override = header_time.clone();
        let header_delay_ms = Arc::new(AtomicUsize::new(0));
        let header_delay = header_delay_ms.clone();
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
                stream.set_nodelay(true).unwrap();
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
                    if method == "blockchain.block.header" {
                        std::thread::sleep(Duration::from_millis(header_delay.load(Ordering::Relaxed) as u64));
                    }
                    let hash = request["params"][0].as_str().unwrap_or_default().to_owned();
                    if method == "blockchain.scripthash.subscribe" { subscribed.insert(hash.clone()); }
                    if method == "blockchain.scripthash.get_history" {
                        assert!(subscribed.contains(&hash), "history queried without subscription on this connection");
                    }
                    if method == "blockchain.scripthash.unsubscribe" { subscribed.remove(&hash); }
                    let txs = data.lock().unwrap();

                    let mut header = bdk_wallet::bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
                    let override_time = header_override.load(Ordering::Relaxed);
                    if override_time != 0 { header.time = override_time as u32; }
                    let header = bytes_to_hex(&encode::serialize(&header));
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
        Self { config, txs, calls, fail, limit, header_time, header_delay_ms, stop, join: Some(join) }
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

fn history_entries(count: usize) -> Arc<Vec<Entry>> {
    Arc::new(vec![Entry { txid: Txid::from_str(&"77".repeat(32)).unwrap(), height: 1 }; count])
}

#[test]
fn history_capacity_evicts_one_lru_entry_and_preserves_recent_hits() {
    let mut cache = Cache::default();
    let key = |i: usize| ("endpoint".to_owned(), i.to_string());
    for i in 0..MAX_HISTORY_KEYS {
        cache.remember_history(key(i), "one".into(), history_entries(1));
    }
    assert!(cache.history(&key(0), "one").is_some());
    cache.remember_history(key(MAX_HISTORY_KEYS), "one".into(), history_entries(1));
    assert_eq!(cache.histories.len(), MAX_HISTORY_KEYS);
    assert_eq!(cache.history_entries, MAX_HISTORY_KEYS);
    assert!(cache.histories.contains_key(&key(0)));
    assert!(!cache.histories.contains_key(&key(1)));
    assert_eq!(cache.counters.history_evictions, 1);
    assert_eq!(cache.counters.history_hits, 1);
    assert!(cache.history(&key(0), "changed").is_none());
    assert!(!cache.histories.contains_key(&key(0)));
    assert_eq!(cache.history_entries, MAX_HISTORY_KEYS - 1);
    assert_eq!(cache.counters.status_changes, 1);
    assert_eq!(cache.counters.history_misses, 1);
}

#[test]
fn history_entry_budget_replacement_and_oversized_admission_are_bounded() {
    let mut cache = Cache::default();
    let key = |s: &str| ("endpoint".to_owned(), s.to_owned());
    cache.remember_history(key("a"), "one".into(), history_entries(20_000));
    cache.remember_history(key("b"), "one".into(), history_entries(20_000));
    cache.remember_history(key("c"), "one".into(), history_entries(20_000));
    assert!(!cache.histories.contains_key(&key("a")));
    assert_eq!(cache.histories.len(), 2);
    assert_eq!(cache.history_entries, 40_000);
    assert_eq!(cache.counters.history_evictions, 1);
    cache.remember_history(key("b"), "two".into(), history_entries(1));
    assert_eq!(cache.history_entries, 20_001);
    cache.remember_history(key("huge"), "one".into(), history_entries(MAX_HISTORY_ENTRIES + 1));
    assert_eq!(cache.history_entries, 20_001);
    assert_eq!(cache.histories.len(), 2);
    assert_eq!(cache.counters.history_oversized, 1);
    // A changed oversized value must not leave the previous digest usable.
    cache.remember_history(key("b"), "three".into(), history_entries(MAX_HISTORY_ENTRIES + 1));
    assert!(cache.history(&key("b"), "two").is_none());
    assert_eq!(cache.history_entries, 20_000);
}

#[test]
fn transaction_cache_evicts_only_needed_entries_and_accounts_bytes() {
    let fixture = Fixture::new();
    let previous = OutPoint { txid: Txid::from_str(&"88".repeat(32)).unwrap(), vout: 0 };
    let mut cache = Cache::default();
    let txs = (0..=MAX_TX_KEYS).map(|i| Arc::new(fixture.tx(i as u64 + 1, previous))).collect::<Vec<_>>();
    for tx in &txs[..MAX_TX_KEYS] { cache.remember_transaction(tx.clone()); }
    assert!(cache.transaction(&txs[0].compute_txid()).is_some());
    cache.remember_transaction(txs[MAX_TX_KEYS].clone());
    assert_eq!(cache.transactions.len(), MAX_TX_KEYS);
    assert!(cache.transactions.contains_key(&txs[0].compute_txid()));
    assert!(!cache.transactions.contains_key(&txs[1].compute_txid()));
    assert_eq!(cache.counters.tx_evictions, 1);
    assert_eq!(cache.tx_bytes, cache.transactions.values().map(|v| v.bytes).sum::<usize>());
    let mut large = fixture.tx(900_001, previous);
    large.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; MAX_TX_BYTES / 2]);
    cache.remember_transaction(Arc::new(large.clone()));
    large.output[0].value = Amount::from_sat(900_002);
    let latest = large.compute_txid();
    cache.remember_transaction(Arc::new(large.clone()));
    assert!(cache.transactions.contains_key(&latest));
    assert!(cache.tx_bytes <= MAX_TX_BYTES);
    assert_eq!(cache.tx_bytes, cache.transactions.values().map(|v| v.bytes).sum::<usize>());
    let bytes = cache.tx_bytes;
    large.output[0].script_pubkey = ScriptBuf::from_bytes(vec![0; MAX_TX_BYTES + 1]);
    cache.remember_transaction(Arc::new(large));
    assert_eq!(cache.tx_bytes, bytes);
    assert_eq!(cache.counters.tx_oversized, 1);
}

#[test]
fn oversized_sequential_scan_reuses_retained_histories_across_three_rounds() {
    let fixture = Fixture::new();
    let tx = fixture.tx(810_001, OutPoint { txid: Txid::from_str(&"99".repeat(32)).unwrap(), vout: 0 });
    let node = Node::new(vec![(tx, 1)]);
    let scripts = (0..MAX_HISTORY_KEYS + 32)
        .map(|i| ScriptBuf::from_bytes((i as u32).to_le_bytes().to_vec())).collect::<Vec<_>>();
    for round in 0..3 {
        let before = node.count("blockchain.scripthash.get_history");
        let results = histories(&node.config, &scripts).unwrap();
        assert_eq!(results.len(), scripts.len());
        assert!(results.iter().all(|h| h.complete && h.entries.len() == 1));
        let downloads = node.count("blockchain.scripthash.get_history") - before;
        assert_eq!(downloads, if round == 0 { scripts.len() } else { 32 });
        assert!(cache().histories.len() <= MAX_HISTORY_KEYS);
    }
    // Status changes still invalidate every retained history; no stale balance.
    node.txs.lock().unwrap()[0].1 = 2;
    let before = node.count("blockchain.scripthash.get_history");
    let results = histories(&node.config, &scripts).unwrap();
    assert_eq!(node.count("blockchain.scripthash.get_history") - before, scripts.len());
    assert!(results.iter().all(|h| h.entries[0].height == 2));
    assert_eq!(node.count("blockchain.scripthash.subscribe"), node.count("blockchain.scripthash.unsubscribe"));
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}

#[test]
fn cached_first_order_is_a_stable_permutation_not_a_changed_result_mapping() {
    let mut cache = Cache::default();
    let hashes = vec!["a".into(), "b".into(), "c".into(), "d".into()];
    cache.remember_history(("endpoint".into(), "c".into()), "one".into(), history_entries(1));
    cache.remember_history(("other".into(), "a".into()), "one".into(), history_entries(1));
    let indices = ordered_script_indices("endpoint", &hashes, &cache);
    assert_eq!(indices, vec![2, 0, 1, 3]);
    let mut restored = vec![String::new(); hashes.len()];
    for i in indices { restored[i] = hashes[i].clone(); }
    assert_eq!(restored, hashes);
}

#[test]
fn production_script_working_set_is_retained_and_status_changes_are_refetched() {
    let fixture = Fixture::new();
    let tx = fixture.tx(820_001, OutPoint { txid: Txid::from_str(&"ab".repeat(32)).unwrap(), vout: 0 });
    let node = Node::new(vec![(tx, 1)]);
    let scripts = (0..1783u32).map(|i| ScriptBuf::from_bytes(i.to_le_bytes().to_vec())).collect::<Vec<_>>();
    for round in 0..3 {
        let before = node.count("blockchain.scripthash.get_history");
        let result = histories(&node.config, &scripts).unwrap();
        assert_eq!(result.len(), 1783);
        assert_eq!(node.count("blockchain.scripthash.get_history") - before, if round == 0 { 1783 } else { 0 });
        assert!(result.iter().all(|h| h.complete && h.entries[0].height == 1));
    }
    node.txs.lock().unwrap()[0].1 = 2;
    let before = node.count("blockchain.scripthash.get_history");
    assert!(histories(&node.config, &scripts).unwrap().iter().all(|h| h.entries[0].height == 2));
    assert_eq!(node.count("blockchain.scripthash.get_history") - before, 1783);
    assert_eq!(node.count("blockchain.scripthash.subscribe"), node.count("blockchain.scripthash.unsubscribe"));
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}

#[test]
fn production_headers_are_bounded_batched_deduplicated_and_revalidated() {
    let node = Node::new(vec![]);
    let mut heights = (1..=1937).collect::<Vec<u32>>();
    heights.extend([1, 42, 1937]);
    assert_eq!(1937usize.div_ceil(HEADER_BATCH), 31);
    let first = confirmation_headers(&node.config, &heights).unwrap();
    assert_eq!(first.len(), 1937);
    assert_eq!(node.count("blockchain.block.header"), 1937);
    // Same height and history may belong to a replacement block after a reorg.
    node.header_time.store(1_700_000_000, Ordering::Relaxed);
    let second = confirmation_headers(&node.config, &heights).unwrap();
    assert_eq!(node.count("blockchain.block.header"), 3874);
    assert_ne!(first[&42].block_hash(), second[&42].block_hash());
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}

#[test]
fn header_deadline_never_applies_partial_wallet_or_broadcasts() {
    let mut fixture = Fixture::new();
    let tx = fixture.tx(830_001, OutPoint { txid: Txid::from_str(&"ac".repeat(32)).unwrap(), vout: 0 });
    let id = tx.compute_txid();
    let node = Node::new(vec![(tx, 1)]);
    node.header_delay_ms.store(1200, Ordering::Relaxed);
    let budget = sync_budget::WalletOperationBudget::enter(Duration::from_secs(1));
    assert!(fixture.run(&node).is_err());
    assert_eq!(node.count("blockchain.transaction.get"), 1);
    assert_eq!(node.count("blockchain.block.header"), 1);
    assert!(fixture.local.as_ref().unwrap().wallet.tx_graph().get_tx(id).is_none());
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
    drop(budget);
    node.header_delay_ms.store(0, Ordering::Relaxed);
    fixture.run(&node).unwrap();
    assert!(fixture.local.as_ref().unwrap().wallet.tx_graph().get_tx(id).is_some());
    assert_eq!(node.count("blockchain.block.header"), 2);
    assert_eq!(node.count("blockchain.transaction.broadcast"), 0);
}
