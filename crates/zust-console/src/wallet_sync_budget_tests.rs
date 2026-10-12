use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

const MNEMONIC: &str = "abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about";

fn test_dir() -> PathBuf {
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let dir = std::env::temp_dir().join(format!("wallet-budget-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
    fs::create_dir_all(&dir).unwrap();
    dir
}
fn wallet(dir: &Path) -> LocalWallet {
    let mnemonic = bdk_wallet::bip39::Mnemonic::parse(MNEMONIC).unwrap();
    LocalWallet::open_with_mnemonic(dir, Network::Regtest, &mnemonic).unwrap()
}

#[test]
fn expired_scope_restores_parent_and_does_not_leak_to_next_operation() {
    {
        let _outer = sync_budget::WalletOperationBudget::enter(Duration::from_secs(1));
        {
            let _inner = sync_budget::WalletOperationBudget::enter(Duration::ZERO);
            assert!(sync_budget::check().is_err());
        }
        sync_budget::check().unwrap();
    }
    sync_budget::check().unwrap();
}

#[test]
fn nested_scope_cannot_extend_whole_operation_deadline() {
    let _outer = sync_budget::WalletOperationBudget::enter(Duration::ZERO);
    let _inner = sync_budget::WalletOperationBudget::enter(Duration::from_secs(120));
    assert!(sync_budget::check().is_err());
}

#[test]
fn wallet_lock_wait_honors_budget_and_releases_after_holder_drops() {
    let dir = test_dir();
    let holder = wallet(&dir);
    let mnemonic = bdk_wallet::bip39::Mnemonic::parse(MNEMONIC).unwrap();
    let started = Instant::now();
    {
        let _budget = sync_budget::WalletOperationBudget::enter(Duration::from_millis(40));
        let result = LocalWallet::open_with_mnemonic(&dir, Network::Regtest, &mnemonic);
        assert!(result.is_err());
        assert!(started.elapsed() < Duration::from_secs(2));
    }
    drop(holder);
    drop(wallet(&dir));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn header_memo_is_scoped_and_never_caches_broadcasts() {
    let params = json!([1]);
    {
        let _scope = sync_budget::WalletOperationBudget::enter(Duration::from_secs(1));
        sync_budget::remember_header("a", "blockchain.block.header", &params, &json!("header"));
        assert_eq!(sync_budget::cached_header("a", "blockchain.block.header", &params), Some(json!("header")));
        assert!(sync_budget::cached_header("b", "blockchain.block.header", &params).is_none());
        sync_budget::remember_header("a", "blockchain.transaction.broadcast", &params, &json!("tx"));
        assert!(sync_budget::cached_header("a", "blockchain.transaction.broadcast", &params).is_none());
    }
    assert!(sync_budget::cached_header("a", "blockchain.block.header", &params).is_none());
}

#[test]
fn expired_rpc_never_connects_or_broadcasts() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let config = ElectrumConfig::new(format!("electrum://{}", listener.local_addr().unwrap()));
    let _scope = sync_budget::WalletOperationBudget::enter(Duration::ZERO);
    assert!(electrum_rpc(&config, "blockchain.transaction.broadcast", json!(["00"])).is_err());
    assert_eq!(listener.accept().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn slow_sync_returns_without_cache_marker_or_late_work() {
    let dir = test_dir();
    let mut local = wallet(&dir);
    local.wallet.reveal_next_address(KeychainKind::External);
    local.persist().unwrap();
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let config = ElectrumConfig::new(format!("electrum://{}", listener.local_addr().unwrap()));
    let calls = Arc::new(AtomicUsize::new(0));
    let count = calls.clone();
    let server = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        count.fetch_add(1, Ordering::Relaxed);
        let request: Value = serde_json::from_str(&line).unwrap();
        std::thread::sleep(Duration::from_millis(180));
        let _ = writeln!(stream, "{}", json!({"id":request["id"], "result":[]}));
        line.clear();
        if reader.read_line(&mut line).unwrap_or(0) > 0 { count.fetch_add(1, Ordering::Relaxed); }
    });
    let started = Instant::now();
    {
        let _scope = sync_budget::WalletOperationBudget::enter(Duration::from_millis(80));
        assert!(sync_wallet_cached_with_chain_source(&mut local, &ChainSource::Electrum(config), Duration::from_secs(30)).is_err());
    }
    assert!(started.elapsed() < Duration::from_secs(2));
    assert!(!dir.join(BDK_SYNC_CACHE_FILE).exists());
    server.join().unwrap();
    // A timed-out subscription must not start history or unsubscribe work.
    assert_eq!(calls.load(Ordering::Relaxed), 1);
    drop(local);
    drop(wallet(&dir));
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn recent_persisted_sync_is_reused_but_wallet_mutation_invalidates_it() {
    let dir = test_dir();
    let mut local = wallet(&dir);
    local.persist().unwrap();
    let source = ChainSource::Electrum(ElectrumConfig::new("electrum://127.0.0.1:1".into()));
    fs::write(dir.join(BDK_SYNC_CACHE_FILE), serde_json::to_vec(&BdkSyncCache {
        chain_source: source.cache_key(), esplora: String::new(), synced_at_secs: now_secs(),
    }).unwrap()).unwrap();
    assert!(!sync_wallet_cached_with_chain_source(&mut local, &source, Duration::from_secs(30)).unwrap());
    local.wallet.reveal_next_address(KeychainKind::External);
    local.persist().unwrap();
    assert!(!dir.join(BDK_SYNC_CACHE_FILE).exists());
    drop(local);
    fs::remove_dir_all(dir).unwrap();
}

#[test]
fn history_sync_deduplicates_transactions_and_headers_without_verbose_rpc() {
    let dir = test_dir();
    let mut local = wallet(&dir);
    let address = local.wallet.reveal_next_address(KeychainKind::External);
    local.wallet.reveal_next_address(KeychainKind::Internal);
    local.persist().unwrap();
    let tx = Transaction {
        version: bdk_wallet::bitcoin::transaction::Version::TWO,
        lock_time: bdk_wallet::bitcoin::absolute::LockTime::ZERO,
        input: vec![bdk_wallet::bitcoin::TxIn::default()],
        output: vec![TxOut { value: Amount::from_sat(100_000), script_pubkey: address.address.script_pubkey() }],
    };
    let txid = tx.compute_txid();
    let header = bdk_wallet::bitcoin::blockdata::constants::genesis_block(Network::Regtest).header;
    let header_hex = bytes_to_hex(&encode::serialize(&header));
    let raw_tx = bytes_to_hex(&encode::serialize(&tx));
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let source = ChainSource::Electrum(ElectrumConfig::new(format!("electrum://{}", listener.local_addr().unwrap())));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopped = stop.clone();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let recorded = calls.clone();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(Duration::from_millis(50))).unwrap();
        let mut reader = BufReader::new(stream);
        while !stopped.load(Ordering::Relaxed) {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Err(err) if matches!(err.kind(), std::io::ErrorKind::TimedOut | std::io::ErrorKind::WouldBlock) => continue,
                other => { other.unwrap(); }
            }
            let request: Value = serde_json::from_str(&line).unwrap();
            let method = request["method"].as_str().unwrap();
            recorded.lock().unwrap().push(method.to_owned());
            let result = match method {
                "blockchain.scripthash.subscribe" => json!(sha256::Hash::hash(format!("{txid}:1:").as_bytes()).to_string()),
                "blockchain.scripthash.unsubscribe" => json!(true),
                "blockchain.scripthash.get_history" => json!([{"tx_hash":txid.to_string(), "height":1}]),
                "blockchain.transaction.get" => { assert!(request["params"].as_array().unwrap().len() == 1); json!(raw_tx) },
                "blockchain.block.header" => json!(header_hex),
                "blockchain.headers.subscribe" => json!({"height":200, "hex":header_hex}),
                other => panic!("unexpected RPC {other}"),
            };
            writeln!(reader.get_mut(), "{}", json!({"id":request["id"], "result":result})).unwrap();
        }
    });
    let result = sync_wallet_cached_with_chain_source(&mut local, &source, Duration::from_secs(30));
    stop.store(true, Ordering::Relaxed);
    server.join().unwrap();
    assert!(result.unwrap());
    let methods = calls.lock().unwrap();
    assert_eq!(methods.iter().filter(|m| *m == "blockchain.transaction.get").count(), 1);
    assert_eq!(methods.iter().filter(|m| *m == "blockchain.block.header").count(), 1);
    assert!(!sync_wallet_cached_with_chain_source(&mut local, &source, Duration::from_secs(30)).unwrap());
    assert!(local.wallet.list_unspent().any(|u| u.outpoint.txid == txid));
    drop(local);
    drop(wallet(&dir));
    fs::remove_dir_all(dir).unwrap();
}
