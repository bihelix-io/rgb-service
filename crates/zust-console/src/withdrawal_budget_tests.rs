use super::*;
use std::io::{BufRead, BufReader};
use std::net::TcpListener;
use std::sync::atomic::AtomicU64;

struct Fixture {
    backend: LnRgbBtcLnBackend,
    dir: PathBuf,
}
impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!("withdrawal-budget-{}-{}", std::process::id(), NEXT.fetch_add(1, Ordering::Relaxed)));
        let config = BtcLnRuntimeConfig {
            backend: crate::btc_ln::BtcLnBackendKind::LnRgb,
            network: Network::Regtest,
            l1_data_dir: dir.join("l1"), storage_dir: dir.clone(),
            esplora: "electrum://127.0.0.1:1".into(),
            esplora_urls: vec!["electrum://127.0.0.1:1".into()],
            esplora_api_key: None, rgb_service_url: "http://127.0.0.1:1".into(),
            account_id: "offline-test".into(), listen: None,
            entropy_mnemonic: Some("abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about".into()),
            trusted_peers_0conf: vec![], accept_inbound_channels: false, announce_for_forwarding: false,
        };
        Self { backend: LnRgbBtcLnBackend::new(config), dir }
    }
    fn transaction(local: &mut LocalWallet) -> Transaction {
        Transaction {
            version: bitcoin::transaction::Version::TWO, lock_time: LockTime::ZERO,
            input: vec![bitcoin::TxIn {
                previous_output: OutPoint {
                    txid: "1111111111111111111111111111111111111111111111111111111111111111".parse().unwrap(),
                    vout: 0,
                },
                ..bitcoin::TxIn::default()
            }],
            output: vec![TxOut { value: Amount::from_sat(10_000),
                script_pubkey: local.wallet.reveal_next_address(KeychainKind::External).address.script_pubkey() }],
        }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) { let _ = fs::remove_dir_all(&self.dir); }
}

#[test]
fn expired_withdrawal_never_enters_broadcast() {
    let f = Fixture::new();
    let mut local = f.backend.open_l1_wallet().unwrap();
    let tx = Fixture::transaction(&mut local);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let source = ChainSource::Electrum(ElectrumConfig::new(format!("electrum://{}", listener.local_addr().unwrap())));
    let _scope = sync_budget::WalletOperationBudget::enter(Duration::ZERO);
    assert!(f.backend.broadcast_l1_withdrawal(&mut local, &source, &tx).is_err());
    assert_eq!(listener.accept().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
}

#[test]
fn non_electrum_withdrawal_never_connects_or_stages_payment() {
    let f = Fixture::new();
    let mut local = f.backend.open_l1_wallet().unwrap();
    let tx = Fixture::transaction(&mut local);
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let source = ChainSource::from_esplora_or_default(Network::Regtest, Some(&url)).unwrap();
    let error = f.backend.broadcast_l1_withdrawal(&mut local, &source, &tx).unwrap_err();
    assert!(error.to_string().contains("BTC_WITHDRAWAL_ELECTRUM_REQUIRED"));
    assert!(local.wallet.get_tx(tx.compute_txid()).is_none());
    assert_eq!(listener.accept().unwrap_err().kind(), std::io::ErrorKind::WouldBlock);
}

fn mock_broadcast(mode: &str) {
    let f = Fixture::new();
    let mut local = f.backend.open_l1_wallet().unwrap();
    let tx = Fixture::transaction(&mut local);
    let txid = tx.compute_txid();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let source = ChainSource::Electrum(ElectrumConfig::new(format!("electrum://{}", listener.local_addr().unwrap())));
    let mode_owned = mode.to_owned();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let request: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(request["method"], "blockchain.transaction.broadcast");
        match mode_owned.as_str() {
            "success" => writeln!(reader.get_mut(), "{}", json!({"id":request["id"],"result":txid.to_string()})).unwrap(),
            "wrong_txid" => writeln!(reader.get_mut(), "{}", json!({"id":request["id"],"result":"wrong"})).unwrap(),
            "timeout" => std::thread::sleep(Duration::from_millis(180)),
            "disconnect" => {},
            _ => panic!("unknown mode"),
        }
        // Closing the peer and listener ensures a hidden whole-transfer retry
        // or public Esplora fallback would not pass this success/latency test.
    });
    let started = Instant::now();
    let _scope = sync_budget::WalletOperationBudget::enter(if mode == "timeout" { Duration::from_millis(80) } else { Duration::from_secs(2) });
    let result = withdrawal_attempt::run(|attempt| f.backend.broadcast_l1_withdrawal_tracked(&mut local, &source, &tx, attempt));
    if let Err(error) = &result { assert_eq!(withdrawal_attempt::failure(error)["broadcast_state"], "outcome_unknown"); }
    assert!(started.elapsed() < Duration::from_secs(3));
    if mode == "success" { assert_eq!(result.unwrap(), txid); } else { assert!(result.is_err()); }
    // Unknown outcomes remain in the wallet and are not silently refunded or
    // made available for a new payment merely because the RPC failed.
    assert!(local.wallet.get_tx(txid).is_some());
    server.join().unwrap();
}

#[test]
fn successful_broadcast_keeps_original_transaction() { mock_broadcast("success"); }
#[test]
fn disconnected_broadcast_does_not_retry_or_fallback() { mock_broadcast("disconnect"); }
#[test]
fn timed_out_broadcast_does_not_retry_or_fallback() { mock_broadcast("timeout"); }
#[test]
fn mismatched_reply_is_not_a_success() { mock_broadcast("wrong_txid"); }

#[test]
fn invalid_batch_is_proven_terminated_without_broadcast() {
    let f=Fixture::new();
    let error=f.backend.transfer_l1_batch_with_inputs_json("",2,"").unwrap_err();
    assert_eq!(withdrawal_attempt::failure(&error)["broadcast_state"],"not_attempted");
}

#[test]
fn withdrawal_lock_timeout_is_proven_terminated() {
    let f=Fixture::new();
    let mut holder=f.backend.open_l1_wallet().unwrap();
    let address=holder.wallet.reveal_next_address(KeychainKind::External).address.to_string();
    let _budget=sync_budget::WalletOperationBudget::enter(Duration::from_millis(40));
    let error=f.backend.transfer_l1_with_inputs_json(&address,1000,2,"1111111111111111111111111111111111111111111111111111111111111111:0").unwrap_err();
    assert_eq!(withdrawal_attempt::failure(&error)["broadcast_state"],"not_attempted");
    assert_eq!(withdrawal_attempt::failure(&error)["task_terminated"],true);
}
