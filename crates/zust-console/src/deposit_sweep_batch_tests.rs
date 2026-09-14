use super::*;
use std::net::TcpListener;

fn address(index: u8) -> String {
    let secp = bitcoin::secp256k1::Secp256k1::new();
    let secret = bitcoin::secp256k1::SecretKey::from_slice(&[index; 32]).unwrap();
    Address::p2wpkh(&CompressedPublicKey(PublicKey::from_secret_key(&secp, &secret)), Network::Bitcoin).to_string()
}

fn utxo(index: usize, value: u64) -> Value {
    json!({"tx_hash": format!("{index:064x}"), "tx_pos": 0, "value": value, "height": 800000})
}

fn candidate(count: usize) -> Candidate {
    let utxos = normalize_utxos(Value::Array((1..=count).map(|i| utxo(i, 10000)).collect()), true)
        .unwrap().as_array().unwrap().clone();
    Candidate {
        ident: "local-test-account".into(), address: address(1), custody: json!("10000"),
        safe: utxos.clone(), utxos, protected: vec![], failed: vec![], error: None,
    }
}

// One accepted TCP connection must carry every listunspent request in a batch.
fn electrum_mock(count: usize, omit_last: bool, fail_one: bool) -> (String, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let source = format!("electrum://{}", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let job = thread::spawn(move || {
        let end = Instant::now() + Duration::from_secs(3);
        let mut stream = loop {
            match listener.accept() {
                Ok((stream, _)) => break stream,
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    assert!(Instant::now() < end, "mock connection deadline");
                    thread::sleep(Duration::from_millis(2));
                }
                Err(e) => panic!("accept: {e}"),
            }
        };
        stream.set_nonblocking(false).unwrap();
        stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut calls = Vec::new();
        for _ in 0..count {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let call: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(call["method"], "blockchain.scripthash.listunspent");
            calls.push(call);
        }
        // Out-of-order replies exercise ID correlation, not array order.
        for call in calls.iter().rev() {
            let id = call["id"].as_u64().unwrap();
            if omit_last && id == (count - 1) as u64 { continue; }
            let response = if fail_one && id == 1 {
                json!({"id": id, "error": {"code": 1, "message": "mock address error"}})
            } else {
                json!({"id": id, "result": [utxo(id as usize + 1, 10000 + id)]})
            };
            writeln!(stream, "{response}").unwrap();
        }
        stream.flush().unwrap();
        if omit_last { thread::sleep(Duration::from_millis(250)); }
    });
    (source, job)
}

#[test]
fn electrum_reuses_connection_and_correlates_reversed_ids() {
    let (source, job) = electrum_mock(5, false, false);
    let addresses: Vec<_> = (1..=5).map(address).collect();
    let results = electrum_batch(&source, &addresses, Instant::now() + Duration::from_secs(2));
    job.join().unwrap();
    assert_eq!(results.len(), 5);
    for (id, result) in results.into_iter().enumerate() {
        assert_eq!(result.unwrap()[0]["value"], 10000 + id);
    }
}

#[test]
fn electrum_one_rpc_error_does_not_discard_other_addresses() {
    let (source, job) = electrum_mock(3, false, true);
    let results = electrum_batch(&source, &[address(1), address(2), address(3)], Instant::now() + Duration::from_secs(2));
    job.join().unwrap();
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
    assert!(results[2].is_ok());
}

#[test]
fn electrum_deadline_retains_completed_address_results() {
    let (source, job) = electrum_mock(2, true, false);
    let started = Instant::now();
    let results = electrum_batch(&source, &[address(1), address(2)], started + Duration::from_millis(120));
    assert!(started.elapsed() < Duration::from_secs(1));
    job.join().unwrap();
    assert!(results[0].is_ok());
    assert!(results[1].is_err());
}

#[test]
fn malformed_duplicate_and_excessive_utxos_fail_closed() {
    assert!(normalize_utxos(json!({}), true).is_err());
    assert!(normalize_utxos(json!([{"height": 1}]), true).is_err());
    assert!(normalize_utxos(json!([utxo(1, 1000), utxo(1, 1000)]), true).is_err());
    assert!(normalize_utxos(json!([utxo(1, 2_100_000_000_000_001)]), true).is_err());
    assert!(normalize_utxos(Value::Array(vec![utxo(1, 1000); MAX_INPUTS + 1]), true).is_err());
}

#[test]
fn unconfirmed_and_zero_value_outputs_are_not_selected() {
    let mut pending = utxo(2, 5000);
    pending["height"] = json!(0);
    let result = normalize_utxos(json!([utxo(1, 1000), pending, utxo(3, 0)]), true).unwrap();
    assert_eq!(result.as_array().unwrap().len(), 1);
    assert_eq!(result[0]["value"], 1000);
}

fn rgb_mock(batches: usize, malformed: bool) -> (String, thread::JoinHandle<Vec<usize>>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/v1/assets/by-utxo", listener.local_addr().unwrap());
    listener.set_nonblocking(true).unwrap();
    let job = thread::spawn(move || {
        let mut sizes = Vec::new();
        for _ in 0..batches {
            let end = Instant::now() + Duration::from_secs(3);
            let mut stream = loop {
                match listener.accept() {
                    Ok((stream, _)) => break stream,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        assert!(Instant::now() < end, "mock HTTP connection deadline");
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(e) => panic!("accept HTTP: {e}"),
                }
            };
            stream.set_nonblocking(false).unwrap();
            stream.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut length = None;
            loop {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                assert!(!line.is_empty());
                if line == "\r\n" { break; }
                if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = Some(value.trim().parse::<usize>().unwrap());
                }
            }
            let mut body = vec![0; length.unwrap()];
            reader.read_exact(&mut body).unwrap();
            let requests: Vec<Value> = serde_json::from_slice(&body).unwrap();
            sizes.push(requests.len());
            let responses: Vec<Value> = requests.into_iter().map(|request| {
                if malformed {
                    json!({"outpoint": request["outpoint"], "ok": true})
                } else {
                    json!({"outpoint": request["outpoint"], "address": request["address"],
                        "account_id": request["account_id"], "allocations": []})
                }
            }).collect();
            let body = serde_json::to_vec(&responses).unwrap();
            write!(stream, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).unwrap();
            stream.write_all(&body).unwrap();
            stream.flush().unwrap();
        }
        sizes
    });
    (url, job)
}

#[test]
fn rgb_checks_are_chunked_and_preserve_all_safe_inputs() {
    let (url, job) = rgb_mock(2, false);
    let mut candidates = vec![candidate(65)];
    candidates[0].safe.clear();
    classify_rgb(&mut candidates, &url, Instant::now() + Duration::from_secs(3));
    assert_eq!(job.join().unwrap(), vec![64, 1]);
    assert_eq!(candidates[0].safe.len(), 65);
    assert!(candidates[0].failed.is_empty());
}

#[test]
fn missing_rgb_allocations_are_not_treated_as_safe() {
    let (url, job) = rgb_mock(1, true);
    let mut candidates = vec![candidate(1)];
    candidates[0].safe.clear();
    classify_rgb(&mut candidates, &url, Instant::now() + Duration::from_secs(3));
    job.join().unwrap();
    assert!(candidates[0].safe.is_empty());
    assert_eq!(candidates[0].failed.len(), 1);
}

#[test]
fn expired_rgb_budget_skips_without_network_access() {
    let mut candidates = vec![candidate(2)];
    candidates[0].safe.clear();
    classify_rgb(&mut candidates, "http://127.0.0.1:1", Instant::now());
    assert!(candidates[0].safe.is_empty());
    assert_eq!(candidates[0].failed.len(), 2);
}

#[test]
fn zero_fee_rate_preserves_two_sat_vb_and_psbt_accounting() {
    let candidate = candidate(2);
    let sweep = build_sweep(&candidate, &address(2), 0, None).unwrap();
    assert_eq!(sweep["fee_rate_sat_vb"], "2");
    assert_eq!(sweep["prepare"]["fee_rate_sat_vb"], 2);
    assert_eq!(sweep["input_sats"], 20000);
    assert_eq!(sweep["fee_sats"], 354);
    let psbt = Psbt::from_str(sweep["psbt"].as_str().unwrap()).unwrap();
    assert_eq!(psbt.inputs.len(), 2);
    assert_eq!(psbt.unsigned_tx.output[0].value.to_sat(), 19646);
    assert_eq!(psbt.unsigned_tx.output[0].script_pubkey, Address::from_str(&address(2)).unwrap().require_network(Network::Bitcoin).unwrap().script_pubkey());
    for input in psbt.inputs {
        assert!(input.partial_sigs.is_empty());
        assert!(input.final_script_witness.is_none());
        assert_eq!(input.witness_utxo.unwrap().value.to_sat(), 10000);
    }
}

#[test]
fn duplicate_inputs_dust_and_fee_overflow_are_rejected() {
    let mut duplicate = candidate(1);
    duplicate.safe.push(duplicate.safe[0].clone());
    assert!(build_sweep(&duplicate, &address(2), 2, None).is_err());
    let mut dust = candidate(1);
    dust.safe[0]["value"] = json!(100);
    assert!(build_sweep(&dust, &address(2), 2, None).is_err());
    assert!(build_sweep(&candidate(1), &address(2), u64::MAX, None).is_err());
}
