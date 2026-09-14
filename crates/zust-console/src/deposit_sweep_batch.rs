//! Request-scoped chain snapshots. No snapshot is accepted from an HTTP caller,
//! persisted, or reused across requests. Broadcast validation remains separate.
use super::*;

const ADDRESS_BATCH: usize = 20;
const RGB_BATCH: usize = 64;
const MAX_INPUTS: usize = 5000;
const MAX_BODY: usize = 4 * 1024 * 1024;
const IO_BUDGET: Duration = Duration::from_secs(15);
const PLAN_BUDGET: Duration = Duration::from_secs(20);
const CALL_BUDGET: Duration = Duration::from_secs(5);
static RUNNING: AtomicBool = AtomicBool::new(false);

#[cfg(test)]
#[path = "deposit_sweep_batch_tests.rs"]
mod tests;

struct PlanGuard;
impl Drop for PlanGuard {
    fn drop(&mut self) {
        RUNNING.store(false, Ordering::Release);
    }
}

struct Candidate {
    ident: String,
    address: String,
    custody: Value,
    utxos: Vec<Value>,
    safe: Vec<Value>,
    protected: Vec<Value>,
    failed: Vec<Value>,
    error: Option<String>,
}

pub(super) extern "C" fn prepare_deposit_sweeps(
    rows: *const Dynamic,
    target: *const Dynamic,
    fee_rate: u64,
    min_sats: u64,
) -> *const Dynamic {
    let rows = unsafe { &*rows };
    let target = unsafe { &*target };
    consolidation_native_result("deposit_sweep_plan", || {
        ensure!(target.is_str(), "target must be a string");
        let rows = dynamic_to_json(rows);
        let rows = rows.as_array().context("deposit sweep rows must be an array")?.clone();
        ensure!(rows.len() <= 500, "deposit sweep address limit exceeded");
        let target = Address::from_str(target.as_str())?.require_network(Network::Bitcoin)?.to_string();
        ensure!(RUNNING.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok(),
            "BTC_DEPOSIT_SWEEP_PLAN_BUSY: another plan is running; retry after it finishes");
        let _guard = PlanGuard;
        // Root configuration is resolved on the VM thread, not a spawned thread.
        let sources = btc_esplora_urls();
        let rgb_url = daemon_route_url("/v1/assets/by-utxo")?;
        let data_dir = btc_wallet_data_dir();
        let xpub = btc_address_xpub_config()?;
        let start = Instant::now();
        let trace_id = BTC_CONSOLIDATION_TRACE_ID.fetch_add(1, Ordering::Relaxed);
        // Keep blocking HTTP client construction and I/O out of the async runtime.
        let job = thread::Builder::new().name("btc-deposit-sweep".into()).spawn(move || -> Result<Value> {
            let span = tracing::info_span!("btc_deposit_sweep", trace_id);
            let _entered = span.enter();
            tracing::info!(addresses = rows.len(), "deposit sweep plan started");
            let store = LocalNodeStore::open(&data_dir)?;
            let io_deadline = start + IO_BUDGET;
            let deadline = start + PLAN_BUDGET;
            let mut candidates = Vec::new();
            let mut addresses = BTreeSet::new();
            for row in rows {
                let ident = row.get("wallet_address").and_then(Value::as_str).unwrap_or("").to_string();
                let address = row.get("deposit_address").and_then(Value::as_str).unwrap_or("").to_string();
                let check = (|| -> Result<()> {
                    ensure!(Instant::now() < deadline, "BTC_DEPOSIT_SWEEP_DEADLINE_EXCEEDED");
                    ensure!(!ident.is_empty(), "missing wallet_address");
                    Address::from_str(&address)?.require_network(Network::Bitcoin)?;
                    let mapped = store.get_ident_btc_address(&ident)?.context("unknown BTC ident")?;
                    ensure!(mapped == address, "BTC_DEPOSIT_SWEEP_ADDRESS_MISMATCH");
                    ensure!(addresses.insert(address.clone()), "duplicate deposit address in plan");
                    Ok(())
                })();
                candidates.push(Candidate {
                    ident, address, custody: row.get("custody_available_sats").cloned().unwrap_or(json!("0")),
                    utxos: vec![], safe: vec![], protected: vec![], failed: vec![],
                    error: check.err().map(|e| format!("{e:#}")),
                });
            }
            let stage = Instant::now();
            load_utxos(&mut candidates, &sources, io_deadline);
            tracing::info!(stage = "utxos", elapsed_ms = stage.elapsed().as_millis() as u64,
                "deposit sweep stage completed");
            let stage = Instant::now();
            classify_rgb(&mut candidates, &rgb_url, io_deadline);
            tracing::info!(stage = "rgb_batch", elapsed_ms = stage.elapsed().as_millis() as u64,
                "deposit sweep stage completed");
            let mut sweeps = Vec::new();
            let mut skipped = Vec::new();
            for candidate in candidates {
                let stage = Instant::now();
                let attempt = (|| -> Result<Value> {
                    if let Some(error) = &candidate.error { bail!("{error}"); }
                    ensure!(Instant::now() < deadline, "BTC_DEPOSIT_SWEEP_DEADLINE_EXCEEDED");
                    ensure!(!candidate.safe.is_empty(), "{}",
                        if !candidate.protected.is_empty() { "RGB-bearing UTXOs are excluded from BTC sweep" }
                        else if !candidate.failed.is_empty() { "RGB UTXO check failed; excluded from BTC sweep" }
                        else { "no confirmed sweepable UTXO" });
                    let sum = candidate.safe.iter().try_fold(0u64, |sum, u| {
                        sum.checked_add(u["value"].as_u64().context("missing UTXO value")?)
                            .context("BTC amount overflow")
                    })?;
                    ensure!(sum >= min_sats, "confirmed sweepable amount is below min_input_sats");
                    let key = match &xpub {
                        Some(config) => btc_xpub_input_key_source(&store, config, &candidate.address)?,
                        None => None,
                    };
                    ensure!(Instant::now() < deadline, "BTC_DEPOSIT_SWEEP_DEADLINE_EXCEEDED");
                    build_sweep(&candidate, &target, fee_rate, key)
                })();
                match attempt {
                    Ok(sweep) => sweeps.push(sweep),
                    Err(error) => {
                        let error = format!("{error:#}");
                        tracing::warn!(address = %candidate.address, stage = "prepare_psbt", %error,
                            "deposit sweep address skipped");
                        let selected_sats: u64 = candidate.safe.iter().filter_map(|u| u["value"].as_u64()).sum();
                        skipped.push(json!({"wallet_address": candidate.ident,
                            "deposit_address": candidate.address, "selected_sats": selected_sats,
                            "input_count": candidate.safe.len(), "rgb_protected_outpoints": candidate.protected,
                            "rgb_check_failed": candidate.failed, "reason": error}));
                    }
                }
                tracing::info!(stage = "prepare_psbt", elapsed_ms = stage.elapsed().as_millis() as u64,
                    "deposit sweep address completed");
            }
            tracing::info!(sweeps = sweeps.len(), skipped = skipped.len(),
                elapsed_ms = start.elapsed().as_millis() as u64, "deposit sweep plan completed");
            Ok(json!({"sweeps": sweeps, "skipped": skipped}))
        }).context("start deposit sweep worker")?;
        let result = job.join().map_err(|_| anyhow::anyhow!("deposit sweep worker panicked"))??;
        Ok(ok(result))
    })
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let left = deadline.saturating_duration_since(Instant::now());
    ensure!(!left.is_zero(), "BTC_DEPOSIT_SWEEP_DEADLINE_EXCEEDED");
    Ok(left)
}

// Check the deadline on every buffer refill, including a peer trickling a line.
fn read_rpc_line(reader: &mut BufReader<TcpStream>, deadline: Instant) -> Result<Value> {
    let mut line = Vec::new();
    loop {
        reader.get_ref().set_read_timeout(Some(remaining(deadline)?))?;
        let buf = reader.fill_buf()?;
        ensure!(!buf.is_empty(), "Electrum closed the batch connection");
        let newline = buf.iter().position(|byte| *byte == b'\n');
        let count = newline.map(|i| i + 1).unwrap_or(buf.len());
        ensure!(line.len() + count <= MAX_BODY, "Electrum response size limit exceeded");
        line.extend_from_slice(&buf[..count]);
        reader.consume(count);
        if newline.is_some() { return Ok(serde_json::from_slice(&line)?); }
    }
}

fn electrum_batch(source: &str, addresses: &[String], deadline: Instant) -> Vec<Result<Value>> {
    let mut results: BTreeMap<usize, Result<Value>> = BTreeMap::new();
    let run = (|| -> Result<()> {
        let endpoint = electrum_endpoint(source)?;
        let remote = endpoint.to_socket_addrs()?.next().context("Electrum DNS returned no addresses")?;
        let mut stream = TcpStream::connect_timeout(&remote, remaining(deadline)?)?;
        for (id, address) in addresses.iter().enumerate() {
            let address = Address::from_str(address)?.require_network(Network::Bitcoin)?;
            let call = json!({"id": id, "method": "blockchain.scripthash.listunspent",
                "params": [electrum_script_hash_hex(&address.script_pubkey())]});
            stream.set_write_timeout(Some(remaining(deadline)?))?;
            writeln!(stream, "{call}")?;
        }
        stream.set_write_timeout(Some(remaining(deadline)?))?;
        stream.flush()?;
        let mut reader = BufReader::new(stream);
        while results.len() < addresses.len() {
            let response = read_rpc_line(&mut reader, deadline)?;
            let id = response.get("id").and_then(Value::as_u64).context("Electrum response missing id")?;
            let id = usize::try_from(id)?;
            ensure!(id < addresses.len() && !results.contains_key(&id), "unexpected or duplicate Electrum response id");
            let result = if let Some(error) = response.get("error").filter(|v| !v.is_null()) {
                Err(anyhow::anyhow!("Electrum listunspent failed: {error}"))
            } else {
                response.get("result").cloned().context("Electrum result missing")
                    .and_then(|value| normalize_utxos(value, true))
            };
            results.insert(id, result);
        }
        Ok(())
    })();
    let missing = run.err().map(|e| format!("{e:#}")).unwrap_or_else(|| "Electrum response missing".into());
    (0..addresses.len()).map(|id| results.remove(&id)
        .unwrap_or_else(|| Err(anyhow::anyhow!("{missing}")))).collect()
}

fn normalize_utxos(value: Value, electrum: bool) -> Result<Value> {
    let items = value.as_array().context("UTXO response is not an array")?;
    ensure!(items.len() <= MAX_INPUTS, "UTXO response exceeds plan input limit");
    let mut seen = BTreeSet::new();
    let mut output = Vec::new();
    for item in items {
        let confirmed = if electrum {
            item.get("height").and_then(Value::as_i64).context("UTXO height missing")? > 0
        } else {
            item.pointer("/status/confirmed").and_then(Value::as_bool).context("UTXO confirmation status missing")?
        };
        if !confirmed { continue; }
        let txid = item.get(if electrum { "tx_hash" } else { "txid" }).and_then(Value::as_str).context("UTXO txid missing")?;
        let txid = bitcoin::Txid::from_str(txid)?;
        let vout = item.get(if electrum { "tx_pos" } else { "vout" }).and_then(Value::as_u64).context("UTXO vout missing")?;
        let outpoint = OutPoint { txid, vout: u32::try_from(vout)? };
        ensure!(seen.insert(outpoint), "duplicate UTXO in node response");
        let value = item.get("value").and_then(Value::as_u64).context("UTXO value missing")?;
        ensure!(value <= 2_100_000_000_000_000, "UTXO amount exceeds Bitcoin supply");
        if value == 0 { continue; }
        output.push(json!({"txid": txid.to_string(), "vout": outpoint.vout,
            "outpoint": outpoint.to_string(), "value": value, "status": {"confirmed": true}}));
    }
    Ok(Value::Array(output))
}

fn read_http_json(response: reqwest::blocking::Response) -> Result<Value> {
    let response = response.error_for_status()?;
    let mut bytes = Vec::new();
    response.take((MAX_BODY + 1) as u64).read_to_end(&mut bytes)?;
    ensure!(bytes.len() <= MAX_BODY, "HTTP response size limit exceeded");
    Ok(serde_json::from_slice(&bytes)?)
}

fn load_utxos(candidates: &mut [Candidate], sources: &[String], deadline: Instant) {
    let indexes: Vec<usize> = candidates.iter().enumerate().filter(|(_, c)| c.error.is_none()).map(|(i, _)| i).collect();
    let mut total = 0;
    for chunk in indexes.chunks(ADDRESS_BATCH) {
        let mut pending = chunk.to_vec();
        let mut errors = BTreeMap::new();
        for source in sources {
            if pending.is_empty() || Instant::now() >= deadline { break; }
            let addresses: Vec<String> = pending.iter().map(|i| candidates[*i].address.clone()).collect();
            let batch_deadline = deadline.min(Instant::now() + CALL_BUDGET);
            let results = if is_electrum_chain_source(source) {
                electrum_batch(source, &addresses, batch_deadline)
            } else {
                addresses.iter().map(|address| -> Result<Value> {
                    let response = rgb_service_http_client()?.get(format!("{}/address/{address}/utxo", source.trim_end_matches('/')))
                        .timeout(remaining(batch_deadline)?).send()?;
                    normalize_utxos(read_http_json(response)?, false)
                }).collect()
            };
            let mut retry = Vec::new();
            for (index, result) in pending.into_iter().zip(results) {
                match result {
                    Ok(value) => {
                        let utxos = value.as_array().cloned().unwrap_or_default();
                        if total + utxos.len() > MAX_INPUTS {
                            candidates[index].error = Some("BTC_DEPOSIT_SWEEP_INPUT_LIMIT_EXCEEDED: reduce limit".into());
                        } else {
                            total += utxos.len();
                            candidates[index].utxos = utxos;
                        }
                    }
                    Err(e) => { errors.insert(index, format!("{e:#}")); retry.push(index); }
                }
            }
            pending = retry;
        }
        for index in pending {
            candidates[index].error = Some(errors.remove(&index).unwrap_or_else(||
                "BTC_DEPOSIT_SWEEP_DEADLINE_EXCEEDED_OR_NO_CHAIN_SOURCE".into()));
        }
    }
}

fn classify_rgb(candidates: &mut [Candidate], url: &str, deadline: Instant) {
    let mut refs = Vec::new();
    for (index, candidate) in candidates.iter().enumerate().filter(|(_, c)| c.error.is_none()) {
        for utxo in &candidate.utxos {
            refs.push((index, utxo.clone(), json!({"account_id": candidate.address,
                "address": candidate.address, "outpoint": utxo["outpoint"], "confirmed": true})));
        }
    }
    for chunk in refs.chunks(RGB_BATCH) {
        let stage = Instant::now();
        let batch = (|| -> Result<Vec<Value>> {
            let requests: Vec<Value> = chunk.iter().map(|(_, _, request)| request.clone()).collect();
            let response = rgb_service_http_client()?.post(url).header("content-type", "application/json")
                .timeout(remaining(deadline)?.min(CALL_BUDGET)).body(serde_json::to_vec(&requests)?).send()?;
            let response = read_http_json(response)?;
            let results = response.as_array().context("RGB batch response is not an array")?;
            ensure!(results.len() == chunk.len(), "RGB batch response length mismatch");
            Ok(results.clone())
        })();
        tracing::info!(stage = "rgb_batch_request", inputs = chunk.len(),
            elapsed_ms = stage.elapsed().as_millis() as u64, success = batch.is_ok(),
            "deposit sweep RGB batch completed");
        for (position, (index, utxo, request)) in chunk.iter().enumerate() {
            let check = (|| -> Result<Vec<Value>> {
                let responses = batch.as_ref().map_err(|e| anyhow::anyhow!("{e:#}"))?;
                let response = &responses[position];
                ensure!(response.get("outpoint") == request.get("outpoint"), "RGB batch outpoint mismatch");
                for field in ["address", "account_id"] {
                    if let Some(value) = response.get(field).filter(|v| !v.is_null()) {
                        ensure!(Some(value) == request.get(field), "RGB batch address/account mismatch");
                    }
                }
                ensure!(response.get("ok").and_then(Value::as_bool) != Some(false), "RGB check returned failure");
                ensure!(!response.get("error").is_some_and(|v| !v.is_null() && v.as_str() != Some("")),
                    "RGB check error: {}", response.get("error").unwrap_or(&Value::Null));
                Ok(response.get("allocations").and_then(Value::as_array)
                    .context("RGB allocations missing or malformed")?.clone())
            })();
            let candidate = &mut candidates[*index];
            match check {
                Ok(allocations) if allocations.is_empty() => candidate.safe.push(utxo.clone()),
                Ok(allocations) => candidate.protected.push(json!({"outpoint": utxo["outpoint"],
                    "address": candidate.address, "allocation_count": allocations.len(), "allocations": allocations})),
                Err(error) => candidate.failed.push(json!({"outpoint": utxo["outpoint"],
                    "address": candidate.address, "error": format!("{error:#}")})),
            }
        }
    }
}

fn build_sweep(
    candidate: &Candidate,
    target: &str,
    fee_rate: u64,
    key: Option<(PublicKey, Fingerprint, DerivationPath)>,
) -> Result<Value> {
    let sender = Address::from_str(&candidate.address)?.require_network(Network::Bitcoin)?;
    let recipient = Address::from_str(target)?.require_network(Network::Bitcoin)?;
    let fee_rate = if fee_rate == 0 { 2 } else { fee_rate };
    let mut seen = BTreeSet::new();
    let mut inputs = Vec::new();
    let mut input_sats = 0u64;
    for utxo in &candidate.safe {
        let outpoint = OutPoint::from_str(utxo["outpoint"].as_str().context("missing outpoint")?)?;
        ensure!(seen.insert(outpoint), "duplicate selected outpoint");
        let value = utxo["value"].as_u64().context("missing input value")?;
        input_sats = input_sats.checked_add(value).context("input amount overflow")?;
        ensure!(input_sats <= 2_100_000_000_000_000, "input amount exceeds Bitcoin supply");
        inputs.push((outpoint, value));
    }
    ensure!(!inputs.is_empty(), "no safe inputs");
    let estimated_vbytes = 10 + (inputs.len() as u64) * 68 + 31;
    let fee_sats = fee_rate.checked_mul(estimated_vbytes).context("fee overflow")?;
    let amount_sats = input_sats.checked_sub(fee_sats).context("selected funds do not cover sweep fee")?;
    ensure!(amount_sats >= 546, "sweep output would be dust");
    let tx = Transaction {
        version: Version::TWO, lock_time: LockTime::ZERO,
        input: inputs.iter().map(|(outpoint, _)| TxIn {
            previous_output: *outpoint, script_sig: ScriptBuf::new(),
            sequence: Sequence::ENABLE_RBF_NO_LOCKTIME, witness: Witness::new(),
        }).collect(),
        output: vec![TxOut { value: Amount::from_sat(amount_sats), script_pubkey: recipient.script_pubkey() }],
    };
    let mut psbt = Psbt::from_unsigned_tx(tx)?;
    for (i, (_, value)) in inputs.iter().enumerate() {
        psbt.inputs[i].witness_utxo = Some(TxOut { value: Amount::from_sat(*value), script_pubkey: sender.script_pubkey() });
        if let Some((public_key, fingerprint, path)) = &key {
            psbt.inputs[i].bip32_derivation.insert(*public_key, (*fingerprint, path.clone()));
        }
    }
    let outpoints = inputs.iter().map(|(point, _)| point.to_string()).collect::<Vec<_>>().join(",");
    let psbt = psbt.to_string();
    let prepare = json!({"ok": true, "module": "btc", "operation": "prepare_sweep_with_inputs",
        "ident": candidate.ident, "from": candidate.address, "to": target,
        "amount_sats": amount_sats, "input_sats": input_sats, "fee_sats": fee_sats,
        "fee_rate_sat_vb": fee_rate, "estimated_vbytes": estimated_vbytes,
        "inputs": inputs.iter().map(|(p, v)| json!({"outpoint": p.to_string(), "value": v})).collect::<Vec<_>>(),
        "unsigned_psbt": psbt, "psbt": psbt, "signer": "xpub_owner_wallet",
        "bip32_derivation_count": if key.is_some() { inputs.len() } else { 0 }});
    Ok(json!({"wallet_address": candidate.ident, "ident": candidate.ident,
        "deposit_address": candidate.address, "target_address": target,
        "custody_available_sats": candidate.custody, "input_sats": input_sats,
        "amount_sats": amount_sats, "fee_sats": fee_sats, "fee_rate_sat_vb": fee_rate.to_string(),
        "input_count": inputs.len(), "outpoints": outpoints,
        "rgb_protected_outpoints": candidate.protected, "rgb_check_failed": candidate.failed,
        "unsigned_psbt": psbt, "psbt": psbt, "prepare": prepare,
        "signing_request": {"kind": "btc_deposit_sweep_psbt", "wallet_address": candidate.ident,
            "deposit_address": candidate.address, "target_address": target,
            "input_sats": input_sats, "amount_sats": amount_sats, "fee_sats": fee_sats, "outpoints": outpoints}}))
}
