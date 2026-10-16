//! Read-only, bounded incremental Electrum synchronization. Nothing here signs,
//! broadcasts, claims a withdrawal or decides whether money can be refunded.
use super::*;
use std::collections::{BTreeSet, HashSet};
use std::time::Instant;

const BATCH: usize = 16;
// Separate small transaction batches (potentially large responses) from bounded
// history subscriptions and fixed-size 80-byte headers. Keep a finite working
// set large enough for the production wallet without lifting the entry budget.
const HISTORY_BATCH: usize = 64;
const HEADER_BATCH: usize = 64;
const MAX_HISTORY_KEYS: usize = 4096;
const MAX_HISTORY_ENTRIES: usize = 50_000;
const MAX_TX_BYTES: usize = 8 * 1024 * 1024;
const MAX_TX_KEYS: usize = 1024;

#[derive(Clone, Debug, PartialEq)]
struct Entry { txid: Txid, height: i64 }
struct Snapshot { status: String, entries: Arc<Vec<Entry>>, used: Instant }
struct CachedTransaction { tx: Arc<Transaction>, bytes: usize, used: Instant }
#[derive(Default)]
struct CacheCounters {
    history_hits: u64,
    history_misses: u64,
    status_changes: u64,
    history_evictions: u64,
    history_oversized: u64,
    tx_hits: u64,
    tx_misses: u64,
    tx_evictions: u64,
    tx_oversized: u64,
}
#[derive(Default)]
struct Cache {
    histories: HashMap<(String, String), Snapshot>,
    history_entries: usize,
    transactions: HashMap<Txid, CachedTransaction>,
    tx_bytes: usize,
    counters: CacheCounters,
}
static CACHE: OnceLock<Mutex<Cache>> = OnceLock::new();

fn cache() -> std::sync::MutexGuard<'static, Cache> {
    CACHE.get_or_init(|| Mutex::new(Cache::default())).lock().unwrap_or_else(|p| p.into_inner())
}

impl Cache {
    fn history(&mut self, key: &(String, String), status: &str) -> Option<Arc<Vec<Entry>>> {
        if let Some(old) = self.histories.get_mut(key) {
            if old.status == status {
                old.used = Instant::now();
                self.counters.history_hits += 1;
                return Some(Arc::clone(&old.entries));
            }
            self.counters.status_changes += 1;
            self.remove_history(key);
        }
        self.counters.history_misses += 1;
        None
    }

    fn remove_history(&mut self, key: &(String, String)) {
        if let Some(old) = self.histories.remove(key) { self.history_entries -= old.entries.len(); }
    }

    fn remember_history(&mut self, key: (String, String), status: String, entries: Arc<Vec<Entry>>) {
        self.remove_history(&key);
        if entries.len() > MAX_HISTORY_ENTRIES {
            self.counters.history_oversized += 1;
            return;
        }
        while self.histories.len() >= MAX_HISTORY_KEYS || self.history_entries + entries.len() > MAX_HISTORY_ENTRIES {
            let oldest = self.histories.iter().min_by_key(|(_, value)| value.used)
                .map(|(key, _)| key.clone()).expect("nonempty bounded history cache");
            self.remove_history(&oldest);
            self.counters.history_evictions += 1;
        }
        self.history_entries += entries.len();
        self.histories.insert(key, Snapshot { status, entries, used: Instant::now() });
    }

    fn transaction(&mut self, id: &Txid) -> Option<Arc<Transaction>> {
        if let Some(old) = self.transactions.get_mut(id) {
            old.used = Instant::now();
            self.counters.tx_hits += 1;
            Some(Arc::clone(&old.tx))
        } else {
            self.counters.tx_misses += 1;
            None
        }
    }

    fn remember_transaction(&mut self, tx: Arc<Transaction>) {
        let id = tx.compute_txid();
        if let Some(old) = self.transactions.get_mut(&id) {
            old.used = Instant::now();
            return;
        }
        let size = encode::serialize(tx.as_ref()).len();
        if size > MAX_TX_BYTES {
            self.counters.tx_oversized += 1;
            return;
        }
        while self.transactions.len() >= MAX_TX_KEYS || self.tx_bytes + size > MAX_TX_BYTES {
            let oldest = *self.transactions.iter().min_by_key(|(_, value)| value.used)
                .map(|(key, _)| key).expect("nonempty bounded transaction cache");
            self.tx_bytes -= self.transactions.remove(&oldest).unwrap().bytes;
            self.counters.tx_evictions += 1;
        }
        self.tx_bytes += size;
        self.transactions.insert(id, CachedTransaction { tx, bytes: size, used: Instant::now() });
    }

    fn report(&self, phase: &str, completed: bool, started: Instant) {
        tracing::info!(stage = "cache_stats", phase, completed,
            elapsed_ms = started.elapsed().as_millis() as u64,
            history_keys = self.histories.len(), history_entries = self.history_entries,
            max_history_keys = MAX_HISTORY_KEYS, max_history_entries = MAX_HISTORY_ENTRIES,
            tx_keys = self.transactions.len(), tx_bytes = self.tx_bytes,
            history_hits_total = self.counters.history_hits,
            history_misses_total = self.counters.history_misses,
            status_changes_total = self.counters.status_changes,
            history_evictions_total = self.counters.history_evictions,
            history_oversized_total = self.counters.history_oversized,
            tx_hits_total = self.counters.tx_hits, tx_misses_total = self.counters.tx_misses,
            tx_evictions_total = self.counters.tx_evictions, tx_oversized_total = self.counters.tx_oversized,
            "Electrum wallet cache process totals");
    }
}

// Emit statistics on errors/deadlines too, not just on successful wallet updates.
struct CacheReport { started: Instant, completed: bool }
impl Drop for CacheReport {
    fn drop(&mut self) { cache().report("sync_end", self.completed, self.started); }
}

// A sequential scan larger than an LRU cache would otherwise evict its cached
// tail before reaching it on every round. Check retained entries first, but put
// results back in the original order so expected-txid eviction stays correct.
fn ordered_script_indices(endpoint: &str, hashes: &[String], cache: &Cache) -> Vec<usize> {
    let mut indices = (0..hashes.len()).collect::<Vec<_>>();
    indices.sort_by_key(|i| !cache.histories.contains_key(&(endpoint.to_owned(), hashes[*i].clone())));
    indices
}

fn stage(name: &str, started: Instant, items: usize) {
    tracing::info!(stage = name, elapsed_ms = started.elapsed().as_millis() as u64,
        items, remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
}

// No retries and no detached tasks. All requests share the enclosing deadline.
fn batch(config: &ElectrumConfig, calls: &[(String, Value)]) -> Result<Vec<Value>> {
    sync_budget::check()?;
    let started = Instant::now();
    let methods = calls.iter().map(|(method, _)| method.as_str()).collect::<BTreeSet<_>>()
        .into_iter().collect::<Vec<_>>().join(",");
    let wire = calls.iter().enumerate().map(|(i, (method, params))|
        (i as u64, method.clone(), params.clone())).collect::<Vec<_>>();
    tracing::info!(stage = "rpc_batch_start", %methods, calls = calls.len(),
        remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
    let result = (|| {
        let mut values = crate::electrum_transport::pipeline(&config.url, &wire,
            Duration::from_secs(5), sync_budget::rpc_deadline(Duration::from_secs(35)))?;
        (0..calls.len()).map(|i| values.remove(&(i as u64))
            .context("missing Electrum sync batch response")?
            .map_err(|error| anyhow!("Electrum {} failed: {error}", calls[i].0))).collect::<Result<Vec<_>>>()
    })();
    tracing::info!(stage = "rpc_batch_end", %methods, ok = result.is_ok(),
        calls = calls.len(), elapsed_ms = started.elapsed().as_millis() as u64,
        remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
    // Preserve RPC context, but make whole-round deadline exhaustion explicit.
    let values = result.or_else(|error| {
        sync_budget::check().with_context(|| format!("{error:#}"))?;
        Err(error)
    })?;
    sync_budget::check()?;
    Ok(values)
}

fn parse_history(value: &Value, expected_status: &str) -> Result<Arc<Vec<Entry>>> {
    let mut entries = Vec::new();
    let mut status_text = String::new();
    let mut seen = HashSet::new();
    for item in value.as_array().context("Electrum history is not an array")? {
        let text = item.get("tx_hash").and_then(Value::as_str).context("Electrum history missing txid")?;
        let txid = Txid::from_str(text).context("invalid Electrum history txid")?;
        let height = item.get("height").and_then(Value::as_i64).context("Electrum history missing height")?;
        anyhow::ensure!(height >= -1 && height <= u32::MAX as i64, "invalid Electrum history height");
        anyhow::ensure!(seen.insert(txid), "duplicate transaction in Electrum history");
        status_text.push_str(&format!("{text}:{height}:"));
        entries.push(Entry { txid, height });
    }
    anyhow::ensure!(sha256::Hash::hash(status_text.as_bytes()).to_string() == expected_status,
        "Electrum history changed during wallet sync: status digest mismatch");
    Ok(Arc::new(entries))
}

struct History { entries: Arc<Vec<Entry>>, complete: bool }

fn session_batch(session: &mut crate::electrum_transport::ReadSession, calls: &[(String, Value)])
    -> Result<Vec<std::result::Result<Value, String>>>
{
    sync_budget::check()?;
    let started = Instant::now();
    let methods = calls.iter().map(|(method, _)| method.as_str()).collect::<BTreeSet<_>>()
        .into_iter().collect::<Vec<_>>().join(",");
    tracing::info!(stage = "rpc_batch_start", %methods, calls = calls.len(),
        remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
    let wire = calls.iter().enumerate().map(|(i, (method, params))|
        (i as u64, method.clone(), params.clone())).collect::<Vec<_>>();
    let result = session.pipeline(&wire, sync_budget::rpc_deadline(Duration::from_secs(35)));
    tracing::info!(stage = "rpc_batch_end", %methods, ok = result.is_ok(),
        calls = calls.len(), elapsed_ms = started.elapsed().as_millis() as u64,
        remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
    let mut values = result.or_else(|error| {
        sync_budget::check().with_context(|| format!("{error:#}"))?;
        Err(error)
    })?;
    sync_budget::check()?;
    (0..calls.len()).map(|i| values.remove(&(i as u64)).context("missing Electrum session response")).collect()
}

fn parse_unspent_history(value: &Value) -> Result<Arc<Vec<Entry>>> {
    let mut entries = HashMap::new();
    for item in value.as_array().context("Electrum listunspent is not an array")? {
        let txid = Txid::from_str(item["tx_hash"].as_str().context("listunspent missing txid")?)?;
        let height = item["height"].as_i64().context("listunspent missing height")?;
        anyhow::ensure!(height >= -1 && height <= u32::MAX as i64, "invalid listunspent height");
        if let Some(old) = entries.insert(txid, height) {
            anyhow::ensure!(old == height, "inconsistent listunspent heights");
        }
    }
    Ok(Arc::new(entries.into_iter().map(|(txid, height)| Entry { txid, height }).collect()))
}

fn histories(config: &ElectrumConfig, scripts: &[ScriptBuf]) -> Result<Vec<History>> {
    let started = Instant::now();
    let mut results: Vec<Option<History>> = (0..scripts.len()).map(|_| None).collect();
    let mut reused = 0usize;
    let all_hashes = scripts.iter().map(electrum_script_hash_hex).collect::<Vec<_>>();
    let indices = ordered_script_indices(&config.url, &all_hashes, &cache());
    let total_batches = indices.len().div_ceil(HISTORY_BATCH);
    for (batch_index, indices) in indices.chunks(HISTORY_BATCH).enumerate() {
        let batch_started = Instant::now();
        let reused_before = reused;
        let hashes = indices.iter().map(|i| all_hashes[*i].clone()).collect::<Vec<_>>();
        let group = crate::electrum_transport::with_read_session(&config.url, Duration::from_secs(5),
            sync_budget::rpc_deadline(Duration::from_secs(35)), |session| {
            let subscribe = hashes.iter().map(|hash|
                ("blockchain.scripthash.subscribe".to_owned(), json!([hash]))).collect::<Vec<_>>();
            let statuses = session_batch(session, &subscribe)?;
            let mut group: Vec<Option<History>> = (0..indices.len()).map(|_| None).collect();
            let mut missing = Vec::new();
            let mut fallback = Vec::new();
            for (i, response) in statuses.into_iter().enumerate() {
                let value = match response {
                    Ok(value) => value,
                    Err(error) if is_electrum_history_limit_error(&anyhow!(error.clone())) => {
                        fallback.push(i); continue;
                    }
                    Err(error) => bail!("Electrum subscribe failed: {error}"),
                };
                if value.is_null() {
                    cache().remove_history(&(config.url.clone(), hashes[i].clone()));
                    group[i] = Some(History { entries: Arc::new(Vec::new()), complete: true });
                    continue;
                }
                let status = value.as_str().context("invalid Electrum subscription status")?;
                anyhow::ensure!(status.len() == 64 && status.bytes().all(|b| b.is_ascii_hexdigit()),
                    "invalid Electrum subscription digest");
                let key = (config.url.clone(), hashes[i].clone());
                let cached = cache().history(&key, status);
                if let Some(entries) = cached {
                    reused += 1;
                    group[i] = Some(History { entries, complete: true });
                } else { missing.push((i, key, status.to_owned())); }
            }
            // History and unspent responses can be large. Keep their original
            // small batch bound even when subscription status batches are wider.
            for missing in missing.chunks(BATCH) {
                let calls = missing.iter().map(|(_, (_, hash), _)|
                    ("blockchain.scripthash.get_history".to_owned(), json!([hash]))).collect::<Vec<_>>();
                for ((i, key, status), response) in missing.iter().zip(session_batch(session, &calls)?) {
                    match response {
                        Ok(value) => {
                            let entries = parse_history(&value, &status)?;
                            cache().remember_history(key.clone(), status.clone(), Arc::clone(&entries));
                            group[*i] = Some(History { entries, complete: true });
                        }
                        Err(error) if is_electrum_history_limit_error(&anyhow!(error.clone())) => fallback.push(*i),
                        Err(error) => bail!("Electrum history failed: {error}"),
                    }
                }
            }
            if !fallback.is_empty() {
                tracing::warn!(stage = "history_limit_listunspent", scripts = fallback.len(),
                    "Electrum history exceeds server limit; unspent-only result is not complete history");
                for fallback in fallback.chunks(BATCH) {
                let calls = fallback.iter().map(|i|
                    ("blockchain.scripthash.listunspent".to_owned(), json!([hashes[*i]]))).collect::<Vec<_>>();
                for (i, response) in fallback.iter().zip(session_batch(session, &calls)?) {
                    let value = response.map_err(|error| anyhow!("Electrum listunspent fallback failed: {error}"))?;
                    group[*i] = Some(History { entries: parse_unspent_history(&value)?, complete: false });
                    // Do not label an unspent-only snapshot as complete history.
                    cache().remove_history(&(config.url.clone(), hashes[*i].clone()));
                }
                }
            }
            let unsubscribe = hashes.iter().map(|hash|
                ("blockchain.scripthash.unsubscribe".to_owned(), json!([hash]))).collect::<Vec<_>>();
            for response in session_batch(session, &unsubscribe)? {
                anyhow::ensure!(response.map_err(|error| anyhow!("Electrum unsubscribe failed: {error}"))?.is_boolean(),
                    "invalid Electrum unsubscribe response");
            }
            group.into_iter().map(|entries| entries.context("missing script history")).collect::<Result<Vec<_>>>()
        })?;
        for (index, history) in indices.iter().zip(group) { results[*index] = Some(history); }
        tracing::info!(stage = "history_batch_finished", scripts = indices.len(),
            batch_index = batch_index + 1, total_batches,
            cache_hits = reused - reused_before,
            elapsed_ms = batch_started.elapsed().as_millis() as u64,
            remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
        cache().report("history_batch_end", false, started);
    }
    tracing::info!(stage = "history_finished", scripts = scripts.len(), reused,
        elapsed_ms = started.elapsed().as_millis() as u64, remaining_ms = ?sync_budget::remaining_ms(),
        "Electrum wallet incremental sync");
    results.into_iter().map(|history| history.context("missing ordered script history")).collect()
}

fn decode_transaction(value: &Value, expected: Txid) -> Result<Arc<Transaction>> {
    let raw = value.as_str().context("Electrum raw transaction is not a string")?;
    let tx: Transaction = encode::deserialize(&hex_to_bytes(raw)?)?;
    anyhow::ensure!(tx.compute_txid() == expected, "Electrum transaction txid mismatch");
    Ok(Arc::new(tx))
}

fn confirmation_headers(config: &ElectrumConfig, heights: &[u32]) -> Result<HashMap<u32, Header>> {
    let started = Instant::now();
    let heights = heights.iter().copied().collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
    let total_batches = heights.len().div_ceil(HEADER_BATCH);
    let mut headers = HashMap::new();
    tracing::info!(stage = "headers_start", items = heights.len(), total_batches,
        batch_size = HEADER_BATCH, remaining_ms = ?sync_budget::remaining_ms(),
        "Electrum wallet incremental sync");
    // Re-fetch every confirmation height, including unchanged histories: an
    // unchanged history digest does not prove that its block hash is unchanged.
    for (index, heights) in heights.chunks(HEADER_BATCH).enumerate() {
        let batch_started = Instant::now();
        let calls = heights.iter().map(|height|
            ("blockchain.block.header".to_owned(), json!([height]))).collect::<Vec<_>>();
        let result = batch(config, &calls).and_then(|values| {
            heights.iter().zip(values).map(|(height, value)| {
                let header = decode_header_hex(value.as_str().context("invalid Electrum block header")?)?;
                Ok((*height, header))
            }).collect::<Result<Vec<_>>>()
        });
        tracing::info!(stage = "headers_batch_finished", batch_index = index + 1, total_batches,
            calls = heights.len(), first_height = heights.first(), last_height = heights.last(),
            ok = result.is_ok(), elapsed_ms = batch_started.elapsed().as_millis() as u64,
            total_elapsed_ms = started.elapsed().as_millis() as u64,
            remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");
        headers.extend(result?);
    }
    stage("headers_finished", started, headers.len());
    Ok(headers)
}

pub(super) fn sync(local: &mut LocalWallet, config: &ElectrumConfig) -> Result<()> {
    let mut report = CacheReport { started: Instant::now(), completed: false };
    cache().report("sync_start", false, report.started);
    let _budget = sync_budget::WalletOperationBudget::enter(Duration::from_secs(120));
    sync_budget::check()?;
    let mut request = local.wallet.start_sync_with_revealed_spks().build();
    let mut scripts = Vec::new();
    let mut expected = Vec::new();
    while let Some(item) = request.next_spk_with_expected_txids() {
        scripts.push(item.spk.clone());
        expected.push(item.expected_txids);
    }
    let histories = histories(config, &scripts)?;
    let mut update = TxUpdate::<ConfirmationBlockTime>::default();
    let mut heights = HashMap::new();
    for (history, expected) in histories.iter().zip(expected) {
        let mut seen = HashSet::new();
        for entry in history.entries.iter() {
            seen.insert(entry.txid);
            if let Some(previous) = heights.insert(entry.txid, entry.height) {
                anyhow::ensure!(previous == entry.height, "Electrum history changed during wallet sync");
            }
        }
        for id in expected {
            if history.complete && !seen.contains(&id) { update.evicted_ats.insert((id, request.start_time())); }
        }
    }

    let started = Instant::now();
    stage("transactions_start", started, heights.len());
    let mut transactions = HashMap::new();
    let mut missing = Vec::new();
    for id in heights.keys() {
        if let Some(tx) = local.wallet.tx_graph().get_tx(*id).or_else(|| cache().transaction(id)) {
            transactions.insert(*id, tx);
        } else { missing.push(*id); }
    }
    // Cache validated immutable transactions even if this round subsequently
    // times out. No confirmations, balances or partial BDK updates are committed.
    for ids in missing.chunks(BATCH) {
        let calls = ids.iter().map(|id| ("blockchain.transaction.get".to_owned(), json!([id.to_string()]))).collect::<Vec<_>>();
        for (id, value) in ids.iter().zip(batch(config, &calls)?) {
            let tx = decode_transaction(&value, *id)?;
            cache().remember_transaction(Arc::clone(&tx));
            transactions.insert(*id, tx);
        }
    }
    tracing::info!(stage = "transactions_finished", downloaded = missing.len(),
        reused = transactions.len() - missing.len(), elapsed_ms = started.elapsed().as_millis() as u64,
        remaining_ms = ?sync_budget::remaining_ms(), "Electrum wallet incremental sync");

    let confirmed_heights = heights.values().copied().filter(|height| *height > 0)
        .map(|height| height as u32).collect::<BTreeSet<_>>().into_iter().collect::<Vec<_>>();
    let headers = confirmation_headers(config, &confirmed_heights)?;
    let mut checkpoints = Vec::new();
    // Confirmation heights/hashes are always revalidated, including for cached
    // raw transactions and unchanged histories. Never cache away a chain reorg.
    for (height, header) in &headers {
        checkpoints.push(BlockId { height: *height, hash: header.block_hash() });
    }
    for (id, height) in &heights {
        let tx = transactions.get(id).context("missing validated transaction")?;
        update.txs.push(Arc::clone(tx));
        if *height > 0 {
            let header = headers.get(&(*height as u32)).context("missing confirmation header")?;
            update.anchors.insert((ConfirmationBlockTime {
                block_id: BlockId { height: *height as u32, hash: header.block_hash() },
                confirmation_time: header.time as u64,
            }, *id));
        } else { update.seen_ats.insert((*id, request.start_time())); }
    }

    let started = Instant::now();
    stage("remaining_status_start", started, 0);
    while let Some(id) = request.next_txid() {
        sync_budget::check()?;
        if heights.contains_key(&id) { continue; }
        if let Some(status) = electrum_transaction_status(config, id)? {
            electrum_add_status_update(config, &mut update, &mut checkpoints, status)?;
        }
    }
    while let Some(outpoint) = request.next_outpoint() {
        sync_budget::check()?;
        if let Some(tx) = transactions.get(&outpoint.txid) {
            if let Some(txout) = tx.output.get(outpoint.vout as usize) { update.txouts.insert(outpoint, txout.clone()); }
        } else if let Some(status) = electrum_transaction_status(config, outpoint.txid)? {
            if let Some(txout) = status.tx.output.get(outpoint.vout as usize) { update.txouts.insert(outpoint, txout.clone()); }
            electrum_add_status_update(config, &mut update, &mut checkpoints, status)?;
        }
    }
    stage("remaining_status_finished", started, 0);
    checkpoints.push(electrum_chain_tip(config)?);
    let checkpoint = extend_latest_checkpoint(local.wallet.latest_checkpoint(), checkpoints)?;
    sync_budget::check()?;
    let started = Instant::now();
    local.wallet.apply_update(Update { tx_update: update, chain: Some(checkpoint), ..Default::default() })
        .context("failed to apply incremental Electrum update")?;
    stage("apply_finished", started, heights.len());
    report.completed = true;
    Ok(())
}

#[cfg(test)]
#[path = "wallet_electrum_sync_tests.rs"]
mod tests;
