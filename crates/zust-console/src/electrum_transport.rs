//! Shared, bounded Electrum transport for wallet RPCs and address pipelines.
//! A transport failure discards the connection. Requests are never replayed here,
//! especially broadcasts whose outcome may be unknown after a write or timeout.
use anyhow::{anyhow, bail, ensure, Context, Result};
use serde_json::{json, Value};
use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::time::{Duration, Instant};

const MAX_ENDPOINTS: usize = 32;
const MAX_CONNECTIONS: usize = 8;
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;
const MAX_IDLE: Duration = Duration::from_secs(300);
static POOLS: OnceLock<Mutex<HashMap<String, Arc<Pool>>>> = OnceLock::new();
static NEXT_ID: AtomicU64 = AtomicU64::new(1);

type Responses = HashMap<u64, std::result::Result<Value, String>>;

#[derive(Default)]
struct State {
    total: usize,
    idle: Vec<(Instant, BufReader<TcpStream>)>,
}
#[derive(Default)]
struct Pool {
    state: Mutex<State>,
    available: Condvar,
}
struct Lease {
    pool: Arc<Pool>,
    connection: Option<BufReader<TcpStream>>,
    reusable: bool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut state = self.pool.state.lock().unwrap_or_else(|p| p.into_inner());
        if self.reusable {
            if let Some(connection) = self.connection.take() {
                state.idle.push((Instant::now(), connection));
            } else {
                state.total -= 1;
            }
        } else {
            self.connection.take();
            state.total -= 1;
        }
        self.pool.available.notify_one();
    }
}

fn remaining(deadline: Instant) -> Result<Duration> {
    let duration = deadline.saturating_duration_since(Instant::now());
    ensure!(!duration.is_zero(), "Electrum request deadline exceeded");
    Ok(duration)
}

fn endpoint(source: &str) -> Result<String> {
    let source = source.trim().trim_end_matches('/');
    let source = source.strip_prefix("electrum://")
        .or_else(|| source.strip_prefix("tcp://")).unwrap_or(source);
    ensure!(!source.contains("://"), "Electrum transport requires plaintext tcp/electrum");
    let authority = source.split('/').next().unwrap_or_default();
    ensure!(authority.contains(':'), "Electrum source must include host:port");
    Ok(authority.to_ascii_lowercase())
}

fn pool_for(endpoint: &str) -> Result<Arc<Pool>> {
    let mut pools = POOLS.get_or_init(|| Mutex::new(HashMap::new()))
        .lock().map_err(|_| anyhow!("Electrum pool registry poisoned"))?;
    if let Some(pool) = pools.get(endpoint) {
        return Ok(Arc::clone(pool));
    }
    if pools.len() >= MAX_ENDPOINTS {
        // Registry entries with no borrowers can be retired without splitting
        // the per-endpoint connection cap across two live pools.
        pools.retain(|_, pool| Arc::strong_count(pool) > 1);
    }
    ensure!(pools.len() < MAX_ENDPOINTS, "Electrum endpoint pool limit reached");
    let pool = Arc::new(Pool::default());
    pools.insert(endpoint.to_string(), Arc::clone(&pool));
    Ok(pool)
}

fn checkout(endpoint: &str, connect_timeout: Duration, deadline: Instant) -> Result<Lease> {
    let pool = pool_for(endpoint)?;
    let mut state = pool.state.lock().map_err(|_| anyhow!("Electrum pool poisoned"))?;
    loop {
        remaining(deadline)?;
        while let Some((idle_since, connection)) = state.idle.pop() {
            if idle_since.elapsed() <= MAX_IDLE {
                drop(state);
                return Ok(Lease { pool, connection: Some(connection), reusable: false });
            }
            state.total -= 1;
        }
        if state.total < MAX_CONNECTIONS {
            state.total += 1;
            drop(state);
            let mut lease = Lease { pool, connection: None, reusable: false };
            // No socket or registry lock is held during DNS resolution or I/O.
            let address = endpoint.to_socket_addrs()
                .with_context(|| format!("resolve Electrum endpoint {endpoint}"))?
                .next().context("Electrum endpoint resolved no addresses")?;
            let stream = TcpStream::connect_timeout(&address, remaining(deadline)?.min(connect_timeout))
                .with_context(|| format!("connect Electrum endpoint {endpoint}"))?;
            stream.set_nodelay(true).context("set Electrum TCP_NODELAY")?;
            lease.connection = Some(BufReader::new(stream));
            return Ok(lease);
        }
        let waited = pool.available.wait_timeout(state, remaining(deadline)?)
            .map_err(|_| anyhow!("Electrum pool poisoned while waiting"))?;
        state = waited.0;
    }
}

fn read_response(reader: &mut BufReader<TcpStream>, deadline: Instant) -> Result<Value> {
    let mut line = Vec::new();
    loop {
        reader.get_ref().set_read_timeout(Some(remaining(deadline)?))?;
        let buffer = reader.fill_buf().context("read Electrum response")?;
        ensure!(!buffer.is_empty(), "Electrum connection closed before response");
        let newline = buffer.iter().position(|byte| *byte == b'\n');
        let count = newline.map(|i| i + 1).unwrap_or(buffer.len());
        ensure!(line.len() + count <= MAX_RESPONSE_BYTES, "Electrum response size limit exceeded");
        line.extend_from_slice(&buffer[..count]);
        reader.consume(count);
        if newline.is_some() {
            return serde_json::from_slice(&line).context("decode Electrum response");
        }
    }
}

pub(crate) fn rpc(
    source: &str, method: &str, params: Value, connect_timeout: Duration, deadline: Instant,
) -> Result<Value> {
    let calls = [(0, method.to_string(), params)];
    let mut responses = pipeline(source, &calls, connect_timeout, deadline)?;
    responses.remove(&0).context("Electrum response missing")?
        .map_err(|error| anyhow!("Electrum {method} failed: {error}"))
}

pub(crate) fn pipeline(
    source: &str, calls: &[(u64, String, Value)], connect_timeout: Duration, deadline: Instant,
) -> Result<Responses> {
    pipeline_inner(source, calls, connect_timeout, deadline, false)
}

// Deposit sweep planning historically preserves successfully checked addresses
// when another address times out. The failed socket must still be discarded.
pub(crate) fn pipeline_partial(
    source: &str, calls: &[(u64, String, Value)], connect_timeout: Duration, deadline: Instant,
) -> Result<Responses> {
    pipeline_inner(source, calls, connect_timeout, deadline, true)
}

fn pipeline_inner(
    source: &str, calls: &[(u64, String, Value)], connect_timeout: Duration, deadline: Instant,
    allow_partial: bool,
) -> Result<Responses> {
    if calls.is_empty() { return Ok(HashMap::new()); }
    let endpoint = endpoint(source)?;
    let methods = calls.iter().map(|(_, method, _)| method.as_str())
        .collect::<HashSet<_>>().into_iter().collect::<Vec<_>>().join(",");
    let run = || -> Result<Responses> {
        let mut caller_ids = HashSet::new();
        for (id, _, _) in calls {
            ensure!(caller_ids.insert(*id), "duplicate Electrum pipeline caller id");
        }
        let mut lease = checkout(&endpoint, connect_timeout, deadline)?;
        let reader = lease.connection.as_mut().context("Electrum connection missing")?;
        let mut pending = HashMap::new();
        for (caller_id, method, params) in calls {
            let wire_id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
            pending.insert(wire_id, *caller_id);
            let request = json!({"id": wire_id, "method": method,
                "params": params.as_array().cloned().unwrap_or_default()});
            let mut bytes = serde_json::to_vec(&request)?;
            bytes.push(b'\n');
            // Recalculate the deadline after each partial write.
            let mut sent = 0;
            while sent < bytes.len() {
                reader.get_ref().set_write_timeout(Some(remaining(deadline)?))?;
                let count = reader.get_mut().write(&bytes[sent..]).context("write Electrum request")?;
                ensure!(count > 0, "Electrum connection closed during request write");
                sent += count;
            }
        }
        let mut responses = HashMap::new();
        let mut notifications = 0usize;
        while !pending.is_empty() {
            let response = match read_response(reader, deadline) {
                Ok(response) => response,
                Err(error) if allow_partial => {
                    let message = format!("Electrum RPC endpoint={endpoint} methods={methods}: {error:#}");
                    for (_, caller_id) in pending.drain() {
                        responses.insert(caller_id, Err(message.clone()));
                    }
                    // Early return leaves reusable=false: unread/late responses
                    // cannot contaminate any subsequent request.
                    return Ok(responses);
                }
                Err(error) => return Err(error),
            };

            if response.get("id").map_or(true, Value::is_null)
                && response.get("method").and_then(Value::as_str).is_some()
            {
                // Header subscriptions may notify on the same reusable socket.
                notifications += 1;
                ensure!(notifications <= 1024, "too many Electrum notifications");
                continue;
            }
            let wire_id = response.get("id").and_then(Value::as_u64)
                .context("Electrum response missing numeric id")?;
            let caller_id = pending.remove(&wire_id)
                .context("unexpected or duplicate Electrum response id")?;
            let result = if let Some(error) = response.get("error").filter(|v| !v.is_null()) {
                Err(error.to_string())
            } else if let Some(result) = response.get("result") {
                Ok(result.clone())
            } else {
                bail!("Electrum response missing result");
            };
            responses.insert(caller_id, result);
        }
        // Only fully drained, correctly correlated responses permit reuse.
        lease.reusable = true;
        Ok(responses)
    };
    run().with_context(|| format!("Electrum RPC endpoint={endpoint} methods={methods}"))
}

#[cfg(test)]
#[path = "electrum_transport_tests.rs"]
mod tests;
