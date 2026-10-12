//! Cooperative deadlines for synchronous wallet work. No detached wallet task
//! is left running when the caller receives an error.
use anyhow::{bail, Result};
use serde_json::Value;
use std::{cell::RefCell, collections::HashMap, marker::PhantomData, rc::Rc,
    time::{Duration, Instant}};

struct State {
    deadline: Instant,
    headers: HashMap<String, Value>,
}
thread_local! {
    static ACTIVE: RefCell<Option<State>> = const { RefCell::new(None) };
}

pub(crate) struct WalletOperationBudget {
    previous: Option<State>,
    // This scope must be dropped on the synchronous thread that entered it.
    _not_send: PhantomData<Rc<()>>,
}

impl WalletOperationBudget {
    pub(crate) fn enter(duration: Duration) -> Self {
        let deadline = rpc_deadline(duration);
        let previous = ACTIVE.with(|slot| slot.replace(Some(State {
            deadline, headers: HashMap::new(),
        })));
        Self { previous, _not_send: PhantomData }
    }
}
impl Drop for WalletOperationBudget {
    fn drop(&mut self) {
        ACTIVE.with(|slot| { slot.replace(self.previous.take()); });
    }
}

pub(crate) fn rpc_deadline(maximum: Duration) -> Instant {
    let maximum = Instant::now() + maximum;
    ACTIVE.with(|slot| slot.borrow().as_ref().map_or(maximum, |s| maximum.min(s.deadline)))
}

pub(crate) fn check() -> Result<()> {
    if ACTIVE.with(|slot| slot.borrow().as_ref().is_some_and(|s| Instant::now() >= s.deadline)) {
        bail!("WALLET_OPERATION_DEADLINE_EXCEEDED");
    }
    Ok(())
}

pub(crate) fn remaining_ms() -> Option<u64> {
    ACTIVE.with(|slot| slot.borrow().as_ref().map(|state|
        state.deadline.saturating_duration_since(Instant::now()).as_millis() as u64))
}

fn header_key(endpoint: &str, method: &str, params: &Value) -> Option<String> {
    matches!(method, "blockchain.headers.subscribe" | "blockchain.block.header")
        .then(|| format!("{endpoint}|{method}|{params}"))
}
pub(crate) fn cached_header(endpoint: &str, method: &str, params: &Value) -> Option<Value> {
    let key = header_key(endpoint, method, params)?;
    ACTIVE.with(|slot| slot.borrow().as_ref()?.headers.get(&key).cloned())
}
pub(crate) fn remember_header(endpoint: &str, method: &str, params: &Value, value: &Value) {
    let Some(key) = header_key(endpoint, method, params) else { return; };
    // Only small header responses, bounded per round, never raw transactions.
    if value.to_string().len() > 2048 { return; }
    ACTIVE.with(|slot| {
        if let Some(state) = slot.borrow_mut().as_mut() {
            if state.headers.len() < 128 { state.headers.insert(key, value.clone()); }
        }
    });
}
