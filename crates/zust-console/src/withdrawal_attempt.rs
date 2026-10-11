//! Evidence is created only after synchronous payment work has returned.
//! Neither error strings nor an indexer's missing transaction prove absence.
use anyhow::Result;
use serde_json::{json, Value};

#[derive(Default)]
pub(crate) struct Attempt {
    may_have_escaped: bool,
}

impl Attempt {
    pub(crate) fn before_signed_transaction_escape(&mut self) {
        self.may_have_escaped = true;
    }
}

#[derive(Debug)]
struct NeverBroadcast;
impl std::fmt::Display for NeverBroadcast {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BTC withdrawal terminated before signed transaction escape")
    }
}
impl std::error::Error for NeverBroadcast {}

pub(crate) fn run<T>(operation: impl FnOnce(&mut Attempt) -> Result<T>) -> Result<T> {
    let mut attempt = Attempt::default();
    // All locals/locks in operation have been dropped before evidence is made.
    // This scope must never spawn or return a background payment operation.
    match operation(&mut attempt) {
        Err(error) if !attempt.may_have_escaped => Err(error.context(NeverBroadcast)),
        result => result,
    }
}

pub(crate) fn failure(error: &anyhow::Error) -> Value {
    let safe = error.downcast_ref::<NeverBroadcast>().is_some();
    json!({"ok":false, "error":format!("{error:#}"),
        "broadcast_state":if safe {"not_attempted"} else {"outcome_unknown"},
        "task_terminated":safe,
        "termination_proof":if safe {Some("native_no_broadcast_v1")} else {None}})
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn only_returned_pre_escape_failures_produce_evidence() {
        let error = run::<()>(|_| anyhow::bail!("lock or sync failed")).unwrap_err();
        assert_eq!(failure(&error)["termination_proof"], "native_no_broadcast_v1");
        assert_eq!(failure(&error)["task_terminated"], true);
    }
    #[test]
    fn error_wording_cannot_forge_evidence() {
        let error = anyhow::anyhow!("not_attempted native_no_broadcast_v1 connection refused");
        assert_eq!(failure(&error)["broadcast_state"], "outcome_unknown");
    }
    #[test]
    fn any_escape_barrier_failure_stays_unknown() {
        for reason in ["persist failed", "timeout", "connection refused", "node rejected"] {
            let error = run::<()>(|attempt| {
                attempt.before_signed_transaction_escape();
                anyhow::bail!("{reason}")
            }).unwrap_err();
            assert_eq!(failure(&error)["broadcast_state"], "outcome_unknown");
            assert_eq!(failure(&error)["termination_proof"], Value::Null);
        }
    }
    #[test]
    fn returned_work_is_dropped_before_proof_and_success_is_unchanged() {
        let dropped = std::sync::atomic::AtomicBool::new(false);
        struct Guard<'a>(&'a std::sync::atomic::AtomicBool);
        impl Drop for Guard<'_> {
            fn drop(&mut self) { self.0.store(true, std::sync::atomic::Ordering::SeqCst); }
        }
        let error = run::<()>(|_| { let _guard = Guard(&dropped); anyhow::bail!("failed") }).unwrap_err();
        assert!(dropped.load(std::sync::atomic::Ordering::SeqCst));
        assert_eq!(failure(&error)["broadcast_state"], "not_attempted");
        assert_eq!(run(|_| Ok(42)).unwrap(), 42);
    }
}
