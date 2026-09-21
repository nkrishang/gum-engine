//! Write-behind for everything that happens after a transaction is mined.
//!
//! A signer is freed the moment its receipt arrives; the database write that records the inclusion must
//! not hold it up. These writes are safe to defer because recovery can always re-derive them from the
//! persisted attempt and the chain — and safe to retry because each one is gated on the attempt's current
//! status (see `store::attempts`). One queue per chain, strictly FIFO, so an attempt's confirmation can
//! never be applied before its inclusion. The queue is bounded: if Postgres falls behind, workers block
//! here rather than letting memory grow without limit.

use std::{sync::Arc, time::Duration};

use tokio::sync::mpsc;

use crate::{
    domain::{addr_hex, hash_hex, Attempt, Inclusion},
    engine::Engine,
    error::StoreError,
    telemetry,
};

pub const SETTLE_QUEUE_DEPTH: usize = 10_000;

#[derive(Debug)]
pub enum SettleOp {
    Inclusion {
        attempt: Box<Attempt>,
        inclusion: Box<Inclusion>,
        confirm_now: bool,
        reincluded: bool,
    },
    Confirmation {
        attempt: Box<Attempt>,
    },
    /// Barrier used by shutdown and tests: resolves once everything queued before it is written.
    Flush(tokio::sync::oneshot::Sender<()>),
}

pub fn spawn(engine: Arc<Engine>, chain_id: u64) -> mpsc::Sender<SettleOp> {
    let (tx, rx) = mpsc::channel(SETTLE_QUEUE_DEPTH);
    tokio::spawn(run(engine, chain_id, rx));
    tx
}

async fn run(engine: Arc<Engine>, chain_id: u64, mut rx: mpsc::Receiver<SettleOp>) {
    while let Some(op) = rx.recv().await {
        metrics::gauge!("gum_settle_queue_depth", "chain" => chain_id.to_string()).set(rx.len() as f64);
        match op {
            SettleOp::Flush(done) => {
                let _ = done.send(());
            }
            SettleOp::Inclusion { attempt, inclusion, confirm_now, reincluded } => {
                apply(&engine, &attempt, "inclusion", || engine.store.settle_inclusion(&attempt, &inclusion, confirm_now, reincluded)).await;
            }
            SettleOp::Confirmation { attempt } => {
                apply(&engine, &attempt, "confirmation", || engine.store.settle_confirmation(&attempt)).await;
            }
        }
    }
}

/// Retries transient failures for as long as it takes (the data is only in memory until it is written);
/// anything else is a logic or data problem that retrying cannot fix, so it is parked and alerted.
async fn apply<F, Fut, T>(engine: &Engine, attempt: &Attempt, what: &'static str, mut write: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, StoreError>>,
{
    let mut delay = Duration::from_millis(100);
    loop {
        match write().await {
            Ok(_) => {
                engine.webhook_wake.notify_one();
                return;
            }
            Err(e) if e.is_transient() => {
                if let Some(suppressed) = telemetry::throttled(&format!("settle.retry:{}", attempt.chain_id), Duration::from_secs(10)) {
                    tracing::warn!(event = "settle.retry", chain = attempt.chain_id, what, code = e.code(), error = %e, suppressed, "post-inclusion write failed; retrying");
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
            Err(e) => {
                let payload = serde_json::json!({
                    "what": what,
                    "attempt_id": attempt.id,
                    "chain_id": attempt.chain_id,
                    "signer": addr_hex(&attempt.signer),
                    "nonce": attempt.nonce,
                    "tx_hash": hash_hex(&attempt.tx_hash),
                    "owner": attempt.owner.id(),
                });
                telemetry::alert(
                    &format!("settle.dead_letter:{}", attempt.id),
                    "settle.dead_letter",
                    "a post-inclusion write cannot be applied and was parked in dead_letters",
                    Some(attempt.chain_id),
                    Some(&addr_hex(&attempt.signer)),
                    serde_json::json!({"what": what, "code": e.code(), "error": e.to_string(), "tx_hash": hash_hex(&attempt.tx_hash)}),
                );
                if let Err(dl) = engine.store.dead_letter(&format!("settle.{what}"), payload, &e.to_string()).await {
                    tracing::error!(event = "settle.dead_letter_failed", code = dl.code(), error = %dl, "could not park the failed write");
                }
                return;
            }
        }
    }
}

/// Waits until every write queued so far for one chain has been applied. Anything that reads or rewrites
/// attempt state (recovery, re-org demotion) calls this first, so it never acts on a stale row.
pub async fn flush_chain(engine: &Engine, chain_id: u64) {
    let Some(tx) = engine.settler(chain_id) else { return };
    let (done, flushed) = tokio::sync::oneshot::channel();
    if tx.send(SettleOp::Flush(done)).await.is_ok() {
        let _ = flushed.await;
    }
}

/// Waits until every write queued so far on every chain has been applied (bounded by `timeout`).
pub async fn flush_all(engine: &Engine, timeout: Duration) {
    let senders: Vec<_> = engine.settlers.read().values().cloned().collect();
    let barriers = senders.into_iter().map(|tx| async move {
        let (done, flushed) = tokio::sync::oneshot::channel();
        if tx.send(SettleOp::Flush(done)).await.is_ok() {
            let _ = flushed.await;
        }
    });
    if tokio::time::timeout(timeout, futures::future::join_all(barriers)).await.is_err() {
        tracing::warn!(event = "settle.flush_timeout", "write-behind queues did not drain in time; recovery will re-derive the rest at next boot");
    }
}
