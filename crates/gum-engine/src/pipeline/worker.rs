//! The pair worker: one task per (signer, chain).
//!
//! Whenever its pair is healthy and the chain's queue is non-empty, the worker takes the next job and
//! drives it to a receipt. Workers share nothing but the queue and the RPC limiter, so a slow KMS call, a
//! stuck transaction or a paused pair never holds anyone else up — and the same signer keeps working on
//! every other chain, where it is a different pair with a different worker.

use std::{
    sync::atomic::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
use chrono::Utc;

use crate::{
    domain::{addr_hex, AttemptPurpose, JobStatus, PauseReason, QueuedJob, RecoveryStep, SlotOwner},
    engine::{Engine, Pair},
    error::{JobFailure, RpcError},
    funds::treasury,
    pipeline::{
        accounting::{self, JobTiming},
        recovery,
        tx::{NotBound, Outcome, TxDriver, TxIntent},
    },
    stats::Bucket,
    telemetry,
};

pub async fn run(engine: Arc<Engine>, pair: Arc<Pair>) {
    let chain = pair.chain.clone();
    loop {
        if engine.shutdown.is_cancelled() {
            return;
        }
        if pair.take_recovery_request() {
            recovery::recover_pair(&engine, &pair).await;
            continue;
        }
        let blocked = pair.is_paused() || pair.view().draining || !chain.is_accepting_work() || engine.global_pause.load(Ordering::Relaxed);
        if blocked {
            tokio::select! {
                _ = pair.wake.notified() => {}
                _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                _ = engine.shutdown.cancelled() => return,
            }
            continue;
        }

        let job = tokio::select! {
            job = chain.queue.pop() => job,
            _ = pair.wake.notified() => continue,
            _ = engine.shutdown.cancelled() => return,
        };
        // The pair may have been paused while it waited; the job goes back untouched.
        if pair.is_paused() || pair.recovery_requested() || engine.shutdown.is_cancelled() {
            chain.queue.push_front(job);
            continue;
        }

        let signer = Some(pair.key.signer);
        engine.stats.transition(chain.chain_id, (None, Bucket::Queued), (signer, Bucket::Processing));
        pair.set_current(Some(job.id));
        let disposition = process(&engine, &pair, &job).await;
        pair.set_current(None);

        match disposition {
            Disposition::Mined => {}
            Disposition::Failed => {
                engine.stats.transition(chain.chain_id, (signer, Bucket::Processing), (signer, Bucket::Failed));
                chain.queue.forget(&job.id);
            }
            Disposition::FailedAfterBind => {
                engine.stats.transition(chain.chain_id, (signer, Bucket::InFlight), (signer, Bucket::Failed));
                chain.queue.forget(&job.id);
            }
            Disposition::Requeue { after_bind, backoff } => {
                let from = if after_bind { Bucket::InFlight } else { Bucket::Processing };
                engine.stats.transition(chain.chain_id, (signer, from), (None, Bucket::Queued));
                chain.queue.push_front(job);
                if !backoff.is_zero() {
                    tokio::time::sleep(backoff).await;
                }
            }
            Disposition::Dropped => {
                // Someone else already owns this job (it was offered twice); it is counted elsewhere.
                engine.stats.transition(chain.chain_id, (signer, Bucket::Processing), (None, Bucket::Queued));
                chain.queue.forget(&job.id);
            }
            // The job stays bound and in flight; recovery or an operator takes it from here.
            Disposition::LeftInFlight => {}
        }
    }
}

enum Disposition {
    Mined,
    /// Failed before any nonce was bound.
    Failed,
    /// Failed after binding, by proof that it never executed.
    FailedAfterBind,
    Requeue {
        after_bind: bool,
        backoff: Duration,
    },
    Dropped,
    LeftInFlight,
}

async fn process(engine: &Arc<Engine>, pair: &Arc<Pair>, job: &QueuedJob) -> Disposition {
    let chain = &pair.chain;
    let started = Instant::now();
    let queue_time = started.duration_since(job.enqueued_at);
    let req = &job.request;

    if req.deadline.is_some_and(|d| d <= Utc::now()) {
        return fail_unbound(engine, pair, job, JobFailure::Expired, "deadline passed before the job could be sent", None).await;
    }

    // Gas: the caller's limit is used as given — simulating is the caller's business. Only when none was
    // supplied do we estimate, and a reverting estimate fails the job before it costs anything.
    let gas_limit = match req.gas_limit {
        Some(g) => g,
        None => match chain.rpc.estimate_gas(pair.key.signer, req.to, &req.data, req.value, chain.rpc.read_opts()).await {
            Ok(g) => (g.saturating_mul(chain.tunables.estimate_multiplier_bps as u64) / 10_000).min(chain.tunables.max_tx_gas),
            Err(RpcError::Response { message, data, .. }) if !message.to_lowercase().contains("insufficient funds") => {
                let revert = data.as_deref().and_then(|d| hex::decode(d.trim_matches('"').trim_start_matches("0x")).ok());
                return fail_unbound(engine, pair, job, JobFailure::SimulationReverted, &format!("gas estimation failed: {message}"), revert.as_deref()).await;
            }
            Err(RpcError::Response { .. }) => {
                // The estimate tripped over the signer's balance, not the call: fund the signer and retry.
                return match ensure_funds(engine, pair, job, req.value).await {
                    Ok(()) => Disposition::Requeue { after_bind: false, backoff: Duration::ZERO },
                    Err(d) => d,
                };
            }
            Err(e) => {
                if let Some(suppressed) = telemetry::throttled(&format!("worker.estimate:{}", chain.chain_id), Duration::from_secs(10)) {
                    tracing::warn!(event = "job.estimate_unavailable", chain = chain.chain_id, job_id = %job.id, error = %e, suppressed, "cannot estimate gas right now; job returned to the queue");
                }
                return Disposition::Requeue { after_bind: false, backoff: Duration::from_secs(1) };
            }
        },
    };

    let fees = match chain.fee_quote(false).await {
        Ok(f) => f,
        Err(e) => {
            if let Some(suppressed) = telemetry::throttled(&format!("worker.fees:{}", chain.chain_id), Duration::from_secs(10)) {
                tracing::warn!(event = "job.fees_unavailable", chain = chain.chain_id, job_id = %job.id, error = %e, suppressed, "cannot read fees right now; job returned to the queue");
            }
            return Disposition::Requeue { after_bind: false, backoff: Duration::from_secs(1) };
        }
    };

    let driver = TxDriver::new(engine, pair);
    let intent = TxIntent { owner: SlotOwner::Job(job.id), purpose: AttemptPurpose::Job, to: req.to, data: req.data.clone(), value: req.value, gas_limit };
    let cost = driver.cost_of(&intent, &fees);
    if cost > chain.cfg.max_job_cost() {
        // Known only now when gas had to be estimated. Never let one job pause signer after signer.
        return fail_unbound(engine, pair, job, JobFailure::InvalidTx, &format!("worst-case cost {cost} wei exceeds max_job_cost {}", chain.cfg.max_job_cost()), None).await;
    }
    if let Err(d) = ensure_funds(engine, pair, job, cost).await {
        return d;
    }
    let prepare_time = started.elapsed();

    let send_started = Instant::now();
    engine.stats.transition(chain.chain_id, (Some(pair.key.signer), Bucket::Processing), (Some(pair.key.signer), Bucket::InFlight));
    match driver.execute(intent, fees).await {
        Outcome::Mined { attempt, inclusion } => {
            let timing = JobTiming { queue: queue_time, prepare: prepare_time, send: send_started.elapsed(), total: job.enqueued_at.elapsed() };
            accounting::mined(engine, pair, &attempt, &inclusion, Some(timing));
            Disposition::Mined
        }
        Outcome::NotBound(reason) => {
            engine.stats.transition(chain.chain_id, (Some(pair.key.signer), Bucket::InFlight), (Some(pair.key.signer), Bucket::Processing));
            match reason {
                NotBound::OwnerGone => {
                    tracing::warn!(event = "job.duplicate_pick", chain = chain.chain_id, job_id = %job.id, "job was already bound elsewhere; dropping this copy");
                    Disposition::Dropped
                }
                NotBound::Signer(e) => {
                    pair.pause(&engine.store, PauseReason::SignerUnavailable, RecoveryStep::AwaitingOperatorResume, format!("signing failed: {e}"));
                    telemetry::alert(
                        &format!("signer.unavailable:{}", pair.key),
                        "signer.unavailable",
                        "signing failed; the pair is paused and will retry periodically",
                        Some(chain.chain_id),
                        Some(&addr_hex(&pair.key.signer)),
                        serde_json::json!({"code": e.code(), "error": e.to_string()}),
                    );
                    schedule_signer_retry(engine.clone(), pair.clone());
                    Disposition::Requeue { after_bind: false, backoff: Duration::ZERO }
                }
                NotBound::Store(e) => {
                    if let Some(suppressed) = telemetry::throttled(&format!("worker.store:{}", chain.chain_id), Duration::from_secs(10)) {
                        tracing::error!(event = "job.bind_failed", chain = chain.chain_id, job_id = %job.id, code = e.code(), error = %e, suppressed, "could not persist the transaction before sending; job returned to the queue");
                    }
                    Disposition::Requeue { after_bind: false, backoff: Duration::from_secs(1) }
                }
                NotBound::Rpc(_) => Disposition::Requeue { after_bind: false, backoff: Duration::from_secs(1) },
            }
        }
        Outcome::Voided { requeued: true, reason } => {
            tracing::warn!(event = "job.requeued", chain = chain.chain_id, job_id = %job.id, reason = %reason, "job returned to the queue with its nonce released");
            Disposition::Requeue { after_bind: true, backoff: Duration::ZERO }
        }
        Outcome::Voided { requeued: false, reason } => {
            metrics::counter!("gum_jobs_failed_total", "chain" => chain.name.clone(), "code" => "invalid_tx").increment(1);
            tracing::warn!(event = "job.failed", chain = chain.chain_id, signer = %addr_hex(&pair.key.signer), job_id = %job.id, code = "invalid_tx", reason = %reason, "job failed: the node rejected the transaction as invalid");
            Disposition::FailedAfterBind
        }
        Outcome::NeedsRecovery => {
            pair.pause(&engine.store, PauseReason::Reorg, RecoveryStep::ReconcilingNonce, "an earlier transaction of this signer is missing on-chain");
            pair.request_recovery();
            Disposition::LeftInFlight
        }
        Outcome::Unresolved | Outcome::Aborted => Disposition::LeftInFlight,
    }
}

/// Makes sure the pair can pay `cost` and stays above its floor, topping it up from the treasury if not.
/// When the treasury cannot help, the pair pauses and the (still unbound) job goes back to the front.
async fn ensure_funds(engine: &Arc<Engine>, pair: &Arc<Pair>, job: &QueuedJob, cost: U256) -> Result<(), Disposition> {
    let chain = &pair.chain;
    let value = job.request.value;
    let available = treasury::spendable_for(pair, value);
    if available >= cost && available >= chain.cfg.signer_min_balance {
        return Ok(());
    }
    pair.pause(
        &engine.store,
        PauseReason::InsufficientFunds,
        RecoveryStep::AwaitingTreasuryTopup,
        format!("available {available} wei; needs {cost} wei and a floor of {} wei", chain.cfg.signer_min_balance),
    );
    match treasury::request_topup(engine, pair, cost, value).await {
        Ok(()) => {
            pair.resume_if(&engine.store, PauseReason::InsufficientFunds);
            Ok(())
        }
        Err(reason) => {
            pair.pause(&engine.store, PauseReason::InsufficientFunds, RecoveryStep::AwaitingTreasuryRefill, reason.clone());
            match engine.store.requeue_front(job.id).await {
                Ok(count) if count > engine.cfg.queue.max_requeues => {
                    Err(fail_unbound(engine, pair, job, JobFailure::Internal, &format!("returned to the queue {count} times for lack of funds: {reason}"), None).await)
                }
                Ok(_) => Err(Disposition::Requeue { after_bind: false, backoff: Duration::ZERO }),
                Err(e) => {
                    tracing::warn!(event = "job.requeue_failed", chain = chain.chain_id, job_id = %job.id, code = e.code(), error = %e, "could not record the requeue; the job keeps its place in memory");
                    Err(Disposition::Requeue { after_bind: false, backoff: Duration::ZERO })
                }
            }
        }
    }
}

async fn fail_unbound(engine: &Arc<Engine>, pair: &Arc<Pair>, job: &QueuedJob, failure: JobFailure, message: &str, revert_data: Option<&[u8]>) -> Disposition {
    let chain = &pair.chain;
    match engine.store.fail_job(job.id, &[JobStatus::Queued], failure, message, revert_data).await {
        Ok(true) => {
            engine.webhook_wake.notify_one();
            metrics::counter!("gum_jobs_failed_total", "chain" => chain.name.clone(), "code" => failure.code()).increment(1);
            tracing::info!(event = "job.failed", chain = chain.chain_id, job_id = %job.id, code = failure.code(), reason = message, "job failed before it was sent");
            Disposition::Failed
        }
        Ok(false) => Disposition::Dropped,
        Err(e) => {
            if let Some(suppressed) = telemetry::throttled(&format!("worker.fail:{}", chain.chain_id), Duration::from_secs(10)) {
                tracing::error!(event = "job.fail_write_failed", chain = chain.chain_id, job_id = %job.id, code = e.code(), error = %e, suppressed, "could not record job failure; job returned to the queue");
            }
            Disposition::Requeue { after_bind: false, backoff: Duration::from_secs(1) }
        }
    }
}

/// A signing failure is usually transient (KMS throttling, network): try the pair again shortly.
fn schedule_signer_retry(engine: Arc<Engine>, pair: Arc<Pair>) {
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(15)).await;
        pair.resume_if(&engine.store, PauseReason::SignerUnavailable);
    });
}
