//! In-memory bookkeeping for state changes: live counters, queue tracking and the per-job log line.
//! Shared by workers, recovery and the confirmation verifier so they cannot drift apart.

use std::time::Duration;

use crate::{
    chain::adapter::ConfirmCheck,
    domain::{addr_hex, hash_hex, Attempt, AttemptPurpose, Inclusion, Outcome, SlotOwner},
    engine::{Engine, Pair},
    stats::Bucket,
};

/// Where a job's time went; logged once, when the job's transaction is mined.
#[derive(Debug, Clone, Copy, Default)]
pub struct JobTiming {
    pub queue: Duration,
    pub prepare: Duration,
    pub send: Duration,
    pub total: Duration,
}

fn terminal_bucket(outcome: Outcome) -> Bucket {
    match outcome {
        Outcome::Success => Bucket::Succeeded,
        Outcome::Reverted => Bucket::Reverted,
    }
}

/// An attempt was mined.
pub fn mined(engine: &Engine, pair: &Pair, attempt: &Attempt, inclusion: &Inclusion, timing: Option<JobTiming>) {
    let SlotOwner::Job(job_id) = attempt.owner else { return };
    let chain = &pair.chain;
    let signer = Some(pair.key.signer);
    let immediate = chain.confirm_check() == ConfirmCheck::Immediate;

    match attempt.purpose {
        AttemptPurpose::Job => {
            engine.stats.transition(chain.chain_id, (signer, Bucket::InFlight), (signer, Bucket::Included));
            if immediate {
                engine.stats.transition(chain.chain_id, (signer, Bucket::Included), (signer, terminal_bucket(inclusion.outcome)));
                chain.queue.forget(&job_id);
            }
            let t = timing.unwrap_or_default();
            metrics::counter!("gum_jobs_included_total", "chain" => chain.name.clone(), "outcome" => inclusion.outcome.as_str()).increment(1);
            if timing.is_some() {
                metrics::histogram!("gum_job_accept_to_included_seconds", "chain" => chain.name.clone()).record(t.total.as_secs_f64());
            }
            // The one routine log line per job.
            tracing::info!(
                event = if immediate { "job.confirmed" } else { "job.included" },
                chain = chain.chain_id,
                signer = %addr_hex(&pair.key.signer),
                job_id = %job_id,
                nonce = attempt.nonce,
                tx_hash = %hash_hex(&inclusion.tx_hash),
                block = inclusion.block_number,
                outcome = inclusion.outcome.as_str(),
                gas_used = inclusion.gas_used,
                fee_paid = %inclusion.fee_paid,
                queue_ms = t.queue.as_millis() as u64,
                prepare_ms = t.prepare.as_millis() as u64,
                send_ms = t.send.as_millis() as u64,
                total_ms = t.total.as_millis() as u64,
                recovered = timing.is_none(),
                "transaction mined"
            );
        }
        AttemptPurpose::Cancel => {
            if immediate {
                cancelled(engine, pair, job_id, attempt);
            }
        }
        AttemptPurpose::Topup => {}
    }
}

/// An included attempt passed its confirmation check (chains with a confirmation delay).
pub fn confirmed(engine: &Engine, pair: &Pair, attempt: &Attempt, inclusion: &Inclusion) {
    let SlotOwner::Job(job_id) = attempt.owner else { return };
    let chain = &pair.chain;
    let signer = Some(pair.key.signer);
    match attempt.purpose {
        AttemptPurpose::Job => {
            engine.stats.transition(chain.chain_id, (signer, Bucket::Included), (signer, terminal_bucket(inclusion.outcome)));
            chain.queue.forget(&job_id);
            tracing::debug!(event = "job.confirmed", chain = chain.chain_id, job_id = %job_id, tx_hash = %hash_hex(&inclusion.tx_hash), block = inclusion.block_number, "inclusion confirmed");
        }
        AttemptPurpose::Cancel => cancelled(engine, pair, job_id, attempt),
        AttemptPurpose::Topup => {}
    }
}

fn cancelled(engine: &Engine, pair: &Pair, job_id: uuid::Uuid, attempt: &Attempt) {
    let chain = &pair.chain;
    let signer = Some(pair.key.signer);
    engine.stats.transition(chain.chain_id, (signer, Bucket::InFlight), (signer, Bucket::Failed));
    chain.queue.forget(&job_id);
    metrics::counter!("gum_jobs_failed_total", "chain" => chain.name.clone(), "code" => "stuck_cancelled").increment(1);
    tracing::warn!(event = "job.failed", chain = chain.chain_id, signer = %addr_hex(&pair.key.signer), job_id = %job_id, nonce = attempt.nonce, code = "stuck_cancelled", "job failed: its nonce was cancelled after it could not be mined");
}

/// A recorded inclusion was re-orged away; the job is in flight again.
pub fn demoted(engine: &Engine, pair: &Pair, attempt: &Attempt) {
    if let (SlotOwner::Job(_), AttemptPurpose::Job) = (attempt.owner, attempt.purpose) {
        let signer = Some(pair.key.signer);
        engine.stats.transition(pair.chain.chain_id, (signer, Bucket::Included), (signer, Bucket::InFlight));
    }
}
