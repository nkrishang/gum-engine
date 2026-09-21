//! Signed transactions and the state changes they drive.
//!
//! The functions here carry the engine's core guarantees:
//!
//! - [`Store::bind_attempt`] runs *before* any broadcast and is the only way a nonce gets used. One
//!   transaction, gated on the lease epoch, that compare-and-sets the nonce slot, stores the signed bytes
//!   and moves the owner forward. If it does not commit, nothing may be sent.
//! - [`Store::settle_inclusion`] / [`Store::settle_confirmation`] are idempotent: each is gated on the
//!   attempt's current status, so a retry (or recovery re-deriving the same receipt) changes nothing and
//!   can never double-count gas or duplicate a webhook.

use alloy::primitives::Bytes;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use super::{jobs::job_row, misc, outbox, parse_addr, parse_hash, parse_u128, parse_u256, Store};
use crate::{
    domain::{addr_hex, hash_hex, Attempt, AttemptPurpose, AttemptStatus, Inclusion, JobId, SlotOwner, WebhookEvent},
    error::{JobFailure, StoreError},
};

/// How a new attempt relates to its nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BindKind {
    /// First use of the nonce: claims the slot and moves the owner out of its initial state.
    First,
    /// Another attempt at a nonce this owner already holds (fee replacement).
    Replacement,
    /// A zero-value self-transfer replacing the job's transaction; the job becomes `cancelling`.
    Cancel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VoidMode {
    NonceUnused,
    NonceTakenByForeignTx,
}

#[derive(Debug, Clone, Copy)]
pub enum VoidDisposition<'a> {
    Requeue,
    Fail { failure: JobFailure, message: &'a str },
}

/// What a settlement did. `Noop` means the state change had already been applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Settled {
    Applied,
    Noop,
}

const ATTEMPT_COLUMNS: &str = "id, chain_id, signer, nonce, purpose, job_id, topup_id, tx_hash, raw_tx, gas_limit, \
     max_fee_per_gas::text AS max_fee_per_gas, max_priority_fee_per_gas::text AS max_priority_fee_per_gas, \
     value::text AS value, status, block_number, block_hash, created_at";

fn attempt_row(row: &sqlx::postgres::PgRow) -> Result<Attempt, StoreError> {
    let purpose: AttemptPurpose = row.try_get::<String, _>("purpose")?.parse().map_err(StoreError::Corrupt)?;
    let job_id: Option<Uuid> = row.try_get("job_id")?;
    let topup_id: Option<Uuid> = row.try_get("topup_id")?;
    let owner = match (job_id, topup_id) {
        (Some(id), _) => SlotOwner::Job(id),
        (None, Some(id)) => SlotOwner::Topup(id),
        (None, None) => return Err(StoreError::Corrupt("attempt has neither job_id nor topup_id".into())),
    };
    Ok(Attempt {
        id: row.try_get("id")?,
        chain_id: row.try_get::<i64, _>("chain_id")? as u64,
        signer: parse_addr(&row.try_get::<String, _>("signer")?)?,
        nonce: row.try_get::<i64, _>("nonce")? as u64,
        purpose,
        owner,
        tx_hash: parse_hash(&row.try_get::<String, _>("tx_hash")?)?,
        raw_tx: Bytes::from(row.try_get::<Option<Vec<u8>>, _>("raw_tx")?.unwrap_or_default()),
        gas_limit: row.try_get::<i64, _>("gas_limit")? as u64,
        max_fee_per_gas: parse_u128(&row.try_get::<String, _>("max_fee_per_gas")?)?,
        max_priority_fee_per_gas: parse_u128(&row.try_get::<String, _>("max_priority_fee_per_gas")?)?,
        value: parse_u256(&row.try_get::<String, _>("value")?)?,
        status: row.try_get::<String, _>("status")?.parse::<AttemptStatus>().map_err(StoreError::Corrupt)?,
        block_number: row.try_get::<Option<i64>, _>("block_number")?.map(|b| b as u64),
        block_hash: row.try_get::<Option<String>, _>("block_hash")?.map(|h| parse_hash(&h)).transpose()?,
        created_at: row.try_get("created_at")?,
    })
}

impl Store {
    /// Persists a signed transaction and binds its nonce. MUST commit before the first broadcast.
    ///
    /// Fails with `Fenced` when this process is not the current leader, `NonceSlotTaken` when the nonce
    /// already belongs to someone else, and `StateConflict` when the owner is not in the expected state
    /// (e.g. the job was picked up twice). In all of those cases nothing was written and nothing may be sent.
    pub async fn bind_attempt(&self, epoch: i64, attempt: &Attempt, kind: BindKind) -> Result<(), StoreError> {
        let mut tx = self.pipeline.begin().await?;

        // FOR SHARE orders this bind against a new leader's epoch bump: either we commit first (under the
        // old epoch, and the new leader will see our rows) or we observe the new epoch and stop.
        let current: i64 = sqlx::query_scalar("SELECT epoch FROM lease_epoch WHERE id FOR SHARE").fetch_one(&mut *tx).await?;
        if current != epoch {
            return Err(StoreError::Fenced { mine: epoch });
        }

        let signer = addr_hex(&attempt.signer);
        let chain_id = attempt.chain_id as i64;
        let nonce = attempt.nonce as i64;

        match kind {
            BindKind::First => {
                let claimed = sqlx::query("INSERT INTO nonce_slots (chain_id, signer, nonce, owner_kind, owner_id) VALUES ($1, $2, $3, $4, $5) ON CONFLICT DO NOTHING")
                    .bind(chain_id)
                    .bind(&signer)
                    .bind(nonce)
                    .bind(attempt.owner.owner_kind())
                    .bind(attempt.owner.id())
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                if claimed != 1 {
                    return Err(StoreError::NonceSlotTaken { chain_id: attempt.chain_id, signer, nonce: attempt.nonce });
                }
            }
            BindKind::Replacement | BindKind::Cancel => {
                let owner: Option<Uuid> = sqlx::query_scalar("SELECT owner_id FROM nonce_slots WHERE chain_id = $1 AND signer = $2 AND nonce = $3")
                    .bind(chain_id)
                    .bind(&signer)
                    .bind(nonce)
                    .fetch_optional(&mut *tx)
                    .await?;
                if owner != Some(attempt.owner.id()) {
                    return Err(StoreError::NonceSlotTaken { chain_id: attempt.chain_id, signer, nonce: attempt.nonce });
                }
            }
        }

        let (job_id, topup_id) = match attempt.owner {
            SlotOwner::Job(id) => (Some(id), None),
            SlotOwner::Topup(id) => (None, Some(id)),
        };
        sqlx::query(
            "INSERT INTO tx_attempts (id, chain_id, signer, nonce, purpose, job_id, topup_id, tx_hash, raw_tx, gas_limit, max_fee_per_gas, max_priority_fee_per_gas, value, status) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11::numeric, $12::numeric, $13::numeric, 'broadcast')",
        )
        .bind(attempt.id)
        .bind(chain_id)
        .bind(&signer)
        .bind(nonce)
        .bind(attempt.purpose.as_str())
        .bind(job_id)
        .bind(topup_id)
        .bind(hash_hex(&attempt.tx_hash))
        .bind(attempt.raw_tx.as_ref())
        .bind(attempt.gas_limit as i64)
        .bind(attempt.max_fee_per_gas.to_string())
        .bind(attempt.max_priority_fee_per_gas.to_string())
        .bind(attempt.value.to_string())
        .execute(&mut *tx)
        .await?;

        let moved = match (attempt.owner, kind) {
            (SlotOwner::Job(id), BindKind::First) => {
                sqlx::query("UPDATE jobs SET status = 'sent', signer = $2, nonce = $3, tx_hash = $4, sent_at = now() WHERE id = $1 AND status = 'queued'")
                    .bind(id)
                    .bind(&signer)
                    .bind(nonce)
                    .bind(hash_hex(&attempt.tx_hash))
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
            }
            (SlotOwner::Job(id), BindKind::Replacement) => sqlx::query("UPDATE jobs SET tx_hash = $2 WHERE id = $1 AND status IN ('sent', 'cancelling')")
                .bind(id)
                .bind(hash_hex(&attempt.tx_hash))
                .execute(&mut *tx)
                .await?
                .rows_affected(),
            (SlotOwner::Job(id), BindKind::Cancel) => {
                sqlx::query("UPDATE jobs SET status = 'cancelling' WHERE id = $1 AND status IN ('sent', 'cancelling')").bind(id).execute(&mut *tx).await?.rows_affected()
            }
            (SlotOwner::Topup(id), BindKind::First) => {
                sqlx::query("UPDATE topups SET status = 'sent', updated_at = now() WHERE id = $1 AND status = 'requested'").bind(id).execute(&mut *tx).await?.rows_affected()
            }
            (SlotOwner::Topup(id), _) => sqlx::query("UPDATE topups SET updated_at = now() WHERE id = $1 AND status = 'sent'").bind(id).execute(&mut *tx).await?.rows_affected(),
        };
        if moved != 1 {
            return Err(StoreError::StateConflict { entity: attempt.owner.owner_kind(), id: attempt.owner.id().to_string(), expected: "bindable" });
        }

        sqlx::query("UPDATE pairs SET nonce_hwm = GREATEST(COALESCE(nonce_hwm, -1), $3), updated_at = now() WHERE chain_id = $1 AND signer = $2")
            .bind(chain_id)
            .bind(&signer)
            .bind(nonce)
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    /// True when an attempt with this hash is stored. Used to resolve an ambiguous bind commit (the
    /// connection died at COMMIT): the answer decides whether the bytes may be broadcast.
    pub async fn attempt_exists(&self, tx_hash: &alloy::primitives::B256) -> Result<bool, StoreError> {
        let found: Option<Uuid> = sqlx::query_scalar("SELECT id FROM tx_attempts WHERE tx_hash = $1").bind(hash_hex(tx_hash)).fetch_optional(&self.pipeline).await?;
        Ok(found.is_some())
    }

    /// Records that `attempt` was mined. With `confirm_now` (chains that finalize on inclusion) the
    /// confirmation is applied in the same transaction.
    pub async fn settle_inclusion(&self, attempt: &Attempt, inc: &Inclusion, confirm_now: bool, reincluded: bool) -> Result<Settled, StoreError> {
        let mut tx = self.pipeline.begin().await?;
        let signer = addr_hex(&attempt.signer);

        let gate = sqlx::query("UPDATE tx_attempts SET status = 'included', block_number = $2, block_hash = $3, gas_used = $4::numeric, fee_paid = $5::numeric, updated_at = now() WHERE id = $1 AND status = 'broadcast'")
            .bind(attempt.id)
            .bind(inc.block_number as i64)
            .bind(hash_hex(&inc.block_hash))
            .bind(inc.gas_used.to_string())
            .bind(inc.fee_paid.to_string())
            .execute(&mut *tx)
            .await?
            .rows_affected();
        if gate != 1 {
            return Ok(Settled::Noop);
        }

        // Only one attempt per nonce can be mined; the others lost.
        sqlx::query("UPDATE tx_attempts SET status = 'replaced', updated_at = now() WHERE chain_id = $1 AND signer = $2 AND nonce = $3 AND id <> $4 AND status = 'broadcast'")
            .bind(attempt.chain_id as i64)
            .bind(&signer)
            .bind(attempt.nonce as i64)
            .bind(attempt.id)
            .execute(&mut *tx)
            .await?;

        misc::add_gas(&mut tx, attempt.chain_id, &signer, attempt.purpose, 1, &inc.gas_used.to_string(), &inc.fee_paid.to_string()).await?;

        match (attempt.owner, attempt.purpose) {
            (SlotOwner::Job(job_id), AttemptPurpose::Job) => {
                let sql = format!(
                    "UPDATE jobs SET status = 'included', outcome = $2, tx_hash = $3, block_number = $4, block_hash = $5, gas_used = $6::numeric, \
                     effective_gas_price = $7::numeric, fee_paid = $8::numeric, l1_fee = $9::numeric, receipt = $10, included_at = now(), webhook_seq = webhook_seq + 1 \
                     WHERE id = $1 AND status IN ('sent', 'cancelling') RETURNING {JOB_RETURNING}"
                );
                let row = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(job_id)
                    .bind(inc.outcome.as_str())
                    .bind(hash_hex(&inc.tx_hash))
                    .bind(inc.block_number as i64)
                    .bind(hash_hex(&inc.block_hash))
                    .bind(inc.gas_used.to_string())
                    .bind(inc.effective_gas_price.to_string())
                    .bind(inc.fee_paid.to_string())
                    .bind(inc.l1_fee.map(|f| f.to_string()))
                    .bind(&inc.receipt)
                    .fetch_optional(&mut *tx)
                    .await?
                    .ok_or(StoreError::StateConflict { entity: "job", id: job_id.to_string(), expected: "sent|cancelling" })?;
                let job = job_row(&row)?;
                outbox::enqueue(&mut tx, &job, WebhookEvent::Included, row.try_get("webhook_seq")?, reincluded).await?;
            }
            // A mined cancel consumes the nonce; the job only fails once that is confirmed.
            (SlotOwner::Job(_), _) => {}
            (SlotOwner::Topup(id), _) => {
                sqlx::query("UPDATE topups SET status = 'included', updated_at = now() WHERE id = $1 AND status = 'sent'").bind(id).execute(&mut *tx).await?;
            }
        }

        if confirm_now {
            confirm_in_tx(&mut tx, attempt).await?;
        }
        tx.commit().await?;
        Ok(Settled::Applied)
    }

    /// Records that an included attempt passed its confirmation check.
    pub async fn settle_confirmation(&self, attempt: &Attempt) -> Result<Settled, StoreError> {
        let mut tx = self.pipeline.begin().await?;
        let applied = confirm_in_tx(&mut tx, attempt).await?;
        tx.commit().await?;
        Ok(applied)
    }

    /// The transaction is still mined, but in a different block than first recorded (a shallow re-org, or
    /// a pre-confirmation receipt whose block hash was provisional). Keeps the stored inclusion — and with
    /// it the `transaction.confirmed` webhook — pointing at the canonical block.
    pub async fn update_inclusion_block(&self, attempt: &Attempt, block_number: u64, block_hash: &alloy::primitives::B256, receipt: &serde_json::Value) -> Result<(), StoreError> {
        let mut tx = self.pipeline.begin().await?;
        sqlx::query("UPDATE tx_attempts SET block_number = $2, block_hash = $3, updated_at = now() WHERE id = $1 AND status = 'included'")
            .bind(attempt.id)
            .bind(block_number as i64)
            .bind(hash_hex(block_hash))
            .execute(&mut *tx)
            .await?;
        if let (SlotOwner::Job(job_id), AttemptPurpose::Job) = (attempt.owner, attempt.purpose) {
            sqlx::query("UPDATE jobs SET block_number = $2, block_hash = $3, receipt = $4 WHERE id = $1 AND status = 'included'")
                .bind(job_id)
                .bind(block_number as i64)
                .bind(hash_hex(block_hash))
                .bind(receipt)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    /// An included attempt vanished from the canonical chain (re-org): take everything the inclusion
    /// changed back, so recovery can rebroadcast the same bytes. Synchronous by design — later writes for
    /// this nonce would otherwise trip the one-mined-attempt-per-nonce index.
    pub async fn demote_inclusion(&self, attempt: &Attempt) -> Result<Settled, StoreError> {
        let mut tx = self.pipeline.begin().await?;
        let row = sqlx::query("UPDATE tx_attempts SET status = 'broadcast', block_number = NULL, block_hash = NULL, updated_at = now() WHERE id = $1 AND status = 'included' RETURNING gas_used::text AS gas_used, fee_paid::text AS fee_paid")
            .bind(attempt.id)
            .fetch_optional(&mut *tx)
            .await?;
        let Some(row) = row else { return Ok(Settled::Noop) };
        let gas_used: Option<String> = row.try_get("gas_used")?;
        let fee_paid: Option<String> = row.try_get("fee_paid")?;
        misc::add_gas(
            &mut tx,
            attempt.chain_id,
            &addr_hex(&attempt.signer),
            attempt.purpose,
            -1,
            &format!("-{}", gas_used.unwrap_or_else(|| "0".into())),
            &format!("-{}", fee_paid.unwrap_or_else(|| "0".into())),
        )
        .await?;

        match (attempt.owner, attempt.purpose) {
            (SlotOwner::Job(id), AttemptPurpose::Job) => {
                sqlx::query("UPDATE jobs SET status = 'sent', outcome = NULL, block_number = NULL, block_hash = NULL, gas_used = NULL, effective_gas_price = NULL, fee_paid = NULL, l1_fee = NULL, receipt = NULL, included_at = NULL WHERE id = $1 AND status = 'included'")
                    .bind(id)
                    .execute(&mut *tx)
                    .await?;
            }
            (SlotOwner::Job(_), _) => {}
            (SlotOwner::Topup(id), _) => {
                sqlx::query("UPDATE topups SET status = 'sent', updated_at = now() WHERE id = $1 AND status = 'included'").bind(id).execute(&mut *tx).await?;
            }
        }
        tx.commit().await?;
        Ok(Settled::Applied)
    }

    /// Undoes a binding whose transaction provably never executed and never can.
    ///
    /// - `NonceUnused`: the node rejected the bytes by a stateless rule before they entered any pool. The
    ///   slot is released and the high-water mark stepped back, so the nonce is used by the next send
    ///   instead of becoming a gap.
    /// - `NonceTakenByForeignTx`: the nonce was consumed on-chain by a transaction that is not ours. The
    ///   slot stays (the nonce is gone for good).
    ///
    /// The job is either failed (with its webhook) or returned to the queue, per `disposition`.
    pub async fn void_binding(&self, epoch: i64, attempt: &Attempt, mode: VoidMode, disposition: VoidDisposition<'_>) -> Result<(), StoreError> {
        let mut tx = self.pipeline.begin().await?;
        let current: i64 = sqlx::query_scalar("SELECT epoch FROM lease_epoch WHERE id FOR SHARE").fetch_one(&mut *tx).await?;
        if current != epoch {
            return Err(StoreError::Fenced { mine: epoch });
        }
        let signer = addr_hex(&attempt.signer);
        let (chain_id, nonce) = (attempt.chain_id as i64, attempt.nonce as i64);

        sqlx::query("UPDATE tx_attempts SET status = 'dropped', raw_tx = NULL, updated_at = now() WHERE chain_id = $1 AND signer = $2 AND nonce = $3 AND status = 'broadcast'")
            .bind(chain_id)
            .bind(&signer)
            .bind(nonce)
            .execute(&mut *tx)
            .await?;

        if mode == VoidMode::NonceUnused {
            sqlx::query("DELETE FROM nonce_slots WHERE chain_id = $1 AND signer = $2 AND nonce = $3 AND owner_id = $4")
                .bind(chain_id)
                .bind(&signer)
                .bind(nonce)
                .bind(attempt.owner.id())
                .execute(&mut *tx)
                .await?;
            sqlx::query("UPDATE pairs SET nonce_hwm = CASE WHEN $3 = 0 THEN NULL ELSE $3 - 1 END, updated_at = now() WHERE chain_id = $1 AND signer = $2 AND nonce_hwm = $3")
                .bind(chain_id)
                .bind(&signer)
                .bind(nonce)
                .execute(&mut *tx)
                .await?;
        }

        match (attempt.owner, disposition) {
            (SlotOwner::Job(id), VoidDisposition::Requeue) => {
                sqlx::query(
                    "UPDATE jobs SET status = 'queued', signer = NULL, nonce = NULL, tx_hash = NULL, sent_at = NULL, requeue_count = requeue_count + 1, \
                     requeue_rank = -(extract(epoch FROM clock_timestamp()) * 1000000)::bigint WHERE id = $1 AND status IN ('sent', 'cancelling')",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
            (SlotOwner::Job(id), VoidDisposition::Fail { failure, message }) => {
                let sql = format!(
                    "UPDATE jobs SET status = 'failed', error_code = $2, error_message = $3, failed_at = now(), webhook_seq = webhook_seq + 1 \
                     WHERE id = $1 AND status IN ('sent', 'cancelling') RETURNING {JOB_RETURNING}"
                );
                if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(id).bind(failure.code()).bind(message).fetch_optional(&mut *tx).await? {
                    let job = job_row(&row)?;
                    outbox::enqueue(&mut tx, &job, WebhookEvent::Failed, row.try_get("webhook_seq")?, false).await?;
                    misc::bump_rollup(&mut tx, job.chain_id, job.signer.as_deref().unwrap_or(""), "failed").await?;
                }
            }
            (SlotOwner::Topup(id), _) => {
                sqlx::query(
                    "UPDATE topups SET status = 'failed', error = 'transaction was rejected before it could be sent', updated_at = now() WHERE id = $1 AND status = 'sent'",
                )
                .bind(id)
                .execute(&mut *tx)
                .await?;
            }
        }
        tx.commit().await?;
        Ok(())
    }

    /// Attempts that still need attention on a chain: broadcast (no receipt yet) or included (awaiting
    /// confirmation). Ordered so recovery can walk each signer's nonces from the lowest up.
    pub async fn load_live_attempts(&self, chain_id: u64) -> Result<Vec<Attempt>, StoreError> {
        let sql = format!("SELECT {ATTEMPT_COLUMNS} FROM tx_attempts WHERE chain_id = $1 AND status IN ('broadcast', 'included') ORDER BY signer, nonce, created_at");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(chain_id as i64).fetch_all(&self.pipeline).await?;
        rows.iter().map(attempt_row).collect()
    }

    pub async fn load_live_attempts_for(&self, chain_id: u64, signer: &alloy::primitives::Address) -> Result<Vec<Attempt>, StoreError> {
        let sql = format!("SELECT {ATTEMPT_COLUMNS} FROM tx_attempts WHERE chain_id = $1 AND signer = $2 AND status IN ('broadcast', 'included') ORDER BY nonce, created_at");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(chain_id as i64).bind(addr_hex(signer)).fetch_all(&self.pipeline).await?;
        rows.iter().map(attempt_row).collect()
    }

    pub async fn attempts_for_job(&self, job_id: JobId) -> Result<Vec<Attempt>, StoreError> {
        let sql = format!("SELECT {ATTEMPT_COLUMNS} FROM tx_attempts WHERE job_id = $1 ORDER BY created_at");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(job_id).fetch_all(&self.api).await?;
        rows.iter().map(attempt_row).collect()
    }
}

const JOB_RETURNING: &str = "id, chain_id, to_addr, data, value::text AS value, gas_limit, deadline, webhook_url, status, outcome, \
     signer, nonce, tx_hash, block_number, block_hash, gas_used::text AS gas_used, \
     effective_gas_price::text AS effective_gas_price, fee_paid::text AS fee_paid, l1_fee::text AS l1_fee, \
     error_code, error_message, revert_data, receipt, created_at, sent_at, included_at, confirmed_at, failed_at, webhook_seq";

async fn confirm_in_tx(tx: &mut Transaction<'_, Postgres>, attempt: &Attempt) -> Result<Settled, StoreError> {
    // The raw bytes are only needed for rebroadcast; a confirmed transaction never needs that again.
    let gate = sqlx::query("UPDATE tx_attempts SET status = 'confirmed', raw_tx = NULL, updated_at = now() WHERE id = $1 AND status = 'included'")
        .bind(attempt.id)
        .execute(&mut **tx)
        .await?
        .rows_affected();
    if gate != 1 {
        return Ok(Settled::Noop);
    }
    sqlx::query("UPDATE tx_attempts SET raw_tx = NULL WHERE chain_id = $1 AND signer = $2 AND nonce = $3 AND status = 'replaced'")
        .bind(attempt.chain_id as i64)
        .bind(addr_hex(&attempt.signer))
        .bind(attempt.nonce as i64)
        .execute(&mut **tx)
        .await?;

    match (attempt.owner, attempt.purpose) {
        (SlotOwner::Job(job_id), AttemptPurpose::Job) => {
            let sql = format!(
                "UPDATE jobs SET status = 'confirmed', confirmed_at = now(), webhook_seq = webhook_seq + 1 WHERE id = $1 AND status = 'included' RETURNING {JOB_RETURNING}"
            );
            if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(job_id).fetch_optional(&mut **tx).await? {
                let job = job_row(&row)?;
                outbox::enqueue(tx, &job, WebhookEvent::Confirmed, row.try_get("webhook_seq")?, false).await?;
                let bucket = if job.outcome.as_deref() == Some("success") { "succeeded" } else { "reverted" };
                misc::bump_rollup(tx, job.chain_id, job.signer.as_deref().unwrap_or(""), bucket).await?;
            }
        }
        // The cancel is confirmed: the nonce was provably consumed by a different transaction, so the
        // job's own transaction can never execute. Only now may the job fail.
        (SlotOwner::Job(job_id), _) => {
            let sql = format!(
                "UPDATE jobs SET status = 'failed', error_code = $2, error_message = $3, failed_at = now(), webhook_seq = webhook_seq + 1 \
                 WHERE id = $1 AND status = 'cancelling' RETURNING {JOB_RETURNING}"
            );
            if let Some(row) = sqlx::query(sqlx::AssertSqlSafe(sql))
                .bind(job_id)
                .bind(JobFailure::StuckCancelled.code())
                .bind("the transaction could not be mined within the fee cap; its nonce was cancelled")
                .fetch_optional(&mut **tx)
                .await?
            {
                let job = job_row(&row)?;
                outbox::enqueue(tx, &job, WebhookEvent::Failed, row.try_get("webhook_seq")?, false).await?;
                misc::bump_rollup(tx, job.chain_id, job.signer.as_deref().unwrap_or(""), "failed").await?;
            }
        }
        (SlotOwner::Topup(id), _) => {
            sqlx::query("UPDATE topups SET status = 'confirmed', updated_at = now() WHERE id = $1 AND status = 'included'").bind(id).execute(&mut **tx).await?;
        }
    }
    Ok(Settled::Applied)
}
