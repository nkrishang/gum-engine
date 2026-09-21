//! Job rows: batched ingest, queue loading, requeue, failure, and reads for the API.

use alloy::primitives::Bytes;
use chrono::{DateTime, Utc};
use sqlx::Row;
use uuid::Uuid;

use super::{outbox, parse_addr, parse_u256, Store};
use crate::{
    domain::{JobId, JobRequest, JobStatus, QueuedJob, WebhookEvent},
    error::{JobFailure, StoreError},
};

/// A validated request waiting to be written.
#[derive(Debug, Clone)]
pub struct NewJob {
    pub id: JobId,
    pub request: JobRequest,
    pub idempotency_key: Option<String>,
    /// Hash of the canonical request body; tells a replay from a conflicting reuse of a key.
    pub request_hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InsertOutcome {
    Created,
    /// The idempotency key was seen before with the same body; this is the original job.
    Replayed(JobId),
    /// The idempotency key was seen before with a different body.
    Conflict,
}

/// A job as the API returns it.
#[derive(Debug, Clone)]
pub struct JobRow {
    pub id: JobId,
    pub chain_id: u64,
    pub to_addr: String,
    pub data: Vec<u8>,
    pub value: String,
    pub gas_limit: Option<i64>,
    pub deadline: Option<DateTime<Utc>>,
    pub webhook_url: String,
    pub status: String,
    pub outcome: Option<String>,
    pub signer: Option<String>,
    pub nonce: Option<i64>,
    pub tx_hash: Option<String>,
    pub block_number: Option<i64>,
    pub block_hash: Option<String>,
    pub gas_used: Option<String>,
    pub effective_gas_price: Option<String>,
    pub fee_paid: Option<String>,
    pub l1_fee: Option<String>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub revert_data: Option<Vec<u8>>,
    pub receipt: Option<serde_json::Value>,
    pub created_at: DateTime<Utc>,
    pub sent_at: Option<DateTime<Utc>>,
    pub included_at: Option<DateTime<Utc>>,
    pub confirmed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
}

const JOB_COLUMNS: &str = "id, chain_id, to_addr, data, value::text AS value, gas_limit, deadline, webhook_url, status, outcome, \
     signer, nonce, tx_hash, block_number, block_hash, gas_used::text AS gas_used, \
     effective_gas_price::text AS effective_gas_price, fee_paid::text AS fee_paid, l1_fee::text AS l1_fee, \
     error_code, error_message, revert_data, receipt, created_at, sent_at, included_at, confirmed_at, failed_at";

pub(crate) fn job_row(row: &sqlx::postgres::PgRow) -> Result<JobRow, StoreError> {
    Ok(JobRow {
        id: row.try_get("id")?,
        chain_id: row.try_get::<i64, _>("chain_id")? as u64,
        to_addr: row.try_get("to_addr")?,
        data: row.try_get("data")?,
        value: row.try_get("value")?,
        gas_limit: row.try_get("gas_limit")?,
        deadline: row.try_get("deadline")?,
        webhook_url: row.try_get("webhook_url")?,
        status: row.try_get("status")?,
        outcome: row.try_get("outcome")?,
        signer: row.try_get("signer")?,
        nonce: row.try_get("nonce")?,
        tx_hash: row.try_get("tx_hash")?,
        block_number: row.try_get("block_number")?,
        block_hash: row.try_get("block_hash")?,
        gas_used: row.try_get("gas_used")?,
        effective_gas_price: row.try_get("effective_gas_price")?,
        fee_paid: row.try_get("fee_paid")?,
        l1_fee: row.try_get("l1_fee")?,
        error_code: row.try_get("error_code")?,
        error_message: row.try_get("error_message")?,
        revert_data: row.try_get("revert_data")?,
        receipt: row.try_get("receipt")?,
        created_at: row.try_get("created_at")?,
        sent_at: row.try_get("sent_at")?,
        included_at: row.try_get("included_at")?,
        confirmed_at: row.try_get("confirmed_at")?,
        failed_at: row.try_get("failed_at")?,
    })
}

impl JobRow {
    pub fn to_queued(&self) -> Result<QueuedJob, StoreError> {
        Ok(QueuedJob {
            id: self.id,
            request: JobRequest {
                chain_id: self.chain_id,
                to: parse_addr(&self.to_addr)?,
                data: Bytes::from(self.data.clone()),
                value: parse_u256(&self.value)?,
                gas_limit: self.gas_limit.map(|g| g as u64),
                deadline: self.deadline,
                webhook_url: self.webhook_url.clone(),
            },
            requeue_count: 0,
            created_at: self.created_at,
            enqueued_at: std::time::Instant::now(),
        })
    }
}

impl Store {
    /// Writes a batch of accepted jobs in one statement. One duplicate idempotency key never fails the
    /// rest of the batch: conflicting rows are skipped and resolved individually afterwards.
    pub async fn insert_jobs(&self, jobs: &[NewJob]) -> Result<Vec<InsertOutcome>, StoreError> {
        if jobs.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::with_capacity(jobs.len());
        let mut chain_ids = Vec::with_capacity(jobs.len());
        let mut idem = Vec::with_capacity(jobs.len());
        let mut hashes = Vec::with_capacity(jobs.len());
        let mut tos = Vec::with_capacity(jobs.len());
        let mut datas: Vec<Vec<u8>> = Vec::with_capacity(jobs.len());
        let mut values = Vec::with_capacity(jobs.len());
        let mut gas: Vec<Option<i64>> = Vec::with_capacity(jobs.len());
        let mut deadlines: Vec<Option<DateTime<Utc>>> = Vec::with_capacity(jobs.len());
        let mut urls = Vec::with_capacity(jobs.len());
        for j in jobs {
            ids.push(j.id);
            chain_ids.push(j.request.chain_id as i64);
            idem.push(j.idempotency_key.clone());
            hashes.push(j.request_hash.clone());
            tos.push(crate::domain::addr_hex(&j.request.to));
            datas.push(j.request.data.to_vec());
            values.push(j.request.value.to_string());
            gas.push(j.request.gas_limit.map(|g| g as i64));
            deadlines.push(j.request.deadline);
            urls.push(j.request.webhook_url.clone());
        }

        let inserted: Vec<Uuid> = sqlx::query_scalar(
            "INSERT INTO jobs (id, chain_id, idempotency_key, request_hash, to_addr, data, value, gas_limit, deadline, webhook_url, status) \
             SELECT u.id, u.chain_id, u.idem, u.rhash, u.to_addr, u.data, u.value::numeric, u.gas_limit, u.deadline, u.url, 'queued' \
             FROM UNNEST($1::uuid[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::bytea[], $7::text[], $8::bigint[], $9::timestamptz[], $10::text[]) \
                  AS u(id, chain_id, idem, rhash, to_addr, data, value, gas_limit, deadline, url) \
             ON CONFLICT (idempotency_key) DO NOTHING \
             RETURNING id",
        )
        .bind(&ids)
        .bind(&chain_ids)
        .bind(&idem)
        .bind(&hashes)
        .bind(&tos)
        .bind(&datas)
        .bind(&values)
        .bind(&gas)
        .bind(&deadlines)
        .bind(&urls)
        .fetch_all(&self.pipeline)
        .await?;

        let inserted: std::collections::HashSet<Uuid> = inserted.into_iter().collect();
        let mut outcomes = Vec::with_capacity(jobs.len());
        for j in jobs {
            if inserted.contains(&j.id) {
                outcomes.push(InsertOutcome::Created);
                continue;
            }
            // Skipped by ON CONFLICT: the key exists (possibly from earlier in this same batch).
            let key = j.idempotency_key.as_deref().unwrap_or_default();
            let existing = sqlx::query("SELECT id, request_hash FROM jobs WHERE idempotency_key = $1").bind(key).fetch_optional(&self.pipeline).await?;
            outcomes.push(match existing {
                Some(row) if row.try_get::<String, _>("request_hash")? == j.request_hash => InsertOutcome::Replayed(row.try_get("id")?),
                Some(_) => InsertOutcome::Conflict,
                None => return Err(StoreError::Corrupt(format!("job {} was neither inserted nor found by its idempotency key", j.id))),
            });
        }
        Ok(outcomes)
    }

    /// The head of a chain's durable queue, in pop order.
    pub async fn load_queued(&self, chain_id: u64, limit: usize) -> Result<Vec<QueuedJob>, StoreError> {
        let sql = format!("SELECT {JOB_COLUMNS}, requeue_count FROM jobs WHERE chain_id = $1 AND status = 'queued' ORDER BY requeue_rank, seq LIMIT $2");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(chain_id as i64).bind(limit as i64).fetch_all(&self.pipeline).await?;
        rows.iter()
            .map(|row| {
                let mut job = job_row(row)?.to_queued()?;
                job.requeue_count = row.try_get::<i32, _>("requeue_count")? as u32;
                Ok(job)
            })
            .collect()
    }

    pub async fn count_queued(&self, chain_id: u64) -> Result<u64, StoreError> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE chain_id = $1 AND status = 'queued'").bind(chain_id as i64).fetch_one(&self.pipeline).await?;
        Ok(n as u64)
    }

    /// Puts an *unbound* job back at the front of its queue. Refused once the job has an attempt: from
    /// then on it belongs to its (signer, nonce) for life.
    pub async fn requeue_front(&self, job_id: JobId) -> Result<u32, StoreError> {
        let count: Option<i32> = sqlx::query_scalar(
            "UPDATE jobs SET requeue_rank = -(extract(epoch FROM clock_timestamp()) * 1000000)::bigint, requeue_count = requeue_count + 1 \
             WHERE id = $1 AND status = 'queued' RETURNING requeue_count",
        )
        .bind(job_id)
        .fetch_optional(&self.pipeline)
        .await?;
        count.map(|c| c as u32).ok_or(StoreError::StateConflict { entity: "job", id: job_id.to_string(), expected: "queued" })
    }

    /// Moves a job to `failed` and enqueues its `transaction.failed` webhook, atomically. `from` lists the
    /// statuses the job may currently be in; a job in any other state is left untouched (returns false).
    pub async fn fail_job(&self, job_id: JobId, from: &[JobStatus], failure: JobFailure, message: &str, revert_data: Option<&[u8]>) -> Result<bool, StoreError> {
        let from: Vec<String> = from.iter().map(|s| s.as_str().to_string()).collect();
        let mut tx = self.pipeline.begin().await?;
        let sql = format!(
            "UPDATE jobs SET status = 'failed', error_code = $2, error_message = $3, revert_data = $4, failed_at = now(), webhook_seq = webhook_seq + 1 \
             WHERE id = $1 AND status = ANY($5) RETURNING {JOB_COLUMNS}, webhook_seq"
        );
        let Some(row) = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(job_id).bind(failure.code()).bind(message).bind(revert_data).bind(&from).fetch_optional(&mut *tx).await? else {
            return Ok(false);
        };
        let job = job_row(&row)?;
        let sequence: i32 = row.try_get("webhook_seq")?;
        outbox::enqueue(&mut tx, &job, WebhookEvent::Failed, sequence, false).await?;
        super::misc::bump_rollup(&mut tx, job.chain_id, job.signer.as_deref().unwrap_or(""), "failed").await?;
        tx.commit().await?;
        Ok(true)
    }

    /// Cancels a job that is still queued (admin).
    pub async fn cancel_queued(&self, job_id: JobId) -> Result<bool, StoreError> {
        self.fail_job(job_id, &[JobStatus::Queued], JobFailure::Cancelled, "cancelled by operator before it was sent", None).await
    }

    pub async fn get_job(&self, job_id: JobId) -> Result<Option<JobRow>, StoreError> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE id = $1");
        let row = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(job_id).fetch_optional(&self.api).await?;
        row.as_ref().map(job_row).transpose()
    }

    /// Non-terminal bound jobs of a chain, for rebuilding in-memory state at boot.
    pub async fn load_live_jobs(&self, chain_id: u64) -> Result<Vec<JobRow>, StoreError> {
        let sql = format!("SELECT {JOB_COLUMNS} FROM jobs WHERE chain_id = $1 AND status IN ('sent', 'included', 'cancelling') ORDER BY signer, nonce");
        let rows = sqlx::query(sqlx::AssertSqlSafe(sql)).bind(chain_id as i64).fetch_all(&self.pipeline).await?;
        rows.iter().map(job_row).collect()
    }
}
