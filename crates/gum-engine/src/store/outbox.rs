//! Webhook outbox. Rows are inserted in the same transaction as the job state change they announce, so
//! an event exists if and only if its state change committed. Delivery happens elsewhere, at-least-once.

use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use super::{jobs::JobRow, Store};
use crate::{domain::WebhookEvent, error::StoreError};

#[derive(Debug, Clone)]
pub struct Delivery {
    pub id: Uuid,
    pub job_id: Uuid,
    pub event: String,
    pub url: String,
    pub payload: serde_json::Value,
    pub attempts: i32,
}

#[derive(Debug, Clone)]
pub struct DeliveryStatus {
    pub event: String,
    pub sequence: i32,
    pub status: String,
    pub attempts: i32,
    pub last_status_code: Option<i32>,
    pub delivered_at: Option<DateTime<Utc>>,
}

/// Builds the webhook body from the job row as it stands *after* the state change.
fn payload(event_id: Uuid, job: &JobRow, event: WebhookEvent, sequence: i32, reincluded: bool) -> serde_json::Value {
    let error = job.error_code.as_ref().map(|code| {
        json!({
            "code": code,
            "message": job.error_message,
            "revert_data": job.revert_data.as_ref().map(|d| format!("0x{}", hex::encode(d))),
        })
    });
    json!({
        "event_id": event_id,
        "event": event.as_str(),
        "sequence": sequence,
        "job_id": job.id,
        "chain_id": job.chain_id,
        "status": job.status,
        "outcome": job.outcome,
        "tx_hash": job.tx_hash,
        "block_number": job.block_number,
        "block_hash": job.block_hash,
        "signer": job.signer,
        "nonce": job.nonce,
        "gas_used": job.gas_used,
        "effective_gas_price": job.effective_gas_price,
        "fee_paid": job.fee_paid,
        "reincluded": reincluded,
        "receipt": job.receipt,
        "error": error,
        "timestamp": Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
    })
}

/// Adds an event for `job` inside the caller's transaction. `version` rises when the same event is
/// legitimately issued again (re-inclusion after a re-org); the unique key makes retries harmless.
pub(crate) async fn enqueue(tx: &mut Transaction<'_, Postgres>, job: &JobRow, event: WebhookEvent, sequence: i32, reincluded: bool) -> Result<Uuid, StoreError> {
    let id = Uuid::now_v7();
    let version: i32 = sqlx::query_scalar("SELECT COALESCE(MAX(version), 0) + 1 FROM webhook_deliveries WHERE job_id = $1 AND event = $2")
        .bind(job.id)
        .bind(event.as_str())
        .fetch_one(&mut **tx)
        .await?;
    // A second `included` for the same job can only mean the first inclusion was re-orged away and the
    // transaction was mined again; say so regardless of which code path noticed.
    let reincluded = reincluded || (event == WebhookEvent::Included && version > 1);
    let body = payload(id, job, event, sequence, reincluded);
    sqlx::query("INSERT INTO webhook_deliveries (id, job_id, event, version, sequence, url, payload, status) VALUES ($1, $2, $3, $4, $5, $6, $7, 'pending')")
        .bind(id)
        .bind(job.id)
        .bind(event.as_str())
        .bind(version)
        .bind(sequence)
        .bind(&job.webhook_url)
        .bind(&body)
        .execute(&mut **tx)
        .await?;
    Ok(id)
}

impl Store {
    /// Claims deliveries that are due. `FOR UPDATE SKIP LOCKED` plus pushing `next_attempt_at` forward
    /// leases each row to this dispatcher for `lease_secs`, so a crash simply lets the lease expire.
    ///
    /// Events of one job are delivered in `sequence` order: an event is not claimable while an earlier
    /// event of the same job is still pending (being delivered, or waiting for a retry). Receivers thus
    /// never see `transaction.confirmed` before `transaction.included`. Jobs never wait on each other.
    pub async fn claim_due_deliveries(&self, limit: i64, lease_secs: i64) -> Result<Vec<Delivery>, StoreError> {
        let rows = sqlx::query(
            "UPDATE webhook_deliveries SET next_attempt_at = now() + make_interval(secs => $2) \
             WHERE id IN (SELECT d.id FROM webhook_deliveries d WHERE d.status = 'pending' AND d.next_attempt_at <= now() \
                            AND NOT EXISTS (SELECT 1 FROM webhook_deliveries e WHERE e.job_id = d.job_id AND e.sequence < d.sequence AND e.status = 'pending') \
                          ORDER BY d.next_attempt_at LIMIT $1 FOR UPDATE OF d SKIP LOCKED) \
             RETURNING id, job_id, event, url, payload, attempts",
        )
        .bind(limit)
        .bind(lease_secs as f64)
        .fetch_all(&self.pipeline)
        .await?;
        rows.iter()
            .map(|r| {
                Ok(Delivery {
                    id: r.try_get("id")?,
                    job_id: r.try_get("job_id")?,
                    event: r.try_get("event")?,
                    url: r.try_get("url")?,
                    payload: r.try_get("payload")?,
                    attempts: r.try_get("attempts")?,
                })
            })
            .collect()
    }

    pub async fn mark_delivered(&self, id: Uuid, status_code: i32) -> Result<(), StoreError> {
        sqlx::query("UPDATE webhook_deliveries SET status = 'delivered', attempts = attempts + 1, last_status_code = $2, last_error = NULL, delivered_at = now() WHERE id = $1")
            .bind(id)
            .bind(status_code)
            .execute(&self.pipeline)
            .await?;
        Ok(())
    }

    /// Records a failed attempt. `retry_in_secs = None` gives up and parks the delivery as `dead`.
    pub async fn mark_delivery_failed(&self, id: Uuid, status_code: Option<i32>, error: &str, retry_in_secs: Option<f64>) -> Result<(), StoreError> {
        match retry_in_secs {
            Some(secs) => {
                sqlx::query("UPDATE webhook_deliveries SET attempts = attempts + 1, last_status_code = $2, last_error = $3, next_attempt_at = now() + make_interval(secs => $4) WHERE id = $1")
                    .bind(id)
                    .bind(status_code)
                    .bind(error)
                    .bind(secs)
                    .execute(&self.pipeline)
                    .await?;
            }
            None => {
                sqlx::query("UPDATE webhook_deliveries SET status = 'dead', attempts = attempts + 1, last_status_code = $2, last_error = $3 WHERE id = $1")
                    .bind(id)
                    .bind(status_code)
                    .bind(error)
                    .execute(&self.pipeline)
                    .await?;
            }
        }
        Ok(())
    }

    /// Pushes a delivery out without counting an attempt (receiver saturated or its breaker open).
    pub async fn defer_delivery(&self, id: Uuid, secs: f64) -> Result<(), StoreError> {
        sqlx::query("UPDATE webhook_deliveries SET next_attempt_at = now() + make_interval(secs => $2) WHERE id = $1 AND status = 'pending'")
            .bind(id)
            .bind(secs)
            .execute(&self.pipeline)
            .await?;
        Ok(())
    }

    /// Makes a delivery due again (admin redelivery).
    pub async fn redeliver(&self, id: Uuid) -> Result<bool, StoreError> {
        let result = sqlx::query("UPDATE webhook_deliveries SET status = 'pending', next_attempt_at = now() WHERE id = $1").bind(id).execute(&self.pipeline).await?;
        Ok(result.rows_affected() == 1)
    }

    pub async fn deliveries_for_job(&self, job_id: Uuid) -> Result<Vec<DeliveryStatus>, StoreError> {
        let rows = sqlx::query("SELECT event, sequence, status, attempts, last_status_code, delivered_at FROM webhook_deliveries WHERE job_id = $1 ORDER BY sequence, created_at")
            .bind(job_id)
            .fetch_all(&self.api)
            .await?;
        rows.iter()
            .map(|r| {
                Ok(DeliveryStatus {
                    event: r.try_get("event")?,
                    sequence: r.try_get("sequence")?,
                    status: r.try_get("status")?,
                    attempts: r.try_get("attempts")?,
                    last_status_code: r.try_get("last_status_code")?,
                    delivered_at: r.try_get("delivered_at")?,
                })
            })
            .collect()
    }
}
