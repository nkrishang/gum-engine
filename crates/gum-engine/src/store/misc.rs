//! Pairs, signer registry, gas ledger, terminal-state rollups, top-ups and dead letters.

use alloy::primitives::{Address, U256};
use sqlx::{Postgres, Row, Transaction};
use uuid::Uuid;

use super::{parse_addr, parse_u256, Store};
use crate::{
    domain::{addr_hex, AttemptPurpose, PairRole, Pause},
    error::StoreError,
};

pub(crate) async fn add_gas(
    tx: &mut Transaction<'_, Postgres>,
    chain_id: u64,
    signer: &str,
    purpose: AttemptPurpose,
    tx_count: i64,
    gas_used: &str,
    fee_paid: &str,
) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO gas_ledger (chain_id, signer, purpose, tx_count, gas_used, fee_paid) VALUES ($1, $2, $3, GREATEST($4, 0), GREATEST($5::numeric, 0), GREATEST($6::numeric, 0)) \
         ON CONFLICT (chain_id, signer, purpose) DO UPDATE SET tx_count = gas_ledger.tx_count + $4, \
             gas_used = GREATEST(gas_ledger.gas_used + $5::numeric, 0), fee_paid = GREATEST(gas_ledger.fee_paid + $6::numeric, 0)",
    )
    .bind(chain_id as i64)
    .bind(signer)
    .bind(purpose.as_str())
    .bind(tx_count)
    .bind(gas_used)
    .bind(fee_paid)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

pub(crate) async fn bump_rollup(tx: &mut Transaction<'_, Postgres>, chain_id: u64, signer: &str, bucket: &str) -> Result<(), StoreError> {
    sqlx::query(
        "INSERT INTO stats_rollup (chain_id, signer, bucket, count) VALUES ($1, $2, $3, 1) ON CONFLICT (chain_id, signer, bucket) DO UPDATE SET count = stats_rollup.count + 1",
    )
    .bind(chain_id as i64)
    .bind(signer)
    .bind(bucket)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

#[derive(Debug, Clone)]
pub struct PairRecord {
    pub chain_id: u64,
    pub signer: Address,
    pub nonce_hwm: Option<u64>,
    pub manual_pause: bool,
}

#[derive(Debug, Clone)]
pub struct GasRow {
    pub chain_id: u64,
    pub signer: String,
    pub purpose: String,
    pub tx_count: i64,
    pub gas_used: String,
    pub fee_paid: String,
}

#[derive(Debug, Clone)]
pub struct RollupRow {
    pub chain_id: u64,
    pub signer: String,
    pub bucket: String,
    pub count: i64,
}

#[derive(Debug, Clone)]
pub struct TopupRecord {
    pub id: Uuid,
    pub chain_id: u64,
    pub signer: Address,
    pub amount: U256,
    pub status: String,
}

impl Store {
    pub async fn register_signer(&self, address: &Address, key_ref: &str, role: PairRole) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO signers (address, key_ref, role) VALUES ($1, $2, $3) ON CONFLICT (address) DO UPDATE SET key_ref = EXCLUDED.key_ref")
            .bind(addr_hex(address))
            .bind(key_ref)
            .bind(role.as_str())
            .execute(&self.pipeline)
            .await?;
        Ok(())
    }

    /// Creates the pair row if it is new and returns its durable state.
    pub async fn ensure_pair(&self, chain_id: u64, signer: &Address, role: PairRole) -> Result<PairRecord, StoreError> {
        let row = sqlx::query(
            "INSERT INTO pairs (chain_id, signer, role) VALUES ($1, $2, $3) \
             ON CONFLICT (chain_id, signer) DO UPDATE SET role = EXCLUDED.role RETURNING nonce_hwm, manual_pause",
        )
        .bind(chain_id as i64)
        .bind(addr_hex(signer))
        .bind(role.as_str())
        .fetch_one(&self.pipeline)
        .await?;
        Ok(PairRecord { chain_id, signer: *signer, nonce_hwm: row.try_get::<Option<i64>, _>("nonce_hwm")?.map(|n| n as u64), manual_pause: row.try_get("manual_pause")? })
    }

    /// Signers that still have live attempts on a chain, so a signer removed from config can be flagged
    /// instead of silently abandoning its in-flight transactions.
    pub async fn signers_with_live_attempts(&self, chain_id: u64) -> Result<Vec<Address>, StoreError> {
        let rows: Vec<String> = sqlx::query_scalar("SELECT DISTINCT signer FROM tx_attempts WHERE chain_id = $1 AND status IN ('broadcast', 'included')")
            .bind(chain_id as i64)
            .fetch_all(&self.pipeline)
            .await?;
        rows.iter().map(|s| parse_addr(s)).collect()
    }

    /// Mirrors a pair's pause state for observability and so manual pauses survive restarts.
    pub async fn record_pause(&self, chain_id: u64, signer: &Address, pause: Option<&Pause>, manual: bool) -> Result<(), StoreError> {
        sqlx::query(
            "UPDATE pairs SET pause_reason = $3, recovery_step = $4, pause_detail = $5, paused_at = $6, manual_pause = $7, updated_at = now() WHERE chain_id = $1 AND signer = $2",
        )
        .bind(chain_id as i64)
        .bind(addr_hex(signer))
        .bind(pause.map(|p| p.reason.as_str()))
        .bind(pause.map(|p| p.recovery_step.as_str()))
        .bind(pause.map(|p| p.detail.as_str()))
        .bind(pause.map(|p| p.since))
        .bind(manual)
        .execute(&self.pipeline)
        .await?;
        Ok(())
    }

    pub async fn gas_ledger(&self) -> Result<Vec<GasRow>, StoreError> {
        let rows =
            sqlx::query("SELECT chain_id, signer, purpose, tx_count, gas_used::text AS gas_used, fee_paid::text AS fee_paid FROM gas_ledger ORDER BY chain_id, signer, purpose")
                .fetch_all(&self.api)
                .await?;
        rows.iter()
            .map(|r| {
                Ok(GasRow {
                    chain_id: r.try_get::<i64, _>("chain_id")? as u64,
                    signer: r.try_get("signer")?,
                    purpose: r.try_get("purpose")?,
                    tx_count: r.try_get("tx_count")?,
                    gas_used: r.try_get("gas_used")?,
                    fee_paid: r.try_get("fee_paid")?,
                })
            })
            .collect()
    }

    pub async fn rollups(&self) -> Result<Vec<RollupRow>, StoreError> {
        let rows = sqlx::query("SELECT chain_id, signer, bucket, count FROM stats_rollup").fetch_all(&self.api).await?;
        rows.iter()
            .map(|r| Ok(RollupRow { chain_id: r.try_get::<i64, _>("chain_id")? as u64, signer: r.try_get("signer")?, bucket: r.try_get("bucket")?, count: r.try_get("count")? }))
            .collect()
    }

    pub async fn create_topup(&self, chain_id: u64, treasury: &Address, signer: &Address, amount: U256) -> Result<Uuid, StoreError> {
        let id = Uuid::now_v7();
        sqlx::query("INSERT INTO topups (id, chain_id, treasury, signer, amount, status) VALUES ($1, $2, $3, $4, $5::numeric, 'requested')")
            .bind(id)
            .bind(chain_id as i64)
            .bind(addr_hex(treasury))
            .bind(addr_hex(signer))
            .bind(amount.to_string())
            .execute(&self.pipeline)
            .await?;
        Ok(id)
    }

    /// A top-up that never got a transaction (e.g. treasury could not cover it).
    pub async fn fail_topup(&self, id: Uuid, error: &str) -> Result<(), StoreError> {
        sqlx::query("UPDATE topups SET status = 'failed', error = $2, updated_at = now() WHERE id = $1 AND status = 'requested'")
            .bind(id)
            .bind(error)
            .execute(&self.pipeline)
            .await?;
        Ok(())
    }

    /// Top-ups that were requested but never bound before a restart; they are re-evaluated from scratch.
    pub async fn fail_unbound_topups(&self, chain_id: u64) -> Result<u64, StoreError> {
        let result = sqlx::query(
            "UPDATE topups SET status = 'failed', error = 'engine restarted before the top-up was sent', updated_at = now() WHERE chain_id = $1 AND status = 'requested'",
        )
        .bind(chain_id as i64)
        .execute(&self.pipeline)
        .await?;
        Ok(result.rows_affected())
    }

    pub async fn get_topup(&self, id: Uuid) -> Result<Option<TopupRecord>, StoreError> {
        let row = sqlx::query("SELECT id, chain_id, signer, amount::text AS amount, status FROM topups WHERE id = $1").bind(id).fetch_optional(&self.pipeline).await?;
        row.map(|r| {
            Ok(TopupRecord {
                id: r.try_get("id")?,
                chain_id: r.try_get::<i64, _>("chain_id")? as u64,
                signer: parse_addr(&r.try_get::<String, _>("signer")?)?,
                amount: parse_u256(&r.try_get::<String, _>("amount")?)?,
                status: r.try_get("status")?,
            })
        })
        .transpose()
    }

    /// Top-ups sent to `signer` within the last hour; a guard against a draining loop.
    pub async fn recent_topups(&self, chain_id: u64, signer: &Address) -> Result<u32, StoreError> {
        let n: i64 = sqlx::query_scalar("SELECT count(*) FROM topups WHERE chain_id = $1 AND signer = $2 AND status <> 'failed' AND created_at > now() - interval '1 hour'")
            .bind(chain_id as i64)
            .bind(addr_hex(signer))
            .fetch_one(&self.pipeline)
            .await?;
        Ok(n as u32)
    }

    /// Parks a write that can never succeed so it is visible to operators instead of being retried forever.
    pub async fn dead_letter(&self, kind: &str, payload: serde_json::Value, error: &str) -> Result<(), StoreError> {
        sqlx::query("INSERT INTO dead_letters (id, kind, payload, error) VALUES ($1, $2, $3, $4)")
            .bind(Uuid::now_v7())
            .bind(kind)
            .bind(payload)
            .bind(error)
            .execute(&self.pipeline)
            .await?;
        Ok(())
    }
}
