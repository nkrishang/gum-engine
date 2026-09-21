//! Postgres persistence. Postgres is the durable source of truth; everything in process memory is a
//! cache that can be rebuilt from here.
//!
//! Three pools keep workloads from starving each other: `pipeline` (binds and settlements — the writes a
//! broadcast waits on), `api` (reads, with a statement timeout so an analytics query can never hold a
//! connection for long) and a dedicated session for the leader lease (see `lease.rs`).

pub mod attempts;
pub mod batcher;
pub mod jobs;
pub mod lease;
pub mod misc;
pub mod outbox;

use std::{str::FromStr, time::Duration};

use alloy::primitives::{Address, B256, U256};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};

use crate::{config::DatabaseConfig, error::StoreError};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

#[derive(Clone)]
pub struct Store {
    pub(crate) pipeline: PgPool,
    pub(crate) api: PgPool,
}

impl Store {
    pub async fn connect(cfg: &DatabaseConfig) -> Result<Self, StoreError> {
        let base = PgConnectOptions::from_str(&cfg.url)?.application_name("gum-engine");
        let pipeline = PgPoolOptions::new().max_connections(cfg.pipeline_pool_size).min_connections(2).acquire_timeout(Duration::from_secs(5)).connect_with(base.clone()).await?;
        let api_opts = base.options([("statement_timeout", cfg.api_statement_timeout_ms.to_string())]);
        let api = PgPoolOptions::new().max_connections(cfg.api_pool_size).acquire_timeout(Duration::from_secs(3)).connect_with(api_opts).await?;
        Ok(Self { pipeline, api })
    }

    /// Connects with retries: on Railway the private network and the database may come up after us.
    pub async fn connect_with_retry(cfg: &DatabaseConfig, max_wait: Duration) -> Result<Self, StoreError> {
        let started = std::time::Instant::now();
        let mut delay = Duration::from_millis(250);
        loop {
            match Self::connect(cfg).await {
                Ok(store) => return Ok(store),
                // Wrong credentials or a missing database will not fix themselves: fail at once.
                Err(StoreError::Db(e)) if e.as_database_error().and_then(|d| d.code().map(|c| c.starts_with("28") || c.starts_with("3D"))).unwrap_or(false) => {
                    return Err(StoreError::Db(e));
                }
                Err(e) if started.elapsed() < max_wait => {
                    tracing::warn!(event = "store.connect_retry", error = %e, retry_in_ms = delay.as_millis() as u64, "database not reachable yet");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(5));
                }
                Err(e) => return Err(e),
            }
        }
    }

    pub async fn migrate(&self) -> Result<(), StoreError> {
        MIGRATOR.run(&self.pipeline).await.map_err(|e| StoreError::Corrupt(format!("migration failed: {e}")))
    }

    pub async fn ping(&self) -> bool {
        tokio::time::timeout(Duration::from_secs(2), sqlx::query("SELECT 1").execute(&self.api)).await.is_ok_and(|r| r.is_ok())
    }

    pub async fn close(&self) {
        self.pipeline.close().await;
        self.api.close().await;
    }
}

// ---- value conversions shared by the store modules ------------------------------------------------

pub(crate) fn parse_addr(s: &str) -> Result<Address, StoreError> {
    Address::from_str(s).map_err(|e| StoreError::Corrupt(format!("address `{s}`: {e}")))
}

pub(crate) fn parse_hash(s: &str) -> Result<B256, StoreError> {
    B256::from_str(s).map_err(|e| StoreError::Corrupt(format!("hash `{s}`: {e}")))
}

pub(crate) fn parse_u256(s: &str) -> Result<U256, StoreError> {
    U256::from_str_radix(s, 10).map_err(|e| StoreError::Corrupt(format!("numeric `{s}`: {e}")))
}

pub(crate) fn parse_u128(s: &str) -> Result<u128, StoreError> {
    s.parse::<u128>().map_err(|e| StoreError::Corrupt(format!("numeric `{s}`: {e}")))
}
