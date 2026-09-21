//! Leader lease: exactly one process may drive signers at a time.
//!
//! Railway starts the new deployment and waits for it to be healthy *before* stopping the old one, so two
//! instances overlap on every deploy. The lease is a session-level Postgres advisory lock held on a
//! dedicated connection, plus a fencing epoch:
//!
//! - the standby polls `pg_try_advisory_lock` until the old leader lets go (or its session dies);
//! - on acquisition the `lease_epoch` row is bumped, and every write that can lead to a broadcast is gated
//!   on that epoch, so a zombie that lost its lock without noticing cannot bind a nonce;
//! - server-side TCP keepalives on the lease session make Postgres notice a SIGKILLed leader in seconds
//!   rather than after the OS default (~2 hours);
//! - the leader heartbeats the session; when it fails, the process stops sending and exits.

use std::{str::FromStr, time::Duration};

use sqlx::{postgres::PgConnectOptions, ConnectOptions, Connection, PgConnection};
use tokio_util::sync::CancellationToken;

use crate::{error::StoreError, telemetry};

/// Arbitrary constant identifying the gum-engine leader lock within a database.
const LOCK_KEY: i64 = 0x67756d5f6c656164; // "gum_lead"

pub struct Lease {
    pub epoch: i64,
    /// Cancelled when the lease session is lost; the process must stop sending immediately.
    pub lost: CancellationToken,
    release: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}

impl Lease {
    /// Blocks until this process becomes the leader.
    pub async fn acquire(database_url: &str, shutdown: &CancellationToken) -> Result<Option<Self>, StoreError> {
        let options = PgConnectOptions::from_str(database_url)?.application_name("gum-engine-lease").options([
            ("tcp_keepalives_idle", "5"),
            ("tcp_keepalives_interval", "2"),
            ("tcp_keepalives_count", "3"),
            ("tcp_user_timeout", "10000"),
        ]);

        let started = std::time::Instant::now();
        let mut conn: Option<PgConnection> = None;
        loop {
            if shutdown.is_cancelled() {
                return Ok(None);
            }
            if conn.is_none() {
                match options.connect().await {
                    Ok(c) => conn = Some(c),
                    Err(e) => {
                        tracing::warn!(event = "lease.connect_failed", error = %e, "cannot open lease session; retrying");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                        continue;
                    }
                }
            }
            let c = conn.as_mut().expect("lease connection present");
            match sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)").bind(LOCK_KEY).fetch_one(&mut *c).await {
                Ok(true) => break,
                Ok(false) => {
                    if started.elapsed() > Duration::from_secs(60) {
                        telemetry::alert(
                            "lease.waiting",
                            "lease.waiting",
                            "standby has waited over 60s for the leader lease; is a previous instance stuck?",
                            None,
                            None,
                            serde_json::json!({"waited_secs": started.elapsed().as_secs()}),
                        );
                    }
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_millis(500)) => {}
                        _ = shutdown.cancelled() => return Ok(None),
                    }
                }
                Err(e) => {
                    tracing::warn!(event = "lease.poll_failed", error = %e, "lease session failed while waiting; reconnecting");
                    conn = None;
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }

        let mut conn = conn.expect("lease connection present");
        // Waits for any bind transaction of a previous leader that still holds the row FOR SHARE, which
        // gives a clean ordering: those binds commit under the old epoch, nothing new can.
        let epoch: i64 = sqlx::query_scalar("UPDATE lease_epoch SET epoch = epoch + 1 WHERE id RETURNING epoch").fetch_one(&mut conn).await?;
        tracing::info!(event = "lease.acquired", epoch, waited_ms = started.elapsed().as_millis() as u64, "this instance is now the leader");

        let lost = CancellationToken::new();
        let (release, mut released) = tokio::sync::oneshot::channel::<()>();
        let lost_signal = lost.clone();
        let task = tokio::spawn(async move {
            let mut misses = 0u32;
            let mut tick = tokio::time::interval(Duration::from_millis(1_500));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tokio::select! {
                    _ = &mut released => {
                        let _ = sqlx::query("SELECT pg_advisory_unlock($1)").bind(LOCK_KEY).execute(&mut conn).await;
                        let _ = conn.close().await;
                        return;
                    }
                    _ = tick.tick() => {
                        // `pg_locks` would be a stronger check, but a live session that answers is what
                        // holds a session-level lock; the epoch gate covers the remaining window.
                        let beat = tokio::time::timeout(Duration::from_secs(3), sqlx::query("SELECT 1").execute(&mut conn)).await;
                        if matches!(beat, Ok(Ok(_))) {
                            misses = 0;
                        } else {
                            misses += 1;
                            tracing::warn!(event = "lease.heartbeat_missed", misses, "lease heartbeat failed");
                            if misses >= 2 {
                                telemetry::alert("lease.lost", "lease.lost", "leader lease session lost; stopping all sends and exiting", None, None, serde_json::json!({"epoch": epoch}));
                                lost_signal.cancel();
                                return;
                            }
                        }
                    }
                }
            }
        });

        Ok(Some(Self { epoch, lost, release, task }))
    }

    /// Hands leadership over. Called last during graceful shutdown, after in-flight work has settled.
    pub async fn release(self) {
        let _ = self.release.send(());
        let _ = tokio::time::timeout(Duration::from_secs(3), self.task).await;
        tracing::info!(event = "lease.released", "leader lease released");
    }
}
