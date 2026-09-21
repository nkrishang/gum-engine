//! Logging, metrics and alerting.
//!
//! Logging policy (Railway drops everything above 500 lines/s, so every line has to earn its place):
//! - one `info` line per job when it reaches a terminal state, carrying the full timing breakdown;
//! - intermediate steps are `debug`; handled anomalies are `warn`; failures needing attention are `error`;
//! - every error is logged exactly once, by the layer that handles it;
//! - anything that can repeat in a tight loop goes through [`throttled`], which collapses repeats into
//!   a single line with a `suppressed` count;
//! - `alert = true` marks the lines an operator must see; they are also forwarded to the alert sink.
//!
//! Every event carries a stable `event` code (e.g. `job.confirmed`, `pair.paused`) plus the identifiers
//! that apply (`chain`, `signer`, `job_id`, `nonce`, `tx_hash`), so Railway's `@field:value` search works.

use std::{
    collections::HashMap,
    sync::OnceLock,
    time::{Duration, Instant},
};

use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use parking_lot::Mutex;
use tokio::sync::mpsc;
use tracing_subscriber::{fmt, EnvFilter};

pub fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,sqlx=warn,hyper=warn,reqwest=warn"));
    let pretty = std::env::var("GUM_LOG_FORMAT").is_ok_and(|v| v == "pretty");
    if pretty {
        fmt().with_env_filter(filter).with_target(false).init();
    } else {
        // `flatten_event` lifts fields to the top level so Railway exposes them as `@attributes`.
        fmt().json().flatten_event(true).with_current_span(false).with_span_list(false).with_target(false).with_env_filter(filter).init();
    }
}

pub fn init_metrics() -> anyhow::Result<PrometheusHandle> {
    PrometheusBuilder::new().install_recorder().map_err(|e| anyhow::anyhow!("failed to install metrics recorder: {e}"))
}

struct ThrottleEntry {
    last_emit: Instant,
    suppressed: u64,
}

static THROTTLE: OnceLock<Mutex<HashMap<String, ThrottleEntry>>> = OnceLock::new();

/// Returns `Some(suppressed_since_last_emit)` when a line for `key` may be emitted now, `None` when it
/// must be swallowed. Callers log only in the `Some` case and include the count.
pub fn throttled(key: &str, every: Duration) -> Option<u64> {
    let map = THROTTLE.get_or_init(|| Mutex::new(HashMap::new()));
    let mut map = map.lock();
    let now = Instant::now();
    match map.get_mut(key) {
        Some(entry) if now.duration_since(entry.last_emit) < every => {
            entry.suppressed += 1;
            None
        }
        Some(entry) => {
            let suppressed = entry.suppressed;
            entry.last_emit = now;
            entry.suppressed = 0;
            Some(suppressed)
        }
        None => {
            // Bound the map: keys embed chain/signer identifiers, never job ids, so this stays small.
            if map.len() > 4096 {
                map.retain(|_, e| now.duration_since(e.last_emit) < Duration::from_secs(600));
            }
            map.insert(key.to_string(), ThrottleEntry { last_emit: now, suppressed: 0 });
            Some(0)
        }
    }
}

/// An operator-worthy event, forwarded to `alerts.webhook_url` when configured.
#[derive(Debug, Clone, serde::Serialize)]
pub struct Alert {
    pub event: String,
    pub message: String,
    pub chain_id: Option<u64>,
    pub signer: Option<String>,
    pub detail: serde_json::Value,
    pub at: chrono::DateTime<chrono::Utc>,
}

static ALERT_TX: OnceLock<mpsc::Sender<Alert>> = OnceLock::new();

/// Installs the alert forwarder. Without a URL, alerts are log lines only.
pub fn init_alert_sink(url: Option<String>, http: reqwest::Client) {
    let Some(url) = url else { return };
    let (tx, mut rx) = mpsc::channel::<Alert>(256);
    if ALERT_TX.set(tx).is_err() {
        return;
    }
    tokio::spawn(async move {
        while let Some(alert) = rx.recv().await {
            let result = http.post(&url).timeout(Duration::from_secs(5)).json(&alert).send().await;
            if let Err(e) = result {
                if let Some(suppressed) = throttled("alert_sink.failed", Duration::from_secs(60)) {
                    tracing::warn!(event = "alert_sink.failed", error = %e.without_url(), suppressed, "alert webhook delivery failed");
                }
            }
        }
    });
}

/// Logs an operator-worthy error (`alert = true`) and forwards it to the alert sink. Throttled per `key`
/// so a persistent condition alerts once a minute rather than once per occurrence.
pub fn alert(key: &str, event: &str, message: &str, chain_id: Option<u64>, signer: Option<&str>, detail: serde_json::Value) {
    let Some(suppressed) = throttled(&format!("alert:{key}"), Duration::from_secs(60)) else { return };
    tracing::error!(
        alert = true,
        event,
        chain = chain_id,
        signer,
        suppressed,
        detail = %detail,
        "{message}"
    );
    metrics::counter!("gum_alerts_total", "event" => event.to_string()).increment(1);
    if let Some(tx) = ALERT_TX.get() {
        let alert = Alert { event: event.to_string(), message: message.to_string(), chain_id, signer: signer.map(str::to_string), detail, at: chrono::Utc::now() };
        // A full channel means the sink is down; the log line above is the durable record.
        let _ = tx.try_send(alert);
    }
}
