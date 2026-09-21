//! Webhook delivery, fully decoupled from the transaction pipeline.
//!
//! The pipeline's only involvement is an outbox row written in the same database transaction as the
//! state change it announces. Everything here works off that table: a claim loop leases due rows, a
//! bounded pool delivers them, and failures are retried with exponential backoff until `max_attempts`,
//! after which the delivery is parked as `dead` (and can be redelivered by an operator). Delivery is
//! at-least-once; receivers dedupe on `event_id`.
//!
//! The API has no authentication and lives on a private network, so the URL a caller supplies is an SSRF
//! vector. Redirects are never followed, the response body is never read, the engine's own address is
//! refused, and an optional host allowlist narrows it further.

use std::{
    collections::HashMap,
    net::IpAddr,
    sync::Arc,
    time::{Duration, Instant},
};

use hmac::{Hmac, KeyInit, Mac};
use parking_lot::Mutex;
use rand::Rng;
use sha2::Sha256;
use tokio::sync::Semaphore;

use crate::{config::WebhookConfig, engine::Engine, store::outbox::Delivery, telemetry};

/// `t=<unix seconds>,v1=<hex hmac_sha256(secret, "<t>.<body>")>`. Signing the timestamp with the body
/// lets receivers reject replays.
pub fn signature_header(secret: &str, timestamp: i64, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts keys of any length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("t={timestamp},v1={}", hex::encode(mac.finalize().into_bytes()))
}

/// Validates a caller-supplied webhook URL at request time.
pub fn validate_url(raw: &str, cfg: &WebhookConfig, own_port: u16) -> Result<(), String> {
    let url = url::Url::parse(raw).map_err(|e| format!("webhook.url is not a valid URL: {e}"))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("webhook.url must use http or https".into());
    }
    if !url.username().is_empty() || url.password().is_some() {
        return Err("webhook.url must not contain credentials".into());
    }
    let host = url.host_str().ok_or("webhook.url has no host")?.trim_start_matches('[').trim_end_matches(']').to_lowercase();
    if !cfg.host_allowlist.is_empty() && !cfg.host_allowlist.iter().any(|h| h.eq_ignore_ascii_case(&host)) {
        return Err(format!("webhook host `{host}` is not in webhook.host_allowlist"));
    }
    let port = url.port_or_known_default().unwrap_or(80);
    let ip = host.parse::<IpAddr>().ok();
    let loopback = ip.is_some_and(|ip| ip.is_loopback() || ip.is_unspecified()) || host == "localhost";
    // Never let a job make the engine call its own (unauthenticated) admin API.
    let own_names = [std::env::var("RAILWAY_PRIVATE_DOMAIN").ok(), std::env::var("HOSTNAME").ok()];
    let is_self = port == own_port && (loopback || own_names.iter().flatten().any(|n| n.eq_ignore_ascii_case(&host)));
    if is_self {
        return Err("webhook.url points at gum-engine itself".into());
    }
    if !cfg.allow_private_hosts {
        let private = match ip {
            Some(IpAddr::V4(v4)) => v4.is_private() || v4.is_loopback() || v4.is_link_local() || v4.is_unspecified(),
            Some(IpAddr::V6(v6)) => v6.is_loopback() || v6.is_unspecified() || (v6.segments()[0] & 0xfe00) == 0xfc00 || (v6.segments()[0] & 0xffc0) == 0xfe80,
            None => host == "localhost" || host.ends_with(".internal") || host.ends_with(".local"),
        };
        if private {
            return Err("webhook.url points at a private address and webhook.allow_private_hosts is false".into());
        }
    }
    Ok(())
}

/// Per-host state: a concurrency cap so one slow receiver cannot occupy the whole pool, and a breaker so
/// a dead one is not hammered.
#[derive(Default)]
struct HostState {
    in_flight: usize,
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

struct Hosts {
    inner: Mutex<HashMap<String, HostState>>,
    max_per_host: usize,
}

impl Hosts {
    /// Reserves a slot for `host`, or says how long to wait before trying again.
    fn try_acquire(&self, host: &str) -> Result<(), Duration> {
        let mut map = self.inner.lock();
        let state = map.entry(host.to_string()).or_default();
        if let Some(until) = state.open_until {
            if Instant::now() < until {
                return Err(until - Instant::now());
            }
            state.open_until = None; // half-open: let one through
        }
        if state.in_flight >= self.max_per_host {
            return Err(Duration::from_millis(500));
        }
        state.in_flight += 1;
        Ok(())
    }

    fn release(&self, host: &str, success: bool) {
        let mut map = self.inner.lock();
        let Some(state) = map.get_mut(host) else { return };
        state.in_flight = state.in_flight.saturating_sub(1);
        if success {
            state.consecutive_failures = 0;
        } else {
            state.consecutive_failures += 1;
            if state.consecutive_failures >= 5 {
                let secs = (5u64 << (state.consecutive_failures - 5).min(5)).min(120);
                state.open_until = Some(Instant::now() + Duration::from_secs(secs));
            }
        }
        if map.len() > 10_000 {
            map.retain(|_, s| s.in_flight > 0 || s.open_until.is_some());
        }
    }
}

pub async fn run(engine: Arc<Engine>) {
    let cfg = engine.cfg.webhook.clone();
    let permits = Arc::new(Semaphore::new(cfg.max_concurrency.max(1)));
    let hosts = Arc::new(Hosts { inner: Mutex::new(HashMap::new()), max_per_host: cfg.max_per_host.max(1) });
    // A claimed row is leased for a little longer than a delivery can take; a crash lets the lease lapse.
    let lease_secs = (cfg.timeout_ms / 1_000 + 30) as i64;
    let mut idle = Duration::from_millis(100);

    loop {
        if engine.shutdown.is_cancelled() {
            return;
        }
        let free = permits.available_permits();
        if free == 0 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            continue;
        }
        let claimed = match engine.store.claim_due_deliveries(free as i64, lease_secs).await {
            Ok(rows) => rows,
            Err(e) => {
                if let Some(suppressed) = telemetry::throttled("webhook.claim", Duration::from_secs(15)) {
                    tracing::warn!(event = "webhook.claim_failed", code = e.code(), error = %e, suppressed, "cannot claim webhook deliveries");
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
        };
        if claimed.is_empty() {
            // Woken the moment a settlement commits; the timer only covers retries coming due and rows
            // written by another instance.
            tokio::select! {
                _ = engine.webhook_wake.notified() => {}
                _ = tokio::time::sleep(idle) => {}
                _ = engine.shutdown.cancelled() => return,
            }
            idle = (idle * 2).min(Duration::from_secs(1));
            continue;
        }
        idle = Duration::from_millis(50);

        for delivery in claimed {
            let permit = match permits.clone().acquire_owned().await {
                Ok(p) => p,
                Err(_) => return,
            };
            let (engine, hosts, cfg) = (engine.clone(), hosts.clone(), cfg.clone());
            tokio::spawn(async move {
                deliver(&engine, &hosts, &cfg, delivery).await;
                drop(permit);
            });
        }
    }
}

async fn deliver(engine: &Engine, hosts: &Hosts, cfg: &WebhookConfig, d: Delivery) {
    let host = url::Url::parse(&d.url).ok().and_then(|u| u.host_str().map(|h| format!("{h}:{}", u.port_or_known_default().unwrap_or(0)))).unwrap_or_default();

    if let Err(wait) = hosts.try_acquire(&host) {
        // Not an attempt: the receiver is saturated or its breaker is open. Come back later.
        let _ = engine.store.defer_delivery(d.id, wait.as_secs_f64().max(0.5)).await;
        return;
    }

    let body = d.payload.to_string();
    let timestamp = chrono::Utc::now().timestamp();
    let started = Instant::now();
    let result = engine
        .http
        .post(&d.url)
        .timeout(Duration::from_millis(cfg.timeout_ms))
        .header("content-type", "application/json")
        .header("user-agent", "gum-engine-webhook/1")
        .header("x-gum-event-id", d.id.to_string())
        .header("x-gum-job-id", d.job_id.to_string())
        .header("x-gum-signature", signature_header(&cfg.signing_secret, timestamp, body.as_bytes()))
        .body(body)
        .send()
        .await;
    metrics::histogram!("gum_webhook_latency_seconds").record(started.elapsed().as_secs_f64());

    let (ok, status, error) = match result {
        Ok(resp) if resp.status().is_success() => (true, Some(resp.status().as_u16() as i32), String::new()),
        Ok(resp) => (false, Some(resp.status().as_u16() as i32), format!("receiver answered {}", resp.status())),
        Err(e) => (false, None, e.without_url().to_string()),
    };
    hosts.release(&host, ok);

    let write = if ok {
        metrics::counter!("gum_webhooks_delivered_total").increment(1);
        let written = engine.store.mark_delivered(d.id, status.unwrap_or(200)).await;
        // The job's next event (if any) just became deliverable.
        engine.webhook_wake.notify_one();
        written
    } else {
        metrics::counter!("gum_webhook_failures_total").increment(1);
        let attempt = d.attempts as u32 + 1;
        let retry_in = (attempt < cfg.max_attempts).then(|| {
            let exp = cfg.backoff_base_ms.saturating_mul(1u64 << attempt.min(20)).min(cfg.backoff_max_ms);
            (exp / 2 + rand::rng().random_range(0..=exp / 2)) as f64 / 1_000.0
        });
        if retry_in.is_none() {
            tracing::warn!(event = "webhook.dead", job_id = %d.job_id, event_id = %d.id, webhook_event = %d.event, attempts = attempt, last_error = %error, "webhook delivery abandoned after max attempts");
        } else if let Some(suppressed) = telemetry::throttled(&format!("webhook.failed:{host}"), Duration::from_secs(30)) {
            tracing::warn!(event = "webhook.failed", host = %host, job_id = %d.job_id, attempt, error = %error, suppressed, "webhook delivery failed; will retry");
        }
        engine.store.mark_delivery_failed(d.id, status, &error, retry_in).await
    };
    if let Err(e) = write {
        // The lease will lapse and the delivery will be attempted again: at-least-once, never lost.
        if let Some(suppressed) = telemetry::throttled("webhook.record", Duration::from_secs(15)) {
            tracing::warn!(event = "webhook.record_failed", code = e.code(), error = %e, suppressed, "could not record webhook delivery result");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(allow_private: bool, allowlist: &[&str]) -> WebhookConfig {
        WebhookConfig {
            signing_secret: "s".into(),
            allow_private_hosts: allow_private,
            host_allowlist: allowlist.iter().map(|s| s.to_string()).collect(),
            max_concurrency: 4,
            max_per_host: 2,
            timeout_ms: 1_000,
            max_attempts: 3,
            backoff_base_ms: 100,
            backoff_max_ms: 1_000,
        }
    }

    #[test]
    fn signature_matches_reference_construction() {
        let header = signature_header("secret", 1_700_000_000, br#"{"a":1}"#);
        let mut mac = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
        mac.update(br#"1700000000.{"a":1}"#);
        assert_eq!(header, format!("t=1700000000,v1={}", hex::encode(mac.finalize().into_bytes())));
    }

    #[test]
    fn refuses_to_call_itself() {
        assert!(validate_url("http://127.0.0.1:8080/v1/admin/chains/1/pause", &cfg(true, &[]), 8080).is_err());
        assert!(validate_url("http://localhost:8080/x", &cfg(true, &[]), 8080).is_err());
        assert!(validate_url("http://[::1]:8080/x", &cfg(true, &[]), 8080).is_err());
        assert!(validate_url("http://127.0.0.1:9000/hook", &cfg(true, &[]), 8080).is_ok());
    }

    #[test]
    fn private_hosts_and_allowlist() {
        assert!(validate_url("http://10.0.0.5/hook", &cfg(false, &[]), 8080).is_err());
        assert!(validate_url("http://service.railway.internal/hook", &cfg(false, &[]), 8080).is_err());
        assert!(validate_url("https://example.com/hook", &cfg(false, &[]), 8080).is_ok());
        assert!(validate_url("https://example.com/hook", &cfg(true, &["hooks.example.com"]), 8080).is_err());
        assert!(validate_url("https://hooks.example.com/hook", &cfg(true, &["hooks.example.com"]), 8080).is_ok());
        assert!(validate_url("ftp://example.com/hook", &cfg(true, &[]), 8080).is_err());
        assert!(validate_url("https://user:pw@example.com/hook", &cfg(true, &[]), 8080).is_err());
    }

    #[test]
    fn breaker_opens_after_repeated_failures() {
        let hosts = Hosts { inner: Mutex::new(HashMap::new()), max_per_host: 2 };
        for _ in 0..5 {
            hosts.try_acquire("h:80").unwrap();
            hosts.release("h:80", false);
        }
        assert!(hosts.try_acquire("h:80").is_err(), "breaker should be open");
        assert!(hosts.try_acquire("other:80").is_ok(), "other hosts are unaffected");
    }
}
