//! Webhook receiver. Records every delivery with its (monotonic) arrival time, verifies
//! `X-Gum-Signature`, and can behave badly on purpose: `/ok`, `/slow?ms=`, `/fail`, `/flaky?p=`.
//! A "dead" receiver is a URL pointing at a closed port.
//!
//! Each hostile mode gets its own listener (port) so an engine-side per-host circuit breaker tripping on
//! the `/fail` receiver cannot be confused with the healthy one.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use hmac::{Hmac, KeyInit, Mac};
use parking_lot::Mutex;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::Sha256;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SinkMode {
    Ok,
    Slow,
    Fail,
    Flaky,
    Dead,
}

impl SinkMode {
    pub const ALL: [SinkMode; 5] = [
        SinkMode::Ok,
        SinkMode::Slow,
        SinkMode::Fail,
        SinkMode::Flaky,
        SinkMode::Dead,
    ];
    pub fn label(&self) -> &'static str {
        match self {
            SinkMode::Ok => "ok",
            SinkMode::Slow => "slow",
            SinkMode::Fail => "fail",
            SinkMode::Flaky => "flaky",
            SinkMode::Dead => "dead",
        }
    }
    /// Whether the verdict may demand that deliveries *arrive* for jobs using this receiver.
    /// `fail` never acknowledges (an engine-side breaker may legitimately stop trying) and `dead` cannot
    /// receive anything.
    pub fn coverage_required(&self) -> bool {
        matches!(self, SinkMode::Ok | SinkMode::Slow | SinkMode::Flaky)
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct Delivery {
    /// Monotonic arrival instant (before any artificial delay of the hostile modes).
    #[serde(skip)]
    pub arrived: Instant,
    pub mode: SinkMode,
    pub responded: u16,
    pub signature_valid: bool,
    pub signature_error: Option<String>,
    /// `now - t` from the signature header, seconds.
    pub signature_age_s: Option<i64>,
    pub event_id: String,
    pub event: String,
    pub sequence: u64,
    pub job_id: String,
    pub status: String,
    pub outcome: Option<String>,
    pub tx_hash: Option<String>,
    pub header_event_id: Option<String>,
    pub header_job_id: Option<String>,
    /// The body failed to parse as a contract-shaped event.
    pub malformed: Option<String>,
}

#[derive(Deserialize)]
struct EventBody {
    event_id: String,
    event: String,
    sequence: u64,
    job_id: String,
    #[serde(default)]
    status: String,
    #[serde(default)]
    outcome: Option<String>,
    #[serde(default)]
    tx_hash: Option<String>,
}

struct Inner {
    secret: Option<Vec<u8>>,
    print: bool,
    deliveries: Mutex<Vec<Delivery>>,
}

#[derive(Clone)]
pub struct Sink {
    inner: Arc<Inner>,
    addrs: HashMap<SinkMode, SocketAddr>,
    dead_port: u16,
}

#[derive(Deserialize, Default)]
struct ModeQuery {
    ms: Option<u64>,
    p: Option<f64>,
}

impl Sink {
    /// Start the sink. With `port = Some(p)` a single listener serves every path (standalone dev mode);
    /// otherwise each mode gets its own ephemeral port.
    pub async fn start(secret: Option<&str>, port: Option<u16>, print: bool) -> Result<Sink> {
        let inner = Arc::new(Inner {
            secret: secret.map(|s| s.as_bytes().to_vec()),
            print,
            deliveries: Mutex::new(Vec::new()),
        });
        let mut addrs = HashMap::new();
        match port {
            Some(p) => {
                let addr = serve(inner.clone(), p).await?;
                for m in SinkMode::ALL {
                    addrs.insert(m, addr);
                }
            }
            None => {
                for m in [
                    SinkMode::Ok,
                    SinkMode::Slow,
                    SinkMode::Fail,
                    SinkMode::Flaky,
                ] {
                    addrs.insert(m, serve(inner.clone(), 0).await?);
                }
            }
        }
        let dead_port = crate::util::free_port()?;
        Ok(Sink {
            inner,
            addrs,
            dead_port,
        })
    }

    pub fn addr(&self, mode: SinkMode) -> Option<SocketAddr> {
        self.addrs.get(&mode).copied()
    }

    /// URL to hand to the engine for a job that should be delivered to a receiver in `mode`.
    pub fn url(&self, mode: SinkMode, slow_ms: u64, flaky_p: f64) -> String {
        match mode {
            SinkMode::Dead => format!("http://127.0.0.1:{}/dead", self.dead_port),
            SinkMode::Ok => format!("http://{}/ok", self.addrs[&mode]),
            SinkMode::Slow => format!("http://{}/slow?ms={slow_ms}", self.addrs[&mode]),
            SinkMode::Fail => format!("http://{}/fail", self.addrs[&mode]),
            SinkMode::Flaky => format!("http://{}/flaky?p={flaky_p}", self.addrs[&mode]),
        }
    }

    pub fn deliveries(&self) -> Vec<Delivery> {
        self.inner.deliveries.lock().clone()
    }
}

async fn serve(inner: Arc<Inner>, port: u16) -> Result<SocketAddr> {
    let app = Router::new()
        .route("/ok", post(h_ok))
        .route("/slow", post(h_slow))
        .route("/fail", post(h_fail))
        .route("/flaky", post(h_flaky))
        .with_state(inner);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("binding the webhook sink on port {port}"))?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            eprintln!("[sink] server error: {e}");
        }
    });
    Ok(addr)
}

pub fn sign(secret: &[u8], t: i64, body: &[u8]) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(t.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    hex::encode(mac.finalize().into_bytes())
}

/// Parse `t=<unix>,v1=<hex>` and verify the HMAC. Returns the timestamp on success.
pub fn verify_signature(secret: &[u8], header: Option<&str>, body: &[u8]) -> Result<i64, String> {
    let header = header.ok_or("missing X-Gum-Signature header")?;
    let mut t: Option<i64> = None;
    let mut v1: Option<&str> = None;
    for part in header.split(',') {
        match part.trim().split_once('=') {
            Some(("t", v)) => t = v.parse().ok(),
            Some(("v1", v)) => v1 = Some(v),
            _ => {}
        }
    }
    let t = t.ok_or_else(|| format!("no valid t= in signature header {header:?}"))?;
    let v1 = v1.ok_or_else(|| format!("no v1= in signature header {header:?}"))?;
    let given = hex::decode(v1).map_err(|_| "v1 is not hex".to_string())?;
    let mut mac = Hmac::<Sha256>::new_from_slice(secret).expect("hmac accepts any key length");
    mac.update(t.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    mac.verify_slice(&given)
        .map_err(|_| "HMAC mismatch".to_string())?;
    Ok(t)
}

fn record(
    inner: &Inner,
    mode: SinkMode,
    responded: u16,
    headers: &HeaderMap,
    body: &[u8],
    arrived: Instant,
) {
    let hdr = |n: &str| {
        headers
            .get(n)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let (signature_valid, signature_error, signature_age_s) = match &inner.secret {
        None => (true, None, None),
        Some(secret) => match verify_signature(secret, hdr("x-gum-signature").as_deref(), body) {
            Ok(t) => (true, None, Some(chrono::Utc::now().timestamp() - t)),
            Err(e) => (false, Some(e), None),
        },
    };
    let mut d = Delivery {
        arrived,
        mode,
        responded,
        signature_valid,
        signature_error,
        signature_age_s,
        event_id: String::new(),
        event: String::new(),
        sequence: 0,
        job_id: String::new(),
        status: String::new(),
        outcome: None,
        tx_hash: None,
        header_event_id: hdr("x-gum-event-id"),
        header_job_id: hdr("x-gum-job-id"),
        malformed: None,
    };
    match serde_json::from_slice::<EventBody>(body) {
        Ok(b) => {
            d.event_id = b.event_id;
            d.event = b.event;
            d.sequence = b.sequence;
            d.job_id = b.job_id;
            d.status = b.status;
            d.outcome = b.outcome;
            d.tx_hash = b.tx_hash;
        }
        Err(e) => {
            d.malformed = Some(format!(
                "{e}: {}",
                crate::api::truncate(&String::from_utf8_lossy(body), 200)
            ));
            d.job_id = d.header_job_id.clone().unwrap_or_default();
        }
    }
    if inner.print {
        let raw: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
        println!(
            "{}",
            serde_json::json!({
                "received_at": chrono::Utc::now().to_rfc3339(),
                "path": mode.label(),
                "responded": responded,
                "signature_valid": d.signature_valid,
                "signature_error": d.signature_error,
                "body": raw,
            })
        );
    }
    inner.deliveries.lock().push(d);
}

async fn h_ok(State(inner): State<Arc<Inner>>, headers: HeaderMap, body: Bytes) -> StatusCode {
    record(&inner, SinkMode::Ok, 200, &headers, &body, Instant::now());
    StatusCode::OK
}

async fn h_slow(
    State(inner): State<Arc<Inner>>,
    Query(q): Query<ModeQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    record(&inner, SinkMode::Slow, 200, &headers, &body, Instant::now());
    tokio::time::sleep(Duration::from_millis(q.ms.unwrap_or(1000))).await;
    StatusCode::OK
}

async fn h_fail(State(inner): State<Arc<Inner>>, headers: HeaderMap, body: Bytes) -> StatusCode {
    record(&inner, SinkMode::Fail, 500, &headers, &body, Instant::now());
    StatusCode::INTERNAL_SERVER_ERROR
}

async fn h_flaky(
    State(inner): State<Arc<Inner>>,
    Query(q): Query<ModeQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> StatusCode {
    let fail = rand::rng().random::<f64>() < q.p.unwrap_or(0.5);
    let code = if fail { 500 } else { 200 };
    record(
        &inner,
        SinkMode::Flaky,
        code,
        &headers,
        &body,
        Instant::now(),
    );
    StatusCode::from_u16(code).expect("static status")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_roundtrip_and_rejection() {
        let body = br#"{"event_id":"e","event":"transaction.included"}"#;
        let sig = sign(b"secret", 1_700_000_000, body);
        let header = format!("t=1700000000,v1={sig}");
        assert_eq!(
            verify_signature(b"secret", Some(&header), body),
            Ok(1_700_000_000)
        );
        assert!(verify_signature(b"other", Some(&header), body).is_err());
        assert!(verify_signature(b"secret", Some(&header), b"tampered").is_err());
        assert!(verify_signature(b"secret", None, body).is_err());
        assert!(verify_signature(b"secret", Some("v1=zz"), body).is_err());
    }

    #[test]
    fn matches_known_hmac_vector() {
        // printf '1.{}' | openssl dgst -sha256 -hmac k
        assert_eq!(
            sign(b"k", 1, b"{}"),
            "3dd49b2593d0f9a349e9e71c4bde3e2b862c2be4003fe9b4ba81332029310158"
        );
    }
}
