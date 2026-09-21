//! Counting JSON-RPC reverse proxy that sits between the engine and one Anvil.
//!
//! * counts every request per JSON-RPC method (single and batch bodies) and records per-method latency;
//! * injects faults at runtime (per method, probability, time window);
//! * optionally adds a fixed symmetric delay (`--rpc-rtt-ms`, default 0) to emulate WAN distance.
//!
//! Built on raw hyper so a handler can kill its own TCP connection without replying — required for the
//! "forwarded, but the response was lost" (indeterminate send) fault.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use bytes::Bytes;
use hdrhistogram::Histogram;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use parking_lot::{Mutex, RwLock};
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::net::TcpListener;
use tokio::sync::Notify;

use crate::util::{new_hist, percentiles_ms, record, Percentiles};

/// Longest a held (blackholed / dropped) connection is kept open before the proxy closes it.
const MAX_HOLD: Duration = Duration::from_secs(120);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum FaultKind {
    /// HTTP 429 with a JSON-RPC shaped body.
    Http429,
    /// HTTP 200 carrying JSON-RPC error -32007 (provider rate limit).
    RpcError32007,
    Http500,
    Http503,
    /// Hold the connection for `secs`, then close it without a response. Never forwarded.
    DelayThenClose {
        secs: f64,
    },
    /// Forward to Anvil, wait for its answer, then close the client connection without replying.
    DropAfterForward,
    /// Never forward, never answer (held until the client gives up).
    Blackhole,
}

impl FaultKind {
    pub fn label(&self) -> &'static str {
        match self {
            FaultKind::Http429 => "http_429",
            FaultKind::RpcError32007 => "rpc_-32007",
            FaultKind::Http500 => "http_500",
            FaultKind::Http503 => "http_503",
            FaultKind::DelayThenClose { .. } => "delay_then_close",
            FaultKind::DropAfterForward => "drop_after_forward",
            FaultKind::Blackhole => "blackhole",
        }
    }
}

#[derive(Clone, Debug)]
pub struct FaultRule {
    /// `None` = every method.
    pub methods: Option<Vec<String>>,
    pub kind: FaultKind,
    pub probability: f64,
    pub from: Instant,
    pub until: Instant,
}

impl FaultRule {
    fn matches(&self, methods: &[String], now: Instant) -> bool {
        if now < self.from || now >= self.until {
            return false;
        }
        match &self.methods {
            None => true,
            Some(ms) => methods.iter().any(|m| ms.iter().any(|x| x == m)),
        }
    }
}

struct MethodStats {
    calls: u64,
    faulted: u64,
    upstream_errors: u64,
    latency: Histogram<u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct MethodSnapshot {
    pub calls: u64,
    pub faulted: u64,
    /// JSON-RPC error responses or transport failures from Anvil itself (not injected).
    pub upstream_errors: u64,
    pub latency_ms: Percentiles,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProxySnapshot {
    pub http_requests: u64,
    pub total_calls: u64,
    pub by_method: BTreeMap<String, MethodSnapshot>,
    pub faults_injected: BTreeMap<String, u64>,
}

struct State {
    upstream: String,
    client: reqwest::Client,
    rtt_half: Duration,
    http_requests: AtomicU64,
    stats: Mutex<BTreeMap<String, MethodStats>>,
    faults: RwLock<Vec<FaultRule>>,
    fault_counts: Mutex<BTreeMap<String, u64>>,
}

#[derive(Clone)]
pub struct Proxy {
    pub addr: SocketAddr,
    state: Arc<State>,
}

impl Proxy {
    pub async fn start(upstream: &str, rtt: Duration) -> Result<Proxy> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .context("binding the RPC proxy")?;
        let addr = listener.local_addr()?;
        let client = reqwest::Client::builder()
            .pool_max_idle_per_host(512)
            .tcp_nodelay(true)
            .http1_only()
            .timeout(Duration::from_secs(60))
            .build()?;
        let state = Arc::new(State {
            upstream: upstream.to_string(),
            client,
            rtt_half: rtt / 2,
            http_requests: AtomicU64::new(0),
            stats: Mutex::new(BTreeMap::new()),
            faults: RwLock::new(Vec::new()),
            fault_counts: Mutex::new(BTreeMap::new()),
        });
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = match listener.accept().await {
                    Ok(x) => x,
                    Err(e) => {
                        eprintln!("[proxy] accept error: {e}");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let st = st.clone();
                tokio::spawn(async move {
                    // The handler fires `kill` to have its own connection torn down without a reply.
                    let kill = Arc::new(Notify::new());
                    let k2 = kill.clone();
                    let svc = service_fn(move |req| handle(st.clone(), k2.clone(), req));
                    let builder =
                        hyper_util::server::conn::auto::Builder::new(TokioExecutor::new());
                    let conn = builder.serve_connection(TokioIo::new(stream), svc);
                    tokio::pin!(conn);
                    tokio::select! {
                        _ = &mut conn => {}
                        _ = kill.notified() => { /* dropping `conn` closes the socket */ }
                    }
                });
            }
        });
        Ok(Proxy { addr, state })
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn add_fault(&self, rule: FaultRule) {
        self.state.faults.write().push(rule);
    }

    pub fn clear_faults(&self) {
        self.state.faults.write().clear();
    }

    pub fn snapshot(&self) -> ProxySnapshot {
        let stats = self.state.stats.lock();
        let by_method: BTreeMap<String, MethodSnapshot> = stats
            .iter()
            .map(|(m, s)| {
                (
                    m.clone(),
                    MethodSnapshot {
                        calls: s.calls,
                        faulted: s.faulted,
                        upstream_errors: s.upstream_errors,
                        latency_ms: percentiles_ms(&s.latency),
                    },
                )
            })
            .collect();
        ProxySnapshot {
            http_requests: self.state.http_requests.load(Ordering::Relaxed),
            total_calls: by_method.values().map(|m| m.calls).sum(),
            by_method,
            faults_injected: self.state.fault_counts.lock().clone(),
        }
    }
}

/// Methods named in a JSON-RPC body (single object or batch array) plus the first id (for error replies).
fn parse_methods(body: &[u8]) -> (Vec<String>, Value) {
    let method_of = |v: &Value| {
        v.get("method")
            .and_then(|m| m.as_str())
            .unwrap_or("<no-method>")
            .to_string()
    };
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Array(items)) if !items.is_empty() => {
            let id = items[0].get("id").cloned().unwrap_or(Value::Null);
            (items.iter().map(method_of).collect(), id)
        }
        Ok(v @ Value::Object(_)) => {
            let id = v.get("id").cloned().unwrap_or(Value::Null);
            (vec![method_of(&v)], id)
        }
        _ => (vec!["<unparseable>".to_string()], Value::Null),
    }
}

fn reply(status: StatusCode, body: Vec<u8>) -> Response<Full<Bytes>> {
    let mut r = Response::new(Full::new(Bytes::from(body)));
    *r.status_mut() = status;
    r.headers_mut().insert(
        "content-type",
        "application/json".parse().expect("static header"),
    );
    r
}

/// Hold the request for `d`, then have the connection task drop the socket. Never actually returns a
/// response: once `kill` fires the connection (and this future with it) is dropped.
async fn hold_then_kill(kill: &Notify, d: Duration) -> Response<Full<Bytes>> {
    tokio::time::sleep(d.min(MAX_HOLD)).await;
    kill.notify_one();
    std::future::pending::<()>().await;
    reply(StatusCode::BAD_GATEWAY, Vec::new())
}

async fn handle(
    st: Arc<State>,
    kill: Arc<Notify>,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let started = Instant::now();
    st.http_requests.fetch_add(1, Ordering::Relaxed);
    let body = match req.into_body().collect().await {
        Ok(b) => b.to_bytes(),
        Err(_) => return Ok(reply(StatusCode::BAD_REQUEST, b"{}".to_vec())),
    };
    let (methods, id) = parse_methods(&body);

    let fault = {
        let now = Instant::now();
        let rules = st.faults.read();
        let mut rng = rand::rng();
        rules
            .iter()
            .find(|r| {
                r.matches(&methods, now)
                    && (r.probability >= 1.0 || rng.random::<f64>() < r.probability)
            })
            .map(|r| r.kind.clone())
    };

    let finish = |faulted: bool, upstream_error: bool| {
        let elapsed = started.elapsed();
        let mut stats = st.stats.lock();
        for m in &methods {
            let e = stats.entry(m.clone()).or_insert_with(|| MethodStats {
                calls: 0,
                faulted: 0,
                upstream_errors: 0,
                latency: new_hist(),
            });
            e.calls += 1;
            e.faulted += faulted as u64;
            e.upstream_errors += upstream_error as u64;
            if !faulted {
                record(&mut e.latency, elapsed);
            }
        }
    };

    if let Some(kind) = &fault {
        *st.fault_counts
            .lock()
            .entry(kind.label().to_string())
            .or_insert(0) += 1;
    }

    match fault {
        Some(FaultKind::Http429) => {
            finish(true, false);
            let b = json!({"jsonrpc":"2.0","error":{"code":429,"message":"rate limited"},"id":id});
            return Ok(reply(
                StatusCode::TOO_MANY_REQUESTS,
                b.to_string().into_bytes(),
            ));
        }
        Some(FaultKind::RpcError32007) => {
            finish(true, false);
            let b = json!({"jsonrpc":"2.0","error":{"code":-32007,"message":"request limit reached"},"id":id});
            return Ok(reply(StatusCode::OK, b.to_string().into_bytes()));
        }
        Some(FaultKind::Http500) => {
            finish(true, false);
            return Ok(reply(
                StatusCode::INTERNAL_SERVER_ERROR,
                b"{\"error\":\"injected 500\"}".to_vec(),
            ));
        }
        Some(FaultKind::Http503) => {
            finish(true, false);
            return Ok(reply(
                StatusCode::SERVICE_UNAVAILABLE,
                b"{\"error\":\"injected 503\"}".to_vec(),
            ));
        }
        Some(FaultKind::DelayThenClose { secs }) => {
            finish(true, false);
            return Ok(hold_then_kill(&kill, Duration::from_secs_f64(secs)).await);
        }
        Some(FaultKind::Blackhole) => {
            finish(true, false);
            return Ok(hold_then_kill(&kill, MAX_HOLD).await);
        }
        Some(FaultKind::DropAfterForward) | None => {}
    }

    if !st.rtt_half.is_zero() {
        tokio::time::sleep(st.rtt_half).await;
    }
    let upstream = st
        .client
        .post(&st.upstream)
        .header("content-type", "application/json")
        .body(body)
        .send()
        .await;
    let (status, bytes) = match upstream {
        Ok(r) => {
            let status = r.status();
            match r.bytes().await {
                Ok(b) => (status, b),
                Err(_) => (
                    StatusCode::BAD_GATEWAY,
                    Bytes::from_static(b"{\"error\":\"upstream body\"}"),
                ),
            }
        }
        Err(_) => (
            StatusCode::BAD_GATEWAY,
            Bytes::from_static(b"{\"error\":\"upstream unreachable\"}"),
        ),
    };
    if !st.rtt_half.is_zero() {
        tokio::time::sleep(st.rtt_half).await;
    }

    if fault == Some(FaultKind::DropAfterForward) {
        finish(true, false);
        return Ok(hold_then_kill(&kill, Duration::ZERO).await);
    }

    let upstream_error = !status.is_success() || contains_rpc_error(&bytes);
    finish(false, upstream_error);
    let mut r = Response::new(Full::new(bytes));
    *r.status_mut() = StatusCode::from_u16(status.as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
    r.headers_mut().insert(
        "content-type",
        "application/json".parse().expect("static header"),
    );
    Ok(r)
}

/// Cheap check for a top-level JSON-RPC `error` member without fully parsing large responses.
fn contains_rpc_error(body: &[u8]) -> bool {
    if body.len() > 4096 {
        return false;
    }
    match serde_json::from_slice::<Value>(body) {
        Ok(Value::Object(o)) => o.contains_key("error"),
        Ok(Value::Array(a)) => a.iter().any(|v| v.get("error").is_some()),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_and_batch() {
        let (m, id) =
            parse_methods(br#"{"jsonrpc":"2.0","id":7,"method":"eth_chainId","params":[]}"#);
        assert_eq!(m, vec!["eth_chainId"]);
        assert_eq!(id, json!(7));
        let (m, _) = parse_methods(br#"[{"id":1,"method":"a"},{"id":2,"method":"b"}]"#);
        assert_eq!(m, vec!["a", "b"]);
        let (m, _) = parse_methods(b"not json");
        assert_eq!(m, vec!["<unparseable>"]);
    }

    /// End-to-end against a stub upstream: counting, 429 injection and drop-after-forward.
    #[tokio::test]
    async fn counts_and_injects_faults() {
        use axum::{routing::post, Json, Router};
        let hits = Arc::new(AtomicU64::new(0));
        let h2 = hits.clone();
        let app = Router::new().route(
            "/",
            post(move |Json(v): Json<Value>| {
                let h = h2.clone();
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                    Json(json!({"jsonrpc":"2.0","id":v["id"],"result":"0x1"}))
                }
            }),
        );
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let up = format!("http://{}", l.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(l, app).await.unwrap() });

        let p = Proxy::start(&up, Duration::ZERO).await.unwrap();
        let c = reqwest::Client::new();
        let call = |m: &'static str| {
            let c = c.clone();
            let url = p.url();
            async move {
                c.post(url)
                    .json(&json!({"jsonrpc":"2.0","id":1,"method":m,"params":[]}))
                    .send()
                    .await
            }
        };
        assert_eq!(call("eth_chainId").await.unwrap().status(), 200);
        assert_eq!(hits.load(Ordering::SeqCst), 1);

        let now = Instant::now();
        p.add_fault(FaultRule {
            methods: Some(vec!["eth_gasPrice".into()]),
            kind: FaultKind::Http429,
            probability: 1.0,
            from: now,
            until: now + Duration::from_secs(60),
        });
        p.add_fault(FaultRule {
            methods: Some(vec!["eth_sendRawTransactionSync".into()]),
            kind: FaultKind::DropAfterForward,
            probability: 1.0,
            from: now,
            until: now + Duration::from_secs(60),
        });
        let r = call("eth_gasPrice").await.unwrap();
        assert_eq!(r.status(), 429);
        let v: Value = r.json().await.unwrap();
        assert_eq!(v["error"]["code"], 429);
        assert_eq!(hits.load(Ordering::SeqCst), 1, "429 must not be forwarded");

        assert!(
            call("eth_sendRawTransactionSync").await.is_err(),
            "connection must be closed without a reply"
        );
        assert_eq!(
            hits.load(Ordering::SeqCst),
            2,
            "drop-after-forward must reach upstream"
        );
        assert_eq!(call("eth_chainId").await.unwrap().status(), 200);

        let s = p.snapshot();
        assert_eq!(s.by_method["eth_chainId"].calls, 2);
        assert_eq!(s.by_method["eth_gasPrice"].faulted, 1);
        assert_eq!(s.by_method["eth_sendRawTransactionSync"].calls, 1);
        assert_eq!(s.total_calls, 4);
    }
}
