//! JSON-RPC client for one chain.
//!
//! A thin layer over `reqwest` (rather than a provider abstraction) so the engine controls exactly what
//! goes over the wire: no implicit nonce/gas/chain-id calls, header-based auth, explicit timeouts, and a
//! precise split between "the node said no", "the provider throttled us" and "we do not know whether the
//! node saw this" — the distinction nonce safety depends on.

use std::{
    sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, Bytes, B256, U256, U64};
use rand::Rng;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use super::{
    credits::CreditMeter,
    limiter::{Lane, Limiter},
    types::{Block, Receipt},
};
use crate::{error::RpcError, telemetry};

#[derive(Debug, Clone, Copy)]
pub struct CallOpts {
    pub lane: Lane,
    pub timeout: Duration,
    /// Retry when the request may not have reached the node (timeouts, resets, 5xx). Only safe for
    /// idempotent calls — reads, and sends of bytes that are already persisted.
    pub retry_transport: u32,
    /// Issue the call even when the circuit breaker is open (health probes).
    pub probe: bool,
}

pub struct RpcClient {
    chain_id: u64,
    chain_name: String,
    http: reqwest::Client,
    /// Separate pool for held-open sync sends so they can never exhaust connections needed for reads.
    sync_http: reqwest::Client,
    url: String,
    token: Option<String>,
    limiter: Arc<Limiter>,
    pub meter: Arc<CreditMeter>,
    next_id: AtomicU64,
    default_timeout: Duration,
    // Circuit breaker: consecutive transport failures open it; any success closes it.
    failure_threshold: u32,
    consecutive_failures: AtomicU32,
    open: AtomicBool,
    last_success: parking_lot::Mutex<Option<Instant>>,
}

pub struct RpcClientParams {
    pub chain_id: u64,
    pub chain_name: String,
    pub url: String,
    pub token: Option<String>,
    pub limiter: Arc<Limiter>,
    pub credits_per_call: u32,
    pub request_timeout: Duration,
    pub connect_timeout: Duration,
    pub failure_threshold: u32,
}

impl RpcClient {
    pub fn new(p: RpcClientParams) -> anyhow::Result<Self> {
        let build = |pool_idle: usize| {
            reqwest::Client::builder()
                .connect_timeout(p.connect_timeout)
                .pool_idle_timeout(Duration::from_secs(90))
                .pool_max_idle_per_host(pool_idle)
                .tcp_keepalive(Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|e| anyhow::anyhow!("failed to build rpc http client: {}", e.without_url()))
        };
        Ok(Self {
            chain_id: p.chain_id,
            chain_name: p.chain_name,
            http: build(32)?,
            sync_http: build(256)?,
            url: p.url,
            token: p.token,
            limiter: p.limiter,
            meter: Arc::new(CreditMeter::new(p.credits_per_call)),
            next_id: AtomicU64::new(1),
            default_timeout: p.request_timeout,
            failure_threshold: p.failure_threshold.max(1),
            consecutive_failures: AtomicU32::new(0),
            open: AtomicBool::new(false),
            last_success: parking_lot::Mutex::new(None),
        })
    }

    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    pub fn is_circuit_open(&self) -> bool {
        self.open.load(Ordering::Relaxed)
    }

    pub fn last_success(&self) -> Option<Instant> {
        *self.last_success.lock()
    }

    pub fn read_opts(&self) -> CallOpts {
        CallOpts { lane: Lane::Read, timeout: self.default_timeout, retry_transport: 2, probe: false }
    }

    pub fn background_opts(&self) -> CallOpts {
        CallOpts { lane: Lane::Background, timeout: self.default_timeout, retry_transport: 1, probe: false }
    }

    pub fn probe_opts(&self) -> CallOpts {
        CallOpts { lane: Lane::Background, timeout: self.default_timeout, retry_transport: 0, probe: true }
    }

    /// One JSON-RPC call with throttling, accounting, breaker and the retries `opts` allows.
    pub async fn call<T: DeserializeOwned>(&self, method: &'static str, params: Value, opts: CallOpts) -> Result<T, RpcError> {
        let mut transport_retries = 0u32;
        let mut throttle_retries = 0u32;
        loop {
            if self.is_circuit_open() && !opts.probe {
                return Err(RpcError::CircuitOpen(self.chain_id));
            }
            self.limiter.acquire(opts.lane).await;
            let started = Instant::now();
            let result = self.call_once::<T>(method, &params, opts, false).await;
            let elapsed = started.elapsed();
            metrics::histogram!("gum_rpc_latency_seconds", "chain" => self.chain_name.clone(), "method" => method).record(elapsed.as_secs_f64());

            match result {
                Ok(v) => {
                    self.on_success(method);
                    return Ok(v);
                }
                Err(e @ RpcError::Response { .. }) => {
                    // The node answered: the link is healthy even though the call failed. Providers bill
                    // per HTTP-200 response, so it is counted as a billable call, not as an rpc error.
                    self.on_success_transport(method, true);
                    return Err(e);
                }
                Err(RpcError::RateLimited(msg)) => {
                    self.meter.record(method, false);
                    self.limiter.penalize();
                    metrics::counter!("gum_rpc_rate_limited_total", "chain" => self.chain_name.clone()).increment(1);
                    if let Some(suppressed) = telemetry::throttled(&format!("rpc.rate_limited:{}", self.chain_id), Duration::from_secs(10)) {
                        tracing::warn!(event = "rpc.rate_limited", chain = self.chain_id, method, suppressed, detail = %msg, "provider rate limit hit; slowing down");
                    }
                    throttle_retries += 1;
                    if throttle_retries > 6 {
                        return Err(RpcError::RateLimited(msg));
                    }
                    tokio::time::sleep(backoff(throttle_retries, 150, 3_000)).await;
                }
                Err(e) => {
                    self.meter.record(method, false);
                    self.on_transport_failure(method, &e);
                    if transport_retries >= opts.retry_transport {
                        return Err(e);
                    }
                    transport_retries += 1;
                    tokio::time::sleep(backoff(transport_retries, 100, 2_000)).await;
                }
            }
        }
    }

    async fn call_once<T: DeserializeOwned>(&self, method: &'static str, params: &Value, opts: CallOpts, held_open: bool) -> Result<T, RpcError> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let body = json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params});
        let client = if held_open { &self.sync_http } else { &self.http };
        let mut req = client.post(&self.url).timeout(opts.timeout).json(&body);
        if let Some(token) = &self.token {
            req = req.header("x-token", token);
        }
        // `without_url` keeps endpoint URLs (which may embed credentials) out of every error and log line.
        let resp = req.send().await.map_err(|e| RpcError::Transport(e.without_url().to_string()))?;
        let status = resp.status();
        let bytes = resp.bytes().await.map_err(|e| RpcError::Transport(e.without_url().to_string()))?;

        if status.as_u16() == 429 {
            return Err(RpcError::RateLimited(snippet(&bytes)));
        }
        if status.is_server_error() {
            return Err(RpcError::Transport(format!("http {}: {}", status.as_u16(), snippet(&bytes))));
        }
        let envelope: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(_) if !status.is_success() => {
                return Err(RpcError::Transport(format!("http {}: {}", status.as_u16(), snippet(&bytes))));
            }
            Err(e) => return Err(RpcError::Transport(format!("undecodable response body: {e}"))),
        };
        if let Some(err) = envelope.get("error").filter(|e| !e.is_null()) {
            let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
            let message = err.get("message").and_then(Value::as_str).unwrap_or("").to_string();
            let data = err.get("data").map(|d| d.as_str().map(str::to_string).unwrap_or_else(|| d.to_string()));
            // QuickNode: 429 in the body, -32007 per-second, -32008 per-minute.
            if matches!(code, 429 | -32007 | -32008) {
                return Err(RpcError::RateLimited(message));
            }
            return Err(RpcError::Response { code, message, data });
        }
        if !status.is_success() {
            return Err(RpcError::Transport(format!("http {}: {}", status.as_u16(), snippet(&bytes))));
        }
        let result = envelope.get("result").cloned().unwrap_or(Value::Null);
        serde_json::from_value(result).map_err(|e| RpcError::Decode(format!("{method}: {e}")))
    }

    fn on_success(&self, method: &'static str) {
        self.on_success_transport(method, true);
    }

    fn on_success_transport(&self, method: &'static str, billable: bool) {
        self.meter.record(method, billable);
        *self.last_success.lock() = Some(Instant::now());
        self.consecutive_failures.store(0, Ordering::Relaxed);
        if self.open.swap(false, Ordering::Relaxed) {
            tracing::info!(event = "rpc.circuit_closed", chain = self.chain_id, "rpc endpoint is answering again");
        }
    }

    fn on_transport_failure(&self, method: &'static str, err: &RpcError) {
        metrics::counter!("gum_rpc_errors_total", "chain" => self.chain_name.clone(), "method" => method).increment(1);
        let failures = self.consecutive_failures.fetch_add(1, Ordering::Relaxed) + 1;
        if failures >= self.failure_threshold && !self.open.swap(true, Ordering::Relaxed) {
            telemetry::alert(
                &format!("rpc.circuit_open:{}", self.chain_id),
                "rpc.circuit_open",
                "rpc endpoint is failing; circuit opened, chain monitor will probe for recovery",
                Some(self.chain_id),
                None,
                json!({"consecutive_failures": failures, "last_error": err.to_string(), "method": method}),
            );
        } else if let Some(suppressed) = telemetry::throttled(&format!("rpc.transport:{}", self.chain_id), Duration::from_secs(10)) {
            tracing::warn!(event = "rpc.transport_error", chain = self.chain_id, method, suppressed, error = %err, "rpc transport failure");
        }
    }

    // ---- typed methods -------------------------------------------------------------------------

    pub async fn eth_chain_id(&self) -> Result<u64, RpcError> {
        let v: U64 = self.call("eth_chainId", json!([]), self.probe_opts()).await?;
        Ok(v.saturating_to())
    }

    /// `tag` is a block tag (`latest`, `finalized`) or a hex number. Transaction hashes only.
    pub async fn get_block(&self, tag: &str, opts: CallOpts) -> Result<Option<Block>, RpcError> {
        self.call("eth_getBlockByNumber", json!([tag, false]), opts).await
    }

    pub async fn get_block_by_number(&self, number: u64, opts: CallOpts) -> Result<Option<Block>, RpcError> {
        self.get_block(&format!("{number:#x}"), opts).await
    }

    pub async fn get_receipt(&self, hash: B256, opts: CallOpts) -> Result<Option<Receipt>, RpcError> {
        self.call("eth_getTransactionReceipt", json!([hash]), opts).await
    }

    /// On-chain nonce at `latest`. A floor for the local nonce, never its source of truth.
    pub async fn get_transaction_count(&self, address: Address, opts: CallOpts) -> Result<u64, RpcError> {
        let v: U64 = self.call("eth_getTransactionCount", json!([address, "latest"]), opts).await?;
        Ok(v.saturating_to())
    }

    pub async fn get_balance(&self, address: Address, opts: CallOpts) -> Result<U256, RpcError> {
        self.call("eth_getBalance", json!([address, "latest"]), opts).await
    }

    pub async fn get_code(&self, address: Address, opts: CallOpts) -> Result<Bytes, RpcError> {
        self.call("eth_getCode", json!([address, "latest"]), opts).await
    }

    pub async fn eth_call(&self, to: Address, data: Bytes, opts: CallOpts) -> Result<Bytes, RpcError> {
        self.call("eth_call", json!([{"to": to, "data": data}, "latest"]), opts).await
    }

    pub async fn estimate_gas(&self, from: Address, to: Address, data: &Bytes, value: U256, opts: CallOpts) -> Result<u64, RpcError> {
        let v: U64 = self.call("eth_estimateGas", json!([{"from": from, "to": to, "data": data, "value": value}]), opts).await?;
        Ok(v.saturating_to())
    }

    /// Fire-and-forget broadcast. Resending identical bytes is idempotent, so transport retries are safe
    /// as long as the attempt was persisted first (the caller's responsibility).
    pub async fn send_raw(&self, raw: &Bytes, retry_transport: u32) -> Result<B256, RpcError> {
        let opts = CallOpts { lane: Lane::Send, timeout: self.default_timeout, retry_transport, probe: false };
        self.call("eth_sendRawTransaction", json!([raw]), opts).await
    }

    /// Broadcast and wait for the receipt in one call (EIP-7966). The connection is held open until the
    /// transaction is included or the node's own timeout fires, so it uses the dedicated pool and is
    /// never retried here: a lost response is `Indeterminate` and handed to the tracker.
    pub async fn send_raw_sync(&self, raw: &Bytes, timeout: Duration) -> Result<Receipt, RpcError> {
        const METHOD: &str = "eth_sendRawTransactionSync";
        if self.is_circuit_open() {
            return Err(RpcError::CircuitOpen(self.chain_id));
        }
        self.limiter.acquire(Lane::Send).await;
        let opts = CallOpts { lane: Lane::Send, timeout, retry_transport: 0, probe: false };
        let started = Instant::now();
        let result = self.call_once::<Receipt>(METHOD, &json!([raw]), opts, true).await;
        metrics::histogram!("gum_rpc_latency_seconds", "chain" => self.chain_name.clone(), "method" => METHOD).record(started.elapsed().as_secs_f64());
        match &result {
            Ok(_) => self.on_success(METHOD),
            Err(RpcError::Response { .. }) => self.on_success_transport(METHOD, true),
            Err(RpcError::RateLimited(_)) => {
                self.meter.record(METHOD, false);
                self.limiter.penalize();
            }
            Err(e) => {
                self.meter.record(METHOD, false);
                self.on_transport_failure(METHOD, e);
            }
        }
        result
    }

    /// Distinguishes "method not supported" from every other outcome by sending bytes no node can decode.
    pub async fn supports_sync_send(&self) -> Result<bool, RpcError> {
        let opts = CallOpts { lane: Lane::Background, timeout: self.default_timeout, retry_transport: 1, probe: true };
        match self.call::<Value>("eth_sendRawTransactionSync", json!(["0xdeadbeef"]), opts).await {
            Ok(_) => Ok(true),
            Err(e) if e.is_method_not_found() => Ok(false),
            Err(RpcError::Response { code, message, .. }) => {
                let text = message.to_lowercase();
                // Some gateways report unknown methods with their own codes.
                let unsupported = matches!(code, -32604)
                    || text.contains("method not found")
                    || text.contains("not supported")
                    || text.contains("unsupported method")
                    || text.contains("does not exist")
                    || text.contains("not available");
                Ok(!unsupported)
            }
            Err(e) => Err(e),
        }
    }
}

fn backoff(attempt: u32, base_ms: u64, max_ms: u64) -> Duration {
    let exp = base_ms.saturating_mul(1u64 << attempt.min(10)).min(max_ms);
    let jitter = rand::rng().random_range(0..=exp / 2);
    Duration::from_millis(exp / 2 + jitter)
}

fn snippet(bytes: &[u8]) -> String {
    let text = String::from_utf8_lossy(bytes);
    let mut s: String = text.chars().take(200).collect();
    if text.chars().count() > 200 {
        s.push('…');
    }
    s
}
