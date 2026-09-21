//! Client + response types for the engine's public HTTP surface (docs/api-contract.md).
//! Parsing is lenient about *extra* fields and strict about the fields the verdict relies on.

// Response types mirror the contract; not every mirrored field is consumed by a check (yet).
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, Utc};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ApiError {
    #[serde(default)]
    pub code: String,
    #[serde(default)]
    pub message: String,
    #[serde(default)]
    pub revert_data: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Timestamps {
    #[serde(default)]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub sent_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub included_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub confirmed_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub failed_at: Option<DateTime<Utc>>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct TxStatus {
    pub job_id: String,
    #[serde(default)]
    pub chain_id: u64,
    pub status: String,
    #[serde(default)]
    pub outcome: Option<String>,
    #[serde(default)]
    pub signer: Option<String>,
    #[serde(default)]
    pub nonce: Option<u64>,
    #[serde(default)]
    pub tx_hash: Option<String>,
    #[serde(default)]
    pub block_number: Option<u64>,
    #[serde(default)]
    pub error: Option<ApiError>,
    #[serde(default)]
    pub timestamps: Timestamps,
}

impl TxStatus {
    pub fn is_terminal(&self) -> bool {
        self.status == "confirmed" || self.status == "failed"
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Pause {
    #[serde(default)]
    pub reason: String,
    #[serde(default)]
    pub detail: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Pair {
    pub chain_id: u64,
    pub signer: String,
    #[serde(default)]
    pub role: String,
    pub state: String,
    #[serde(default)]
    pub pause: Option<Pause>,
    #[serde(default)]
    pub next_nonce: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct SignersResp {
    pub pairs: Vec<Pair>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct Bal {
    pub address: String,
    pub confirmed: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ChainBalances {
    pub chain_id: u64,
    #[serde(default)]
    pub treasury: Option<Bal>,
    #[serde(default)]
    pub signers: Vec<Bal>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct BalancesResp {
    pub chains: Vec<ChainBalances>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct RpcMeter {
    #[serde(default)]
    pub calls: u64,
    #[serde(default)]
    pub errors: u64,
    #[serde(default)]
    pub by_method: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ChainInfo {
    pub chain_id: u64,
    #[serde(default)]
    pub status: String,
    #[serde(default)]
    pub send_mode: Option<String>,
    #[serde(default)]
    pub queue_depth: u64,
    #[serde(default)]
    pub rpc: RpcMeter,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ChainsResp {
    pub chains: Vec<ChainInfo>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct Counts {
    pub queued: u64,
    pub processing: u64,
    pub in_flight: u64,
    pub included: u64,
    pub succeeded: u64,
    pub reverted: u64,
    pub failed: u64,
    pub total: u64,
}

impl Counts {
    /// Jobs that are neither queued nor finished.
    pub fn active(&self) -> u64 {
        self.processing + self.in_flight + self.included
    }
    pub fn finished(&self) -> u64 {
        self.succeeded + self.reverted + self.failed
    }
    pub fn minus(&self, base: &Counts) -> Counts {
        Counts {
            queued: self.queued.saturating_sub(base.queued),
            processing: self.processing.saturating_sub(base.processing),
            in_flight: self.in_flight.saturating_sub(base.in_flight),
            included: self.included.saturating_sub(base.included),
            succeeded: self.succeeded.saturating_sub(base.succeeded),
            reverted: self.reverted.saturating_sub(base.reverted),
            failed: self.failed.saturating_sub(base.failed),
            total: self.total.saturating_sub(base.total),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct ChainCounts {
    pub chain_id: u64,
    #[serde(flatten)]
    pub counts: Counts,
}

#[derive(Clone, Debug, Default, Deserialize)]
pub struct AnalyticsResp {
    pub totals: Counts,
    #[serde(default)]
    pub by_chain: Vec<ChainCounts>,
}

/// Result of one `POST /v1/transactions`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SubmitResult {
    pub status: Option<u16>,
    pub job_id: Option<String>,
    pub replayed: bool,
    pub error_code: Option<String>,
    /// Set when no HTTP response was obtained (connect error, reset, timeout): outcome is indeterminate.
    pub transport_error: Option<String>,
}

#[derive(Clone)]
pub struct EngineClient {
    pub base: String,
    http: reqwest::Client,
}

impl EngineClient {
    pub fn new(base: &str) -> Result<Self> {
        let http = reqwest::Client::builder()
            .pool_max_idle_per_host(2048)
            .pool_idle_timeout(Duration::from_secs(30))
            .tcp_nodelay(true)
            .http1_only()
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(30))
            .build()
            .context("building the shared HTTP client")?;
        Ok(Self {
            base: base.trim_end_matches('/').to_string(),
            http,
        })
    }

    pub async fn get_status_code(&self, path: &str, timeout: Duration) -> Result<u16> {
        let r = self
            .http
            .get(format!("{}{path}", self.base))
            .timeout(timeout)
            .send()
            .await?;
        Ok(r.status().as_u16())
    }

    pub async fn get_json<T: DeserializeOwned>(&self, path: &str, timeout: Duration) -> Result<T> {
        let url = format!("{}{path}", self.base);
        let r = self
            .http
            .get(&url)
            .timeout(timeout)
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        let status = r.status();
        let body = r
            .text()
            .await
            .with_context(|| format!("GET {url}: reading body"))?;
        if !status.is_success() {
            return Err(anyhow!("GET {url} -> {status}: {}", truncate(&body, 300)));
        }
        serde_json::from_str(&body).with_context(|| {
            format!(
                "GET {url}: body does not match the contract: {}",
                truncate(&body, 300)
            )
        })
    }

    /// `Ok(None)` = 404.
    pub async fn tx_status(&self, job_id: &str) -> Result<Option<TxStatus>> {
        let url = format!("{}/v1/transactions/{job_id}", self.base);
        let r = self
            .http
            .get(&url)
            .timeout(Duration::from_secs(10))
            .send()
            .await
            .with_context(|| format!("GET {url}"))?;
        if r.status().as_u16() == 404 {
            return Ok(None);
        }
        let status = r.status();
        let body = r.text().await?;
        if !status.is_success() {
            return Err(anyhow!("GET {url} -> {status}: {}", truncate(&body, 300)));
        }
        let s: TxStatus = serde_json::from_str(&body).with_context(|| {
            format!(
                "GET {url}: body does not match the contract: {}",
                truncate(&body, 300)
            )
        })?;
        Ok(Some(s))
    }

    pub async fn submit(&self, body: &Value, idem_key: Option<&str>) -> SubmitResult {
        let mut req = self
            .http
            .post(format!("{}/v1/transactions", self.base))
            .json(body);
        if let Some(k) = idem_key {
            req = req.header("Idempotency-Key", k);
        }
        let resp = match req.send().await {
            Ok(r) => r,
            Err(e) => {
                return SubmitResult {
                    transport_error: Some(short_err(&e)),
                    ..Default::default()
                }
            }
        };
        let status = resp.status().as_u16();
        let v: Value = match resp.bytes().await {
            Ok(b) => serde_json::from_slice(&b).unwrap_or(Value::Null),
            Err(e) => {
                // Headers arrived but the body did not: for a 2xx we do not know the job id => indeterminate.
                return SubmitResult {
                    status: Some(status),
                    transport_error: Some(short_err(&e)),
                    ..Default::default()
                };
            }
        };
        SubmitResult {
            status: Some(status),
            job_id: v.get("job_id").and_then(|j| j.as_str()).map(str::to_string),
            replayed: v.get("replayed").and_then(|r| r.as_bool()).unwrap_or(false),
            error_code: v
                .pointer("/error/code")
                .and_then(|c| c.as_str())
                .map(str::to_string),
            transport_error: None,
        }
    }
}

fn short_err(e: &reqwest::Error) -> String {
    if e.is_timeout() {
        "timeout".into()
    } else if e.is_connect() {
        "connect".into()
    } else {
        "io".into()
    }
}

pub fn truncate(s: &str, n: usize) -> String {
    if s.len() <= n {
        s.to_string()
    } else {
        let mut end = n;
        while !s.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &s[..end])
    }
}
