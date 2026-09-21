//! `gum-bench fake-engine` — a deliberately simple, in-memory implementation of docs/api-contract.md.
//!
//! It exists to validate the *harness*: a clean fake must produce a clean verdict, and each `--bug`
//! must be caught by the check that claims to catch it. No persistence, no crash recovery (so `chaos`
//! is expected to FAIL against it — which is itself a useful self-check).
//!
//! Bugs: `double-send` (every 7th job is sent again from another signer), `skip-webhook` (every 5th job
//! gets no `transaction.confirmed`), `lose-job` (every 11th accepted job is never processed),
//! `bad-signature` (every 5th webhook is signed with the wrong secret), `wrong-analytics` (succeeded is
//! over-reported by one), `nonce-gap` is not possible on a real chain and therefore not offered.

use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::network::TxSignerSync;
use alloy::primitives::{Address, Bytes, TxKind, B256, U256};
use alloy::providers::Provider;
use alloy::rpc::types::{TransactionReceipt, TransactionRequest};
use alloy::signers::local::PrivateKeySigner;
use alloy::transports::{RpcError, TransportErrorKind};
use anyhow::{anyhow, bail, Context, Result};
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::Notify;
use uuid::Uuid;

use crate::oracle::{provider, DirectProvider};

const MAX_FEE: u128 = 20_000_000_000;
const TIP: u128 = 1_000_000_000;

// ------------------------------------------------------------------ config (per the contract)

#[derive(Deserialize)]
struct Cfg {
    #[serde(default)]
    server: ServerCfg,
    webhook: WebhookCfg,
    signers: SignersCfg,
    chains: BTreeMap<String, ChainCfg>,
}
#[derive(Deserialize, Default)]
struct ServerCfg {
    port: Option<u16>,
}
#[derive(Deserialize)]
struct WebhookCfg {
    signing_secret: String,
}
#[derive(Deserialize)]
struct SignersCfg {
    local_private_keys: Vec<String>,
}
#[derive(Deserialize)]
struct ChainCfg {
    chain_id: u64,
    rpc_url: String,
    treasury_private_key: String,
    signer_min_balance: String,
    topup_amount: String,
}

// ------------------------------------------------------------------ state

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Bug {
    None,
    DoubleSend,
    SkipWebhook,
    LoseJob,
    BadSignature,
    WrongAnalytics,
}

#[derive(Clone)]
struct Req {
    to: Address,
    data: Bytes,
    value: U256,
    gas_limit: Option<u64>,
    deadline: Option<String>,
    webhook_url: Option<String>,
}

struct Job {
    seq_no: u64,
    id: Uuid,
    chain_id: u64,
    req: Req,
    status: &'static str,
    outcome: Option<&'static str>,
    signer: Option<Address>,
    nonce: Option<u64>,
    tx_hash: Option<B256>,
    block_number: Option<u64>,
    block_hash: Option<B256>,
    gas_used: Option<u64>,
    effective_gas_price: Option<u128>,
    error: Option<Value>,
    attempts: Vec<Value>,
    webhooks: Vec<Value>,
    next_sequence: u64,
    created_at: DateTime<Utc>,
    sent_at: Option<DateTime<Utc>>,
    included_at: Option<DateTime<Utc>>,
    confirmed_at: Option<DateTime<Utc>>,
    failed_at: Option<DateTime<Utc>>,
}

struct PairState {
    state: &'static str,
    pause: Option<Value>,
    current_job: Option<Uuid>,
    next_nonce: u64,
    balance: U256,
}

struct Pair {
    signer: PrivateKeySigner,
    role: &'static str,
    st: Mutex<PairState>,
    ghosts: Mutex<VecDeque<Req>>,
}

struct Chain {
    id: u64,
    name: String,
    rpc: DirectProvider,
    queue: Mutex<VecDeque<Uuid>>,
    notify: Notify,
    pairs: Vec<Arc<Pair>>,
    treasury: Arc<Pair>,
    treasury_lock: tokio::sync::Mutex<()>,
    min_balance: U256,
    topup: U256,
    head: AtomicU64,
    rpc_counts: Mutex<BTreeMap<String, u64>>,
    rpc_errors: AtomicU64,
}

struct Fake {
    bug: Bug,
    secret: String,
    http: reqwest::Client,
    jobs: Mutex<HashMap<Uuid, Job>>,
    idem: Mutex<HashMap<String, (String, Uuid)>>,
    chains: BTreeMap<u64, Arc<Chain>>,
    accepted: AtomicU64,
    processed: AtomicU64,
    webhooks_sent: AtomicU64,
}

enum Fail {
    Retry,
    Semantic {
        message: String,
        data: Option<String>,
    },
}

fn classify(e: RpcError<TransportErrorKind>) -> Fail {
    match e {
        RpcError::ErrorResp(p) if p.code == 429 || p.code == -32007 => Fail::Retry,
        RpcError::ErrorResp(p) => Fail::Semantic {
            message: p.message.to_string(),
            data: p
                .data
                .as_ref()
                .map(|d| d.get().trim_matches('"').to_string()),
        },
        _ => Fail::Retry,
    }
}

impl Chain {
    fn count(&self, method: &str) {
        *self
            .rpc_counts
            .lock()
            .entry(method.to_string())
            .or_insert(0) += 1;
    }

    /// Call with unlimited retries on transport-level trouble (the fake is patient, not clever).
    async fn retrying<T, F, Fut>(&self, method: &str, f: F) -> Result<T, (String, Option<String>)>
    where
        F: Fn() -> Fut,
        Fut: std::future::Future<Output = Result<T, RpcError<TransportErrorKind>>>,
    {
        let mut delay = Duration::from_millis(100);
        loop {
            self.count(method);
            match tokio::time::timeout(Duration::from_secs(10), f()).await {
                Ok(Ok(v)) => return Ok(v),
                Ok(Err(e)) => match classify(e) {
                    Fail::Semantic { message, data } => return Err((message, data)),
                    Fail::Retry => {}
                },
                Err(_) => {}
            }
            self.rpc_errors.fetch_add(1, Ordering::Relaxed);
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(2));
        }
    }

    async fn balance(&self, a: Address) -> U256 {
        self.retrying("eth_getBalance", || async { self.rpc.get_balance(a).await })
            .await
            .unwrap_or_default()
    }

    /// Sign + send with `eth_sendRawTransactionSync`, surviving lost responses: the same raw tx is
    /// re-sent / looked up by hash until a receipt exists. `Err` = the node rejected it for good.
    async fn send(
        &self,
        pair: &Pair,
        nonce: u64,
        req: &Req,
        gas: u64,
    ) -> Result<TransactionReceipt, String> {
        let mut tx = TxEip1559 {
            chain_id: self.id,
            nonce,
            gas_limit: gas,
            max_fee_per_gas: MAX_FEE,
            max_priority_fee_per_gas: TIP,
            to: TxKind::Call(req.to),
            value: req.value,
            access_list: Default::default(),
            input: req.data.clone(),
        };
        let sig = pair
            .signer
            .sign_transaction_sync(&mut tx)
            .map_err(|e| format!("sign: {e}"))?;
        let env: TxEnvelope = tx.into_signed(sig).into();
        let hash = *env.tx_hash();
        let raw = Bytes::from(env.encoded_2718());
        let mut delay = Duration::from_millis(200);
        loop {
            self.count("eth_sendRawTransactionSync");
            let sent = tokio::time::timeout(
                Duration::from_secs(15),
                self.rpc.raw_request::<_, TransactionReceipt>(
                    "eth_sendRawTransactionSync".into(),
                    (raw.clone(),),
                ),
            )
            .await;
            match sent {
                Ok(Ok(r)) => return Ok(r),
                Ok(Err(e)) => match classify(e) {
                    Fail::Semantic { message, .. } => {
                        let m = message.to_lowercase();
                        let maybe_landed = m.contains("nonce too low")
                            || m.contains("already known")
                            || m.contains("already imported");
                        if !maybe_landed {
                            return Err(message);
                        }
                    }
                    Fail::Retry => {
                        self.rpc_errors.fetch_add(1, Ordering::Relaxed);
                    }
                },
                Err(_) => {
                    self.rpc_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            // Indeterminate: did an earlier attempt land?
            self.count("eth_getTransactionReceipt");
            if let Ok(Ok(Some(r))) = tokio::time::timeout(
                Duration::from_secs(5),
                self.rpc.get_transaction_receipt(hash),
            )
            .await
            {
                return Ok(r);
            }
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(2));
        }
    }
}

// ------------------------------------------------------------------ pipeline

impl Fake {
    fn emit(self: &Arc<Self>, job_id: Uuid, event: &'static str) {
        let (body, url, event_id, skip, bad_sig) = {
            let mut jobs = self.jobs.lock();
            let Some(j) = jobs.get_mut(&job_id) else {
                return;
            };
            let Some(url) = j.req.webhook_url.clone() else {
                return;
            };
            let event_id = Uuid::new_v4();
            let sequence = j.next_sequence;
            j.next_sequence += 1;
            let body = json!({
                "event_id": event_id, "event": event, "sequence": sequence,
                "job_id": j.id, "chain_id": j.chain_id, "status": j.status, "outcome": j.outcome,
                "tx_hash": j.tx_hash, "block_number": j.block_number, "block_hash": j.block_hash,
                "signer": j.signer, "nonce": j.nonce,
                "gas_used": j.gas_used.map(|g| g.to_string()),
                "effective_gas_price": j.effective_gas_price.map(|g| g.to_string()),
                "fee_paid": fee(j).map(|f| f.to_string()),
                "reincluded": false, "error": j.error, "timestamp": Utc::now().to_rfc3339(),
            });
            let skip = self.bug == Bug::SkipWebhook
                && event == "transaction.confirmed"
                && j.seq_no.is_multiple_of(5);
            j.webhooks.push(json!({"event": event, "sequence": sequence, "status": if skip { "dead" } else { "pending" }, "attempts": 0, "last_status_code": null, "delivered_at": null}));
            (
                body,
                url,
                event_id,
                skip,
                self.bug == Bug::BadSignature && j.seq_no.is_multiple_of(5),
            )
        };
        if skip {
            return;
        }
        let me = self.clone();
        tokio::spawn(async move {
            let raw = serde_json::to_vec(&body).expect("json");
            let mut delay = Duration::from_millis(250);
            for _ in 0..10 {
                let t = Utc::now().timestamp();
                let secret = if bad_sig {
                    "not-the-secret"
                } else {
                    me.secret.as_str()
                };
                let sig = crate::sink::sign(secret.as_bytes(), t, &raw);
                me.webhooks_sent.fetch_add(1, Ordering::Relaxed);
                let res = me
                    .http
                    .post(&url)
                    .header("content-type", "application/json")
                    .header("X-Gum-Event-Id", event_id.to_string())
                    .header("X-Gum-Job-Id", job_id.to_string())
                    .header("X-Gum-Signature", format!("t={t},v1={sig}"))
                    .body(raw.clone())
                    .timeout(Duration::from_secs(10))
                    .send()
                    .await;
                if matches!(&res, Ok(r) if r.status().is_success()) {
                    return;
                }
                tokio::time::sleep(delay).await;
                delay = (delay * 2).min(Duration::from_secs(4));
            }
        });
    }

    fn fail_job(
        self: &Arc<Self>,
        id: Uuid,
        code: &str,
        message: &str,
        revert_data: Option<String>,
    ) {
        if let Some(j) = self.jobs.lock().get_mut(&id) {
            j.status = "failed";
            j.failed_at = Some(Utc::now());
            j.error = Some(json!({"code": code, "message": message, "revert_data": revert_data}));
        }
        self.emit(id, "transaction.failed");
    }

    /// Top the pair up from the treasury; pauses (and keeps retrying) while the treasury is empty.
    async fn ensure_funds(&self, chain: &Chain, pair: &Pair) {
        loop {
            let _g = chain.treasury_lock.lock().await;
            let tbal = chain.balance(chain.treasury.signer.address()).await;
            if tbal < chain.topup + U256::from(10u64).pow(U256::from(16u64)) {
                drop(_g);
                {
                    let mut st = pair.st.lock();
                    st.state = "paused";
                    st.pause = Some(
                        json!({"reason": "InsufficientFunds", "recovery_step": "await_treasury_refill", "since": Utc::now().to_rfc3339(), "detail": format!("treasury balance {tbal}")}),
                    );
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let nonce = chain.treasury.st.lock().next_nonce;
            let req = Req {
                to: pair.signer.address(),
                data: Bytes::new(),
                value: chain.topup,
                gas_limit: Some(21_000),
                deadline: None,
                webhook_url: None,
            };
            chain.treasury.st.lock().state = "busy";
            let sent = chain.send(&chain.treasury, nonce, &req, 21_000).await;
            {
                let mut t = chain.treasury.st.lock();
                t.state = "idle";
                if sent.is_ok() {
                    t.next_nonce += 1;
                }
            }
            if sent.is_err() {
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            }
            let bal = chain.balance(pair.signer.address()).await;
            let mut st = pair.st.lock();
            st.balance = bal;
            st.pause = None;
            st.state = if st.current_job.is_some() {
                "busy"
            } else {
                "idle"
            };
            return;
        }
    }

    async fn process(self: &Arc<Self>, chain: &Arc<Chain>, pair: &Arc<Pair>, id: Uuid) {
        let Some(req) = self.jobs.lock().get(&id).map(|j| j.req.clone()) else {
            return;
        };
        {
            let mut st = pair.st.lock();
            st.state = "busy";
            st.current_job = Some(id);
        }
        let from = pair.signer.address();
        let gas = match req.gas_limit {
            Some(g) => Some(g),
            None => {
                let call = TransactionRequest::default()
                    .from(from)
                    .to(req.to)
                    .input(req.data.clone().into())
                    .value(req.value);
                match chain
                    .retrying("eth_estimateGas", || async {
                        chain.rpc.estimate_gas(call.clone()).await
                    })
                    .await
                {
                    Ok(g) => Some(g * 12 / 10),
                    Err((message, data)) => {
                        self.fail_job(id, "simulation_reverted", &message, data);
                        None
                    }
                }
            }
        };
        if let Some(gas) = gas {
            loop {
                let nonce = pair.st.lock().next_nonce;
                if let Some(j) = self.jobs.lock().get_mut(&id) {
                    j.status = "sent";
                    j.signer = Some(from);
                    j.nonce = Some(nonce);
                    j.sent_at = Some(Utc::now());
                }
                match chain.send(pair, nonce, &req, gas).await {
                    Ok(r) => {
                        let cost =
                            U256::from(r.gas_used as u128 * r.effective_gas_price) + req.value;
                        {
                            let mut st = pair.st.lock();
                            st.next_nonce += 1;
                            st.balance = st.balance.saturating_sub(cost);
                        }
                        chain
                            .head
                            .fetch_max(r.block_number.unwrap_or(0), Ordering::Relaxed);
                        if let Some(j) = self.jobs.lock().get_mut(&id) {
                            j.status = "included";
                            j.outcome = Some(if r.status() { "success" } else { "reverted" });
                            j.tx_hash = Some(r.transaction_hash);
                            j.block_number = r.block_number;
                            j.block_hash = r.block_hash;
                            j.gas_used = Some(r.gas_used);
                            j.effective_gas_price = Some(r.effective_gas_price);
                            j.included_at = Some(Utc::now());
                            j.attempts.push(json!({"tx_hash": r.transaction_hash, "nonce": nonce, "purpose": "job", "status": "included",
                                "gas_limit": gas.to_string(), "max_fee_per_gas": MAX_FEE.to_string(), "max_priority_fee_per_gas": TIP.to_string(),
                                "created_at": Utc::now().to_rfc3339()}));
                        }
                        self.emit(id, "transaction.included");
                        if let Some(j) = self.jobs.lock().get_mut(&id) {
                            j.status = "confirmed";
                            j.confirmed_at = Some(Utc::now());
                        }
                        self.emit(id, "transaction.confirmed");
                        break;
                    }
                    Err(message) if message.to_lowercase().contains("insufficient funds") => {
                        if let Some(j) = self.jobs.lock().get_mut(&id) {
                            j.status = "queued";
                        }
                        self.ensure_funds(chain, pair).await;
                    }
                    Err(message) => {
                        self.fail_job(id, "invalid_tx", &message, None);
                        break;
                    }
                }
            }
        }

        let n = self.processed.fetch_add(1, Ordering::Relaxed) + 1;
        if self.bug == Bug::DoubleSend
            && n.is_multiple_of(7)
            && gas.is_some()
            && chain.pairs.len() > 1
        {
            // Hand the very same payload to a *different* signer, skipping simulation.
            let me = chain
                .pairs
                .iter()
                .position(|p| Arc::ptr_eq(p, pair))
                .unwrap_or(0);
            let other = &chain.pairs[(me + 1) % chain.pairs.len()];
            other.ghosts.lock().push_back(Req {
                gas_limit: Some(150_000),
                ..req
            });
            chain.notify.notify_waiters();
        }
        let low = {
            let mut st = pair.st.lock();
            st.current_job = None;
            st.state = "idle";
            st.balance < chain.min_balance
        };
        if low {
            self.ensure_funds(chain, pair).await;
        }
    }

    async fn worker(self: Arc<Self>, chain: Arc<Chain>, pair: Arc<Pair>) {
        if pair.st.lock().balance < chain.min_balance {
            self.ensure_funds(&chain, &pair).await;
        }
        loop {
            let ghost = pair.ghosts.lock().pop_front();
            if let Some(g) = ghost {
                let nonce = pair.st.lock().next_nonce;
                if chain
                    .send(&pair, nonce, &g, g.gas_limit.unwrap_or(150_000))
                    .await
                    .is_ok()
                {
                    pair.st.lock().next_nonce += 1;
                }
                continue;
            }
            let next = chain.queue.lock().pop_front();
            match next {
                Some(id) => self.process(&chain, &pair, id).await,
                None => {
                    let _ =
                        tokio::time::timeout(Duration::from_millis(100), chain.notify.notified())
                            .await;
                }
            }
        }
    }
}

fn fee(j: &Job) -> Option<u128> {
    Some(j.gas_used? as u128 * j.effective_gas_price?)
}

// ------------------------------------------------------------------ HTTP

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitBody {
    chain_id: u64,
    to: String,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    value: Option<String>,
    #[serde(default)]
    gas_limit: Option<u64>,
    #[serde(default)]
    deadline: Option<String>,
    #[serde(default)]
    webhook: Option<WebhookBody>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookBody {
    url: String,
}

fn err(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
    (
        status,
        Json(json!({"error": {"code": code, "message": message.into()}})),
    )
        .into_response()
}

async fn submit(
    State(f): State<Arc<Fake>>,
    headers: HeaderMap,
    body: axum::body::Bytes,
) -> Response {
    let parsed: SubmitBody = match serde_json::from_slice(&body) {
        Ok(b) => b,
        Err(e) => return err(StatusCode::BAD_REQUEST, "invalid_request", e.to_string()),
    };
    let Some(chain) = f.chains.get(&parsed.chain_id) else {
        return err(
            StatusCode::BAD_REQUEST,
            "unsupported_chain",
            format!("chain {} is not configured", parsed.chain_id),
        );
    };
    let to: Address = match parsed.to.parse() {
        Ok(a) => a,
        Err(_) => return err(StatusCode::BAD_REQUEST, "invalid_request", "bad `to`"),
    };
    let data = match hex::decode(
        parsed
            .data
            .as_deref()
            .unwrap_or("0x")
            .trim_start_matches("0x"),
    ) {
        Ok(d) => Bytes::from(d),
        Err(_) => return err(StatusCode::BAD_REQUEST, "invalid_request", "bad `data`"),
    };
    let value: U256 = match parsed.value.as_deref().unwrap_or("0").parse() {
        Ok(v) => v,
        Err(_) => return err(StatusCode::BAD_REQUEST, "invalid_request", "bad `value`"),
    };
    let key = headers
        .get("idempotency-key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    if key.as_ref().map(|k| k.len() > 128).unwrap_or(false) {
        return err(
            StatusCode::BAD_REQUEST,
            "invalid_request",
            "Idempotency-Key too long",
        );
    }
    let canonical = serde_json::from_slice::<Value>(&body)
        .map(|v| v.to_string())
        .unwrap_or_default();
    let id = Uuid::now_v7();
    if let Some(k) = &key {
        let mut idem = f.idem.lock();
        if let Some((prev_body, prev_id)) = idem.get(k) {
            return if *prev_body == canonical {
                (
                    StatusCode::OK,
                    Json(json!({"job_id": prev_id, "replayed": true})),
                )
                    .into_response()
            } else {
                err(
                    StatusCode::CONFLICT,
                    "idempotency_conflict",
                    "same Idempotency-Key, different body",
                )
            };
        }
        idem.insert(k.clone(), (canonical, id));
    }
    let seq_no = f.accepted.fetch_add(1, Ordering::Relaxed) + 1;
    f.jobs.lock().insert(
        id,
        Job {
            seq_no,
            id,
            chain_id: parsed.chain_id,
            req: Req {
                to,
                data,
                value,
                gas_limit: parsed.gas_limit,
                deadline: parsed.deadline,
                webhook_url: parsed.webhook.map(|w| w.url),
            },
            status: "queued",
            outcome: None,
            signer: None,
            nonce: None,
            tx_hash: None,
            block_number: None,
            block_hash: None,
            gas_used: None,
            effective_gas_price: None,
            error: None,
            attempts: Vec::new(),
            webhooks: Vec::new(),
            next_sequence: 1,
            created_at: Utc::now(),
            sent_at: None,
            included_at: None,
            confirmed_at: None,
            failed_at: None,
        },
    );
    if !(f.bug == Bug::LoseJob && seq_no.is_multiple_of(11)) {
        chain.queue.lock().push_back(id);
        chain.notify.notify_one();
    }
    (StatusCode::ACCEPTED, Json(json!({"job_id": id}))).into_response()
}

async fn status(State(f): State<Arc<Fake>>, Path(id): Path<String>) -> Response {
    let Ok(id) = id.parse::<Uuid>() else {
        return err(StatusCode::NOT_FOUND, "not_found", "no such job");
    };
    let jobs = f.jobs.lock();
    let Some(j) = jobs.get(&id) else {
        return err(StatusCode::NOT_FOUND, "not_found", "no such job");
    };
    let ts = |t: &Option<DateTime<Utc>>| t.map(|t| t.to_rfc3339());
    Json(json!({
        "job_id": j.id, "chain_id": j.chain_id, "status": j.status, "outcome": j.outcome,
        "request": {"to": j.req.to, "data": j.req.data, "value": j.req.value.to_string(), "gas_limit": j.req.gas_limit,
                    "deadline": j.req.deadline, "webhook_url": j.req.webhook_url},
        "signer": j.signer, "nonce": j.nonce, "tx_hash": j.tx_hash, "block_number": j.block_number, "block_hash": j.block_hash,
        "gas_used": j.gas_used.map(|g| g.to_string()), "effective_gas_price": j.effective_gas_price.map(|g| g.to_string()),
        "fee_paid": fee(j).map(|x| x.to_string()), "l1_fee": null, "error": j.error,
        "attempts": j.attempts, "webhooks": j.webhooks,
        "timestamps": {"created_at": j.created_at.to_rfc3339(), "sent_at": ts(&j.sent_at), "included_at": ts(&j.included_at),
                       "confirmed_at": ts(&j.confirmed_at), "failed_at": ts(&j.failed_at)},
    }))
    .into_response()
}

#[derive(Default, Clone, Copy)]
struct C {
    queued: u64,
    in_flight: u64,
    included: u64,
    succeeded: u64,
    reverted: u64,
    failed: u64,
}
impl C {
    fn add(&mut self, j: &Job) {
        match (j.status, j.outcome) {
            ("queued", _) => self.queued += 1,
            ("sent", _) => self.in_flight += 1,
            ("included", _) => self.included += 1,
            ("confirmed", Some("reverted")) => self.reverted += 1,
            ("confirmed", _) => self.succeeded += 1,
            _ => self.failed += 1,
        }
    }
    fn json(&self, queued: bool) -> Value {
        let q = if queued { self.queued } else { 0 };
        json!({"queued": q, "processing": 0, "in_flight": self.in_flight, "included": self.included, "succeeded": self.succeeded,
               "reverted": self.reverted, "failed": self.failed,
               "total": q + self.in_flight + self.included + self.succeeded + self.reverted + self.failed})
    }
}

async fn analytics(State(f): State<Arc<Fake>>) -> Json<Value> {
    let jobs = f.jobs.lock();
    let mut totals = C::default();
    let mut by_chain: BTreeMap<u64, C> = f.chains.keys().map(|k| (*k, C::default())).collect();
    let mut by_signer: BTreeMap<Address, C> = BTreeMap::new();
    let mut by_cs: BTreeMap<(u64, Address), C> = BTreeMap::new();
    for j in jobs.values() {
        totals.add(j);
        by_chain.entry(j.chain_id).or_default().add(j);
        if let Some(s) = j.signer {
            by_signer.entry(s).or_default().add(j);
            by_cs.entry((j.chain_id, s)).or_default().add(j);
        }
    }
    if f.bug == Bug::WrongAnalytics && totals.succeeded > 0 {
        totals.succeeded += 1;
    }
    let with = |mut v: Value, extra: Value| {
        v.as_object_mut()
            .expect("object")
            .extend(extra.as_object().expect("object").clone());
        v
    };
    Json(json!({
        "totals": totals.json(true),
        "by_chain": by_chain.iter().map(|(k, c)| with(c.json(true), json!({"chain_id": k}))).collect::<Vec<_>>(),
        "by_signer": by_signer.iter().map(|(k, c)| with(c.json(false), json!({"signer": k}))).collect::<Vec<_>>(),
        "by_chain_signer": by_cs.iter().map(|((ch, s), c)| with(c.json(false), json!({"chain_id": ch, "signer": s}))).collect::<Vec<_>>(),
    }))
}

async fn signers(State(f): State<Arc<Fake>>) -> Json<Value> {
    let mut pairs = Vec::new();
    for c in f.chains.values() {
        for p in c.pairs.iter().chain(std::iter::once(&c.treasury)) {
            let st = p.st.lock();
            pairs.push(json!({"chain_id": c.id, "signer": p.signer.address(), "role": p.role, "state": st.state, "pause": st.pause,
                              "current_job": st.current_job, "next_nonce": st.next_nonce}));
        }
    }
    Json(json!({"pairs": pairs}))
}

async fn balances(State(f): State<Arc<Fake>>) -> Json<Value> {
    let mut chains = Vec::new();
    for c in f.chains.values() {
        let bal = |a: Address, confirmed: U256, min: U256| {
            json!({"address": a, "confirmed": confirmed.to_string(), "reserved": "0", "available": confirmed.to_string(),
                   "min_balance": min.to_string(), "low": confirmed < min})
        };
        let mut signers = Vec::new();
        for p in &c.pairs {
            let a = p.signer.address();
            signers.push(bal(a, c.balance(a).await, c.min_balance));
        }
        let t = c.treasury.signer.address();
        chains.push(
            json!({"chain_id": c.id, "name": c.name, "last_reconciled_at": Utc::now().to_rfc3339(),
                           "treasury": bal(t, c.balance(t).await, U256::ZERO), "signers": signers}),
        );
    }
    Json(json!({"chains": chains}))
}

async fn chains_route(State(f): State<Arc<Fake>>) -> Json<Value> {
    let v: Vec<Value> = f
        .chains
        .values()
        .map(|c| {
            let by_method = c.rpc_counts.lock().clone();
            let calls: u64 = by_method.values().sum();
            json!({"chain_id": c.id, "name": c.name, "kind": "geth", "status": "healthy", "head_number": c.head.load(Ordering::Relaxed),
                   "last_observed_at": Utc::now().to_rfc3339(), "send_mode": "sync", "queue_depth": c.queue.lock().len(),
                   "rpc": {"calls": calls, "errors": c.rpc_errors.load(Ordering::Relaxed), "credits_used": calls * 20,
                           "credits_projected_month": 0, "by_method": by_method}})
        })
        .collect();
    Json(json!({"chains": v}))
}

async fn gas(State(f): State<Arc<Fake>>) -> Json<Value> {
    let jobs = f.jobs.lock();
    let mut by_chain: BTreeMap<u64, (u64, u128, u128)> = BTreeMap::new();
    for j in jobs.values() {
        if let (Some(g), Some(fee)) = (j.gas_used, fee(j)) {
            let e = by_chain.entry(j.chain_id).or_default();
            e.0 += 1;
            e.1 += g as u128;
            e.2 += fee;
        }
    }
    let rows: Vec<Value> = by_chain
        .iter()
        .map(|(k, (n, g, fee))| {
            let gasj = json!({"tx_count": n, "gas_used": g.to_string(), "fee_paid": fee.to_string()});
            json!({"chain_id": k, "tx_count": n, "gas_used": g.to_string(), "fee_paid": fee.to_string(), "by_purpose": {"job": gasj}})
        })
        .collect();
    Json(json!({"by_chain": rows, "by_signer": [], "by_chain_signer": []}))
}

async fn readyz(State(f): State<Arc<Fake>>) -> Json<Value> {
    let chains: BTreeMap<String, &str> = f
        .chains
        .keys()
        .map(|k| (k.to_string(), "healthy"))
        .collect();
    Json(json!({"leader": true, "db": true, "chains": chains}))
}

async fn metrics(State(f): State<Arc<Fake>>) -> String {
    format!(
        "# TYPE gum_fake_jobs_accepted counter\ngum_fake_jobs_accepted {}\n# TYPE gum_fake_webhooks_sent counter\ngum_fake_webhooks_sent {}\n",
        f.accepted.load(Ordering::Relaxed),
        f.webhooks_sent.load(Ordering::Relaxed)
    )
}

// ------------------------------------------------------------------ boot

pub async fn run(bug: Option<String>) -> Result<()> {
    let bug = match bug.as_deref() {
        None | Some("none") => Bug::None,
        Some("double-send") => Bug::DoubleSend,
        Some("skip-webhook") => Bug::SkipWebhook,
        Some("lose-job") => Bug::LoseJob,
        Some("bad-signature") => Bug::BadSignature,
        Some("wrong-analytics") => Bug::WrongAnalytics,
        Some(other) => bail!("unknown --bug {other:?} (double-send | skip-webhook | lose-job | bad-signature | wrong-analytics)"),
    };
    let path = std::env::var("GUM_CONFIG").unwrap_or_else(|_| "config/default.toml".into());
    let text =
        std::fs::read_to_string(&path).with_context(|| format!("reading GUM_CONFIG {path}"))?;
    let cfg: Cfg = toml::from_str(&text).with_context(|| format!("parsing {path}"))?;
    let port: u16 = match std::env::var("PORT") {
        Ok(p) => p.parse().context("PORT")?,
        Err(_) => cfg.server.port.unwrap_or(8080),
    };

    let mut chains = BTreeMap::new();
    for (name, c) in &cfg.chains {
        let rpc = provider(&c.rpc_url)?;
        let mk = |key: &str, role: &'static str| -> Result<Arc<Pair>> {
            Ok(Arc::new(Pair {
                signer: key
                    .parse()
                    .map_err(|e| anyhow!("bad private key in config: {e}"))?,
                role,
                st: Mutex::new(PairState {
                    state: "idle",
                    pause: None,
                    current_job: None,
                    next_nonce: 0,
                    balance: U256::ZERO,
                }),
                ghosts: Mutex::new(VecDeque::new()),
            }))
        };
        let pairs = cfg
            .signers
            .local_private_keys
            .iter()
            .map(|k| mk(k, "signer"))
            .collect::<Result<Vec<_>>>()?;
        let chain = Arc::new(Chain {
            id: c.chain_id,
            name: name.clone(),
            rpc,
            queue: Mutex::new(VecDeque::new()),
            notify: Notify::new(),
            pairs,
            treasury: mk(&c.treasury_private_key, "treasury")?,
            treasury_lock: tokio::sync::Mutex::new(()),
            min_balance: c.signer_min_balance.parse().context("signer_min_balance")?,
            topup: c.topup_amount.parse().context("topup_amount")?,
            head: AtomicU64::new(0),
            rpc_counts: Mutex::new(BTreeMap::new()),
            rpc_errors: AtomicU64::new(0),
        });
        for p in chain.pairs.iter().chain(std::iter::once(&chain.treasury)) {
            let a = p.signer.address();
            let nonce = chain
                .retrying("eth_getTransactionCount", || async {
                    chain.rpc.get_transaction_count(a).await
                })
                .await
                .map_err(|(m, _)| anyhow!("eth_getTransactionCount: {m}"))?;
            let balance = chain.balance(a).await;
            let mut st = p.st.lock();
            st.next_nonce = nonce;
            st.balance = balance;
        }
        chains.insert(c.chain_id, chain);
    }

    let fake = Arc::new(Fake {
        bug,
        secret: cfg.webhook.signing_secret.clone(),
        http: reqwest::Client::builder()
            .pool_max_idle_per_host(256)
            .build()?,
        jobs: Mutex::new(HashMap::new()),
        idem: Mutex::new(HashMap::new()),
        chains,
        accepted: AtomicU64::new(0),
        processed: AtomicU64::new(0),
        webhooks_sent: AtomicU64::new(0),
    });
    for chain in fake.chains.values() {
        for pair in &chain.pairs {
            tokio::spawn(fake.clone().worker(chain.clone(), pair.clone()));
        }
    }

    let app = Router::new()
        .route("/healthz", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .route("/v1/transactions", post(submit))
        .route("/v1/transactions/{id}", get(status))
        .route("/v1/signers", get(signers))
        .route("/v1/signers/balances", get(balances))
        .route("/v1/chains", get(chains_route))
        .route("/v1/analytics/transactions", get(analytics))
        .route("/v1/analytics/gas", get(gas))
        .with_state(fake);
    let listener = tokio::net::TcpListener::bind(("127.0.0.1", port))
        .await
        .with_context(|| format!("binding port {port}"))?;
    println!(
        "{}",
        json!({"level": "info", "msg": "fake-engine listening", "port": port, "bug": format!("{bug:?}")})
    );
    axum::serve(listener, app).await?;
    Ok(())
}
