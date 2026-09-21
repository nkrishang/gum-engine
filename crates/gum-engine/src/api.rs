//! HTTP API. See `docs/api-contract.md` for the contract; this file is its implementation.
//!
//! There is no authentication by design: the service is only reachable on the private network. Request
//! validation is strict (unknown fields are rejected, stateless transaction rules are checked here) so
//! that nothing a signer would have to reject ever reaches one.

use std::{
    collections::BTreeMap,
    str::FromStr,
    sync::atomic::Ordering,
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, Bytes, U256};
use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::{
    chain::adapter::{FeeQuote, TxShape},
    domain::{addr_hex, hash_hex, JobRequest, PairKey, PairRole, PauseReason, QueuedJob, RecoveryStep},
    engine::Engine,
    funds::treasury,
    queue::Admit,
    signer::estimate_encoded_len,
    stats::Bucket,
    store::{
        batcher::IngestError,
        jobs::{InsertOutcome, NewJob},
    },
    webhook,
};

pub fn router(engine: Arc<Engine>) -> Router {
    let body_limit = engine.cfg.server.body_limit_bytes;
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_text))
        .route("/v1/transactions", post(submit))
        .route("/v1/transactions/{job_id}", get(get_transaction))
        .route("/v1/signers", get(signers))
        .route("/v1/signers/balances", get(balances))
        .route("/v1/chains", get(chains))
        .route("/v1/analytics/transactions", get(analytics_transactions))
        .route("/v1/analytics/gas", get(analytics_gas))
        .route("/v1/admin/pairs/{chain_id}/{signer}/{action}", post(admin_pair))
        .route("/v1/admin/chains/{chain_id}/{action}", post(admin_chain))
        .route("/v1/admin/pause", post(admin_pause_all))
        .route("/v1/admin/resume", post(admin_resume_all))
        .route("/v1/admin/jobs/{job_id}/cancel", post(admin_cancel_job))
        .route("/v1/admin/webhooks/{event_id}/redeliver", post(admin_redeliver))
        .layer(tower_http::limit::RequestBodyLimitLayer::new(body_limit))
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
        .with_state(engine)
}

struct ApiError {
    status: StatusCode,
    code: &'static str,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
        Self { status, code, message: message.into() }
    }
    fn invalid(message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, "invalid_request", message)
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, "not_found", message)
    }
    fn unavailable(code: &'static str, message: impl Into<String>) -> Self {
        Self::new(StatusCode::SERVICE_UNAVAILABLE, code, message)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.status, Json(json!({"error": {"code": self.code, "message": self.message}}))).into_response()
    }
}

// ---- health ---------------------------------------------------------------------------------------

/// Liveness only. Deliberately independent of leadership: Railway waits for this before it stops the
/// previous deployment, and the previous deployment is what holds the lease.
async fn healthz() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

async fn readyz(State(engine): State<Arc<Engine>>) -> Response {
    let db = engine.store.ping().await;
    let leader = engine.is_leader();
    let chains: BTreeMap<String, Value> = engine.chains.values().map(|c| (c.chain_id.to_string(), json!(c.status()))).collect();
    // Ready = able to process: every chain has its pairs, and none of them is still reconciling at boot.
    let booting: Vec<String> = engine
        .chains
        .values()
        .filter(|c| !c.started.load(Ordering::Relaxed) || engine.pairs_on(c.chain_id).iter().any(|p| p.pause_reason() == Some(PauseReason::Booting)))
        .map(|c| c.chain_id.to_string())
        .collect();
    let status = if db && leader && booting.is_empty() { StatusCode::OK } else { StatusCode::SERVICE_UNAVAILABLE };
    (status, Json(json!({"leader": leader, "db": db, "chains": chains, "booting": booting}))).into_response()
}

async fn metrics_text(State(engine): State<Arc<Engine>>) -> String {
    engine.prometheus.render()
}

// ---- submit ---------------------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct SubmitBody {
    chain_id: u64,
    to: String,
    #[serde(default)]
    data: Option<String>,
    #[serde(default)]
    value: Option<Value>,
    #[serde(default)]
    gas_limit: Option<Value>,
    #[serde(default)]
    deadline: Option<DateTime<Utc>>,
    webhook: WebhookBody,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct WebhookBody {
    url: String,
}

fn parse_amount(v: &Value, field: &str) -> Result<U256, ApiError> {
    let parsed = match v {
        Value::String(s) => match s.strip_prefix("0x") {
            Some(hex) => U256::from_str_radix(hex, 16).ok(),
            None => U256::from_str_radix(s, 10).ok(),
        },
        Value::Number(n) => n.as_u64().map(U256::from),
        _ => None,
    };
    parsed.ok_or_else(|| ApiError::invalid(format!("`{field}` must be a non-negative integer (decimal string, 0x-hex string or number)")))
}

/// Intrinsic gas of a call (EIP-2028 calldata pricing). Anything below this can never be mined, so it is
/// refused here instead of being discovered by a signer.
fn intrinsic_gas(data: &[u8]) -> u64 {
    let zeros = data.iter().filter(|b| **b == 0).count() as u64;
    21_000 + zeros * 4 + (data.len() as u64 - zeros) * 16
}

async fn submit(State(engine): State<Arc<Engine>>, headers: HeaderMap, body: axum::body::Bytes) -> Result<Response, ApiError> {
    let started = Instant::now();
    let parsed: SubmitBody = serde_json::from_slice(&body).map_err(|e| ApiError::invalid(format!("invalid request body: {e}")))?;
    let chain = engine.chain(parsed.chain_id).ok_or_else(|| ApiError::new(StatusCode::BAD_REQUEST, "unsupported_chain", format!("chain {} is not configured", parsed.chain_id)))?;

    let to = Address::from_str(&parsed.to).map_err(|_| ApiError::invalid("`to` must be a 20-byte 0x-hex address"))?;
    let data = match parsed.data.as_deref() {
        None | Some("") | Some("0x") => Bytes::new(),
        Some(s) => Bytes::from(hex::decode(s.strip_prefix("0x").unwrap_or(s)).map_err(|_| ApiError::invalid("`data` must be 0x-hex"))?),
    };
    let value = parsed.value.as_ref().map(|v| parse_amount(v, "value")).transpose()?.unwrap_or(U256::ZERO);
    let gas_limit = match &parsed.gas_limit {
        None | Some(Value::Null) => None,
        Some(v) => Some(u64::try_from(parse_amount(v, "gas_limit")?).map_err(|_| ApiError::invalid("`gas_limit` is too large"))?),
    };
    if let Some(gas) = gas_limit {
        let floor = intrinsic_gas(&data);
        if gas < floor {
            return Err(ApiError::invalid(format!("`gas_limit` {gas} is below the intrinsic gas of this call ({floor})")));
        }
        if gas > chain.tunables.max_tx_gas {
            return Err(ApiError::invalid(format!("`gas_limit` {gas} exceeds the chain's per-transaction limit ({})", chain.tunables.max_tx_gas)));
        }
        // One job must never be able to pause a whole chain's signers: cap what it may cost up front.
        let fees = FeeQuote {
            max_fee_per_gas: chain.adapter.fee_quote(chain.last_base_fee().unwrap_or(0), &chain.tunables).max_fee_per_gas,
            max_priority_fee_per_gas: chain.tunables.priority_fee_wei,
        };
        let cost = chain.adapter.worst_case_cost(&TxShape { gas_limit: gas, value, encoded_len: estimate_encoded_len(data.len()) }, &fees, &chain.cost_context());
        if cost > chain.cfg.max_job_cost() {
            return Err(ApiError::new(
                StatusCode::BAD_REQUEST,
                "job_too_expensive",
                format!("worst-case cost {cost} wei exceeds this chain's max_job_cost ({} wei)", chain.cfg.max_job_cost()),
            ));
        }
    } else if value > chain.cfg.max_job_cost() {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, "job_too_expensive", format!("`value` exceeds this chain's max_job_cost ({} wei)", chain.cfg.max_job_cost())));
    }
    if data.len() > 120_000 {
        return Err(ApiError::invalid("`data` exceeds 120000 bytes"));
    }
    if parsed.deadline.is_some_and(|d| d <= Utc::now()) {
        return Err(ApiError::invalid("`deadline` is in the past"));
    }
    webhook::validate_url(&parsed.webhook.url, &engine.cfg.webhook, engine.cfg.server.port).map_err(ApiError::invalid)?;

    let idempotency_key = match headers.get("idempotency-key") {
        None => None,
        Some(v) => {
            let key = v.to_str().map_err(|_| ApiError::invalid("Idempotency-Key must be ASCII"))?.trim();
            if key.is_empty() || key.len() > 128 {
                return Err(ApiError::invalid("Idempotency-Key must be 1-128 characters"));
            }
            Some(key.to_string())
        }
    };

    // Backpressure, per chain: a flood on one chain must not take ingest away from the others.
    if engine.stats.queued(chain.chain_id) as usize >= engine.cfg.queue.max_depth {
        return Err(ApiError::unavailable("queue_full", format!("chain {} has {} jobs queued; retry later", chain.chain_id, engine.cfg.queue.max_depth)));
    }
    let cap = engine.cfg.rpc.daily_credit_cap;
    if cap > 0 && engine.chains.values().map(|c| c.rpc.meter.credits_today()).sum::<u64>() >= cap {
        return Err(ApiError::unavailable("shedding", "daily RPC credit cap reached; new jobs are refused until the next UTC day"));
    }

    let request = JobRequest { chain_id: chain.chain_id, to, data, value, gas_limit, deadline: parsed.deadline, webhook_url: parsed.webhook.url };
    // Canonical form, so the same logical request hashes the same regardless of JSON formatting.
    let canonical = format!(
        "{}|{}|0x{}|{}|{:?}|{:?}|{}",
        request.chain_id,
        addr_hex(&request.to),
        hex::encode(&request.data),
        request.value,
        request.gas_limit,
        request.deadline,
        request.webhook_url
    );
    let request_hash = hex::encode(Sha256::digest(canonical.as_bytes()));
    let job_id = Uuid::now_v7();
    let new_job = NewJob { id: job_id, request: request.clone(), idempotency_key, request_hash };

    // Durable before acknowledged — and counted exactly once even if leadership changes right now.
    let gate = engine.ingest_gate.read().await;
    let outcome = engine.ingest.submit(new_job).await.map_err(|e| match e {
        IngestError::Overloaded => ApiError::unavailable("queue_full", "ingest is saturated; retry shortly"),
        IngestError::Unavailable(detail) => {
            if let Some(suppressed) = crate::telemetry::throttled("api.store_unavailable", Duration::from_secs(10)) {
                tracing::error!(event = "api.store_unavailable", error = %detail, suppressed, "cannot persist incoming jobs");
            }
            ApiError::unavailable("store_unavailable", "the job could not be persisted; retry")
        }
    })?;

    let response = match outcome {
        InsertOutcome::Created => {
            if engine.is_leader() {
                let queued = QueuedJob { id: job_id, request, requeue_count: 0, created_at: Utc::now(), enqueued_at: started };
                // `Deferred` (window full) and the standby case are both picked up by the leader's sweep.
                if chain.queue.admit(queued) == Admit::Queued {
                    engine.stats.add(chain.chain_id, None, Bucket::Queued, 1);
                }
            }
            metrics::counter!("gum_jobs_accepted_total", "chain" => chain.name.clone()).increment(1);
            tracing::debug!(event = "job.accepted", chain = chain.chain_id, job_id = %job_id, "job accepted");
            (StatusCode::ACCEPTED, Json(json!({"job_id": job_id}))).into_response()
        }
        InsertOutcome::Replayed(original) => (StatusCode::OK, Json(json!({"job_id": original, "replayed": true}))).into_response(),
        InsertOutcome::Conflict => return Err(ApiError::new(StatusCode::CONFLICT, "idempotency_conflict", "this Idempotency-Key was already used with a different request body")),
    };
    drop(gate);
    metrics::histogram!("gum_accept_latency_seconds").record(started.elapsed().as_secs_f64());
    Ok(response)
}

// ---- job status -----------------------------------------------------------------------------------

async fn get_transaction(State(engine): State<Arc<Engine>>, Path(job_id): Path<String>) -> Result<Json<Value>, ApiError> {
    let job_id = Uuid::parse_str(&job_id).map_err(|_| ApiError::not_found("no such job"))?;
    let store_err = |e: crate::error::StoreError| ApiError::unavailable("store_unavailable", format!("cannot read job: {}", e.code()));
    let job = engine.store.get_job(job_id).await.map_err(store_err)?.ok_or_else(|| ApiError::not_found("no such job"))?;
    let attempts = engine.store.attempts_for_job(job_id).await.map_err(store_err)?;
    let deliveries = engine.store.deliveries_for_job(job_id).await.map_err(store_err)?;

    let error =
        job.error_code.as_ref().map(|code| json!({"code": code, "message": job.error_message, "revert_data": job.revert_data.as_ref().map(|d| format!("0x{}", hex::encode(d)))}));
    Ok(Json(json!({
        "job_id": job.id,
        "chain_id": job.chain_id,
        "status": job.status,
        "outcome": job.outcome,
        "request": {
            "to": job.to_addr,
            "data": format!("0x{}", hex::encode(&job.data)),
            "value": job.value,
            "gas_limit": job.gas_limit,
            "deadline": job.deadline,
            "webhook_url": job.webhook_url,
        },
        "signer": job.signer,
        "nonce": job.nonce,
        "tx_hash": job.tx_hash,
        "block_number": job.block_number,
        "block_hash": job.block_hash,
        "gas_used": job.gas_used,
        "effective_gas_price": job.effective_gas_price,
        "fee_paid": job.fee_paid,
        "l1_fee": job.l1_fee,
        "error": error,
        "attempts": attempts.iter().map(|a| json!({
            "tx_hash": hash_hex(&a.tx_hash),
            "nonce": a.nonce,
            "purpose": a.purpose,
            "status": a.status,
            "gas_limit": a.gas_limit.to_string(),
            "max_fee_per_gas": a.max_fee_per_gas.to_string(),
            "max_priority_fee_per_gas": a.max_priority_fee_per_gas.to_string(),
            "created_at": a.created_at,
        })).collect::<Vec<_>>(),
        "webhooks": deliveries.iter().map(|d| json!({
            "event": d.event,
            "sequence": d.sequence,
            "status": d.status,
            "attempts": d.attempts,
            "last_status_code": d.last_status_code,
            "delivered_at": d.delivered_at,
        })).collect::<Vec<_>>(),
        "timestamps": {
            "created_at": job.created_at,
            "sent_at": job.sent_at,
            "included_at": job.included_at,
            "confirmed_at": job.confirmed_at,
            "failed_at": job.failed_at,
        },
    })))
}

// ---- observability --------------------------------------------------------------------------------

async fn signers(State(engine): State<Arc<Engine>>) -> Json<Value> {
    let pairs: Vec<Value> = engine
        .pairs
        .read()
        .values()
        .map(|p| {
            let v = p.view();
            let state = if v.pause.is_some() {
                "paused"
            } else if v.draining {
                "draining"
            } else if v.current.is_some() {
                "busy"
            } else {
                "idle"
            };
            json!({
                "chain_id": p.key.chain_id,
                "signer": addr_hex(&p.key.signer),
                "role": p.role,
                "state": state,
                "pause": v.pause,
                "current_job": v.current,
                "next_nonce": v.next_nonce,
            })
        })
        .collect();
    Json(json!({"pairs": pairs}))
}

async fn balances(State(engine): State<Arc<Engine>>) -> Json<Value> {
    let chains: Vec<Value> = engine
        .chains
        .values()
        .map(|chain| {
            let bal = |p: &Arc<crate::engine::Pair>, floor: U256| {
                let view = p.ledger.view();
                let available = treasury::spendable(p);
                json!({
                    "address": addr_hex(&p.key.signer),
                    "confirmed": view.confirmed.to_string(),
                    "reserved": view.reserved.to_string(),
                    "available": available.to_string(),
                    "min_balance": floor.to_string(),
                    "low": view.initialised && available < floor,
                    "known": view.initialised,
                })
            };
            let pairs = engine.pairs_on(chain.chain_id);
            let treasury = pairs.iter().find(|p| p.role == PairRole::Treasury).map(|p| bal(p, chain.cfg.treasury_min_balance));
            let signers: Vec<Value> = pairs.iter().filter(|p| p.role == PairRole::Signer).map(|p| bal(p, chain.cfg.signer_min_balance)).collect();
            json!({"chain_id": chain.chain_id, "name": chain.name, "treasury": treasury, "signers": signers})
        })
        .collect();
    Json(json!({"chains": chains}))
}

async fn chains(State(engine): State<Arc<Engine>>) -> Json<Value> {
    let chains: Vec<Value> = engine
        .chains
        .values()
        .map(|c| {
            let h = c.health();
            json!({
                "chain_id": c.chain_id,
                "name": c.name,
                "kind": c.adapter.kind(),
                "status": h.status,
                "operator_paused": c.operator_paused.load(Ordering::Relaxed),
                "head_number": h.head_number,
                "last_observed_at": h.last_observed_at,
                "send_mode": c.send_mode(),
                "confirmation_delay_ms": c.tunables.confirmation_delay_ms,
                "queue_depth": engine.stats.queued(c.chain_id),
                "rpc": c.rpc.meter.snapshot(),
                "rpc_circuit_open": c.rpc.is_circuit_open(),
            })
        })
        .collect();
    Json(json!({"chains": chains, "leader": engine.is_leader(), "global_pause": engine.global_pause.load(Ordering::Relaxed)}))
}

async fn analytics_transactions(State(engine): State<Arc<Engine>>) -> Json<Value> {
    Json(serde_json::to_value(engine.stats.snapshot()).unwrap_or(Value::Null))
}

async fn analytics_gas(State(engine): State<Arc<Engine>>) -> Result<Json<Value>, ApiError> {
    let rows = engine.store.gas_ledger().await.map_err(|e| ApiError::unavailable("store_unavailable", format!("cannot read gas ledger: {}", e.code())))?;

    #[derive(Default, Clone)]
    struct Gas {
        tx_count: i64,
        gas_used: U256,
        fee_paid: U256,
        by_purpose: BTreeMap<String, (i64, U256, U256)>,
    }
    impl Gas {
        fn add(&mut self, purpose: &str, n: i64, gas: U256, fee: U256) {
            self.tx_count += n;
            self.gas_used += gas;
            self.fee_paid += fee;
            let p = self.by_purpose.entry(purpose.to_string()).or_default();
            p.0 += n;
            p.1 += gas;
            p.2 += fee;
        }
        fn json(&self) -> Value {
            let by_purpose: BTreeMap<&String, Value> =
                self.by_purpose.iter().map(|(k, (n, g, f))| (k, json!({"tx_count": n, "gas_used": g.to_string(), "fee_paid": f.to_string()}))).collect();
            json!({"tx_count": self.tx_count, "gas_used": self.gas_used.to_string(), "fee_paid": self.fee_paid.to_string(), "by_purpose": by_purpose})
        }
    }

    let mut by_chain: BTreeMap<u64, Gas> = BTreeMap::new();
    let mut by_signer: BTreeMap<String, Gas> = BTreeMap::new();
    let mut by_both: BTreeMap<(u64, String), Gas> = BTreeMap::new();
    for r in rows {
        let gas = U256::from_str_radix(&r.gas_used, 10).unwrap_or_default();
        let fee = U256::from_str_radix(&r.fee_paid, 10).unwrap_or_default();
        by_chain.entry(r.chain_id).or_default().add(&r.purpose, r.tx_count, gas, fee);
        by_signer.entry(r.signer.clone()).or_default().add(&r.purpose, r.tx_count, gas, fee);
        by_both.entry((r.chain_id, r.signer)).or_default().add(&r.purpose, r.tx_count, gas, fee);
    }
    let with = |mut v: Value, extra: Value| {
        if let (Some(obj), Some(extra)) = (v.as_object_mut(), extra.as_object()) {
            for (k, val) in extra {
                obj.insert(k.clone(), val.clone());
            }
        }
        v
    };
    Ok(Json(json!({
        "by_chain": by_chain.iter().map(|(c, g)| with(g.json(), json!({"chain_id": c}))).collect::<Vec<_>>(),
        "by_signer": by_signer.iter().map(|(s, g)| with(g.json(), json!({"signer": s}))).collect::<Vec<_>>(),
        "by_chain_signer": by_both.iter().map(|((c, s), g)| with(g.json(), json!({"chain_id": c, "signer": s}))).collect::<Vec<_>>(),
    })))
}

// ---- admin ----------------------------------------------------------------------------------------

async fn admin_pair(State(engine): State<Arc<Engine>>, Path((chain_id, signer, action)): Path<(u64, String, String)>) -> Result<Json<Value>, ApiError> {
    let signer = Address::from_str(&signer).map_err(|_| ApiError::invalid("signer must be a 0x-hex address"))?;
    let pair = engine.pair(&PairKey { chain_id, signer }).ok_or_else(|| ApiError::not_found("no such (chain, signer) pair on this instance"))?;
    match action.as_str() {
        "pause" => {
            pair.pause(&engine.store, PauseReason::Manual, RecoveryStep::AwaitingOperatorResume, "paused by operator");
        }
        "resume" => {
            // An operator resume also clears pauses that were waiting for an operator.
            for reason in [PauseReason::Manual, PauseReason::StuckUnresolved, PauseReason::NonceDrift, PauseReason::SignerUnavailable] {
                pair.resume_if(&engine.store, reason);
            }
            pair.request_recovery();
        }
        "recover" => pair.request_recovery(),
        _ => return Err(ApiError::not_found("unknown action; use pause, resume or recover")),
    }
    tracing::warn!(event = "admin.pair", chain = chain_id, signer = %addr_hex(&signer), action = %action, "operator action on pair");
    Ok(Json(json!({"ok": true, "state": pair.view().pause})))
}

async fn admin_chain(State(engine): State<Arc<Engine>>, Path((chain_id, action)): Path<(u64, String)>) -> Result<Json<Value>, ApiError> {
    let chain = engine.chain(chain_id).ok_or_else(|| ApiError::not_found("no such chain"))?;
    match action.as_str() {
        "pause" => chain.operator_paused.store(true, Ordering::Relaxed),
        "resume" => {
            chain.operator_paused.store(false, Ordering::Relaxed);
            for pair in engine.pairs_on(chain_id) {
                pair.wake.notify_waiters();
            }
        }
        "probe" => chain.probe_now.notify_one(),
        _ => return Err(ApiError::not_found("unknown action; use pause, resume or probe")),
    }
    tracing::warn!(event = "admin.chain", chain = chain_id, action = %action, "operator action on chain");
    Ok(Json(json!({"ok": true})))
}

async fn admin_pause_all(State(engine): State<Arc<Engine>>) -> Json<Value> {
    engine.global_pause.store(true, Ordering::Relaxed);
    tracing::warn!(event = "admin.global_pause", "operator engaged the global kill switch: no new jobs will be picked up");
    Json(json!({"ok": true}))
}

async fn admin_resume_all(State(engine): State<Arc<Engine>>) -> Json<Value> {
    engine.global_pause.store(false, Ordering::Relaxed);
    for pair in engine.pairs.read().values() {
        pair.wake.notify_waiters();
    }
    tracing::warn!(event = "admin.global_resume", "operator released the global kill switch");
    Json(json!({"ok": true}))
}

async fn admin_cancel_job(State(engine): State<Arc<Engine>>, Path(job_id): Path<Uuid>) -> Result<Json<Value>, ApiError> {
    let job = engine.store.get_job(job_id).await.map_err(|e| ApiError::unavailable("store_unavailable", e.code()))?.ok_or_else(|| ApiError::not_found("no such job"))?;
    let chain = engine.chain(job.chain_id).ok_or_else(|| ApiError::not_found("job's chain is not configured"))?;
    // Out of memory first, so no worker can pick it up between the two steps.
    let was_waiting = chain.queue.remove(&job_id);
    let cancelled = engine.store.cancel_queued(job_id).await.map_err(|e| ApiError::unavailable("store_unavailable", e.code()))?;
    if cancelled {
        if was_waiting {
            engine.stats.transition(chain.chain_id, (None, Bucket::Queued), (None, Bucket::Failed));
        } else {
            engine.stats.add(chain.chain_id, None, Bucket::Failed, 1);
        }
        engine.webhook_wake.notify_one();
        tracing::warn!(event = "admin.job_cancelled", chain = chain.chain_id, job_id = %job_id, "operator cancelled a queued job");
        Ok(Json(json!({"ok": true})))
    } else {
        Err(ApiError::new(StatusCode::CONFLICT, "not_cancellable", "only jobs that are still queued can be cancelled"))
    }
}

async fn admin_redeliver(State(engine): State<Arc<Engine>>, Path(event_id): Path<Uuid>) -> Result<Json<Value>, ApiError> {
    let found = engine.store.redeliver(event_id).await.map_err(|e| ApiError::unavailable("store_unavailable", e.code()))?;
    if !found {
        return Err(ApiError::not_found("no such webhook event"));
    }
    engine.webhook_wake.notify_one();
    Ok(Json(json!({"ok": true})))
}
