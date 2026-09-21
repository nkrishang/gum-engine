//! The correctness verdict. Pure function over what the harness observed from the outside:
//! HTTP outcomes, final job statuses, webhook deliveries, the at-rest observability routes and the chain.
//!
//! Checks (ids match the README):
//!  a  every accepted job reached a terminal status
//!  b  on-chain execution: exactly once for confirmed jobs, zero for failed jobs, never twice for anyone
//!  c  nonces: nothing stuck in the mempool at rest, API (signer, nonce, tx_hash) agrees with the chain,
//!     no nonce claimed by two jobs, `/v1/signers.next_nonce` equals the on-chain nonce
//!  d  webhooks: coverage, contradiction-free, `sequence` consistent, signatures valid
//!  e  `/v1/analytics/transactions` totals equal ground truth at rest
//!  f  `/v1/signers/balances.confirmed` equals `eth_getBalance` at rest
//!  g  job-mix expectations (reverting with gas_limit → reverted on-chain; without → simulation_reverted)
//!  h  idempotency replays behave per contract
//!  i  API hygiene: no unexpected HTTP statuses for valid requests, observability routes parse

use std::collections::{BTreeMap, HashMap, HashSet};

use alloy::primitives::{Address, U256};
use serde::{Deserialize, Serialize};

use crate::api::{AnalyticsResp, BalancesResp, Counts, SignersResp};
use crate::loadgen::{JobKind, JobRecord, ReplayPlan};
use crate::oracle::{ChainFacts, ChainTx, TxKey};
use crate::sink::Delivery;

const MAX_IDS: usize = 50;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Violation {
    pub check: String,
    pub code: String,
    pub message: String,
    pub count: usize,
    /// Job ids (or another identifier when the job never got one); capped at 50.
    pub job_ids: Vec<String>,
    /// A few concrete examples with details.
    pub examples: Vec<String>,
}

#[derive(Default)]
pub struct Violations {
    map: BTreeMap<(String, String), Violation>,
}

impl Violations {
    pub fn add(
        &mut self,
        check: &str,
        code: &str,
        message: &str,
        id: impl Into<String>,
        detail: impl Into<String>,
    ) {
        let v = self
            .map
            .entry((check.to_string(), code.to_string()))
            .or_insert_with(|| Violation {
                check: check.to_string(),
                code: code.to_string(),
                message: message.to_string(),
                count: 0,
                job_ids: Vec::new(),
                examples: Vec::new(),
            });
        v.count += 1;
        let id = id.into();
        if v.job_ids.len() < MAX_IDS && !id.is_empty() && !v.job_ids.contains(&id) {
            v.job_ids.push(id);
        }
        let detail = detail.into();
        if v.examples.len() < 5 && !detail.is_empty() {
            v.examples.push(detail);
        }
    }
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
    pub fn into_vec(self) -> Vec<Violation> {
        self.map.into_values().collect()
    }
    pub fn extend(&mut self, other: Violations) {
        for (k, v) in other.map {
            match self.map.get_mut(&k) {
                None => {
                    self.map.insert(k, v);
                }
                Some(mine) => {
                    mine.count += v.count;
                    mine.job_ids.extend(
                        v.job_ids
                            .into_iter()
                            .take(MAX_IDS.saturating_sub(mine.job_ids.len())),
                    );
                    mine.examples.extend(
                        v.examples
                            .into_iter()
                            .take(5usize.saturating_sub(mine.examples.len())),
                    );
                }
            }
        }
    }
}

/// At-rest view of the engine's observability routes (`Err` = the route failed or did not parse).
pub struct AtRest {
    pub analytics: Result<AnalyticsResp, String>,
    pub analytics_baseline: Counts,
    pub signers: Result<SignersResp, String>,
    pub balances: Result<BalancesResp, String>,
}

pub struct Input<'a> {
    pub jobs: &'a [JobRecord],
    pub deliveries: &'a [Delivery],
    pub facts: &'a [ChainFacts],
    pub signer_addrs: &'a [Address],
    pub treasury: Address,
}

fn label(j: &JobRecord) -> String {
    j.job_id()
        .map(str::to_string)
        .unwrap_or_else(|| format!("bench#{}({:#x})", j.spec.idx, j.spec.bench_id))
}

/// On-chain executions attributed to each job index.
pub fn attribute<'a>(
    jobs: &[JobRecord],
    facts: &'a [ChainFacts],
) -> (HashMap<usize, Vec<&'a ChainTx>>, Vec<&'a ChainTx>) {
    let mut by_key: HashMap<(u64, TxKey), usize> = HashMap::new();
    for j in jobs {
        let key = match j.spec.kind {
            JobKind::Transfer => TxKey::Recipient(j.spec.to),
            _ => TxKey::Id(j.spec.bench_id),
        };
        by_key.insert((j.spec.chain_id, key), j.spec.idx);
    }
    let mut execs: HashMap<usize, Vec<&ChainTx>> = HashMap::new();
    let mut other = Vec::new();
    for f in facts {
        for tx in &f.txs {
            match by_key.get(&(f.chain_id, tx.key(f.target))) {
                Some(idx) => execs.entry(*idx).or_default().push(tx),
                None => other.push(tx),
            }
        }
    }
    (execs, other)
}

/// Checks that depend only on jobs, webhooks and the chain (a, b, c-partly, d, g, h, i).
pub fn evaluate(input: &Input) -> Violations {
    let mut v = Violations::default();
    let (execs, _) = attribute(input.jobs, input.facts);
    let facts_by_chain: HashMap<u64, &ChainFacts> =
        input.facts.iter().map(|f| (f.chain_id, f)).collect();

    let mut deliveries_by_job: HashMap<&str, Vec<&Delivery>> = HashMap::new();
    for d in input.deliveries {
        deliveries_by_job
            .entry(d.job_id.as_str())
            .or_default()
            .push(d);
    }

    let mut nonce_claims: HashMap<(u64, String, u64), Vec<String>> = HashMap::new();

    for j in input.jobs {
        let id = label(j);
        let ex = execs.get(&j.spec.idx).map(Vec::as_slice).unwrap_or(&[]);
        let n_success = ex.iter().filter(|t| t.success).count();
        let describe = || {
            ex.iter()
                .map(|t| {
                    format!(
                        "{:#x}(from {:#x}, nonce {}, ok={})",
                        t.hash, t.from, t.nonce, t.success
                    )
                })
                .collect::<Vec<_>>()
                .join(", ")
        };

        // ---- b: never twice for anyone, accepted or not.
        if ex.len() > 1 {
            v.add(
                "b",
                "executed_more_than_once",
                "a job's transaction was included on-chain more than once",
                &id,
                format!(
                    "{id}: {} txs on chain {}: {}",
                    ex.len(),
                    j.spec.chain_id,
                    describe()
                ),
            );
        }
        if let Some(f) = facts_by_chain.get(&j.spec.chain_id) {
            let logs = f.hit_logs.get(&j.spec.bench_id).copied().unwrap_or(0);
            if logs > 1 {
                v.add(
                    "b",
                    "hit_logged_more_than_once",
                    "BenchTarget emitted Hit more than once for one id",
                    &id,
                    format!("{id}: {logs} Hit logs"),
                );
            }
            if matches!(
                j.spec.kind,
                JobKind::HitGas | JobKind::HitNoGas | JobKind::BurnGas
            ) && logs as usize != n_success.min(1)
                && ex.len() <= 1
            {
                v.add(
                    "b",
                    "hit_log_mismatch",
                    "Hit logs disagree with the transaction scan",
                    &id,
                    format!("{id}: {logs} logs vs {n_success} successful txs"),
                );
            }
        }

        // ---- i: HTTP hygiene for valid requests.
        match j.submit.status {
            Some(202) | Some(503) => {}
            Some(200) if j.submit.replayed && j.attempts > 1 => {}
            Some(code) => v.add(
                "i",
                &format!("unexpected_http_{code}"),
                "a valid POST /v1/transactions got an unexpected status",
                &id,
                format!("{id}: HTTP {code} error_code={:?}", j.submit.error_code),
            ),
            None => {}
        }
        if matches!(j.submit.status, Some(202) | Some(200)) && j.submit.job_id.is_none() {
            v.add(
                "i",
                "accept_without_job_id",
                "2xx response without a job_id",
                &id,
                "",
            );
        }

        if !j.accepted() {
            if !j.indeterminate && !ex.is_empty() {
                v.add(
                    "b",
                    "rejected_but_executed",
                    "a job the API definitively rejected was executed on-chain",
                    &id,
                    describe(),
                );
            }
            continue;
        }

        // ---- h: idempotency replays.
        if let Some(r) = &j.replay {
            let res = &r.result;
            let inconclusive = res.transport_error.is_some() || res.status == Some(503);
            if !inconclusive {
                match r.plan {
                    ReplayPlan::Same => {
                        if !(res.status == Some(200)
                            && res.replayed
                            && res.job_id == j.submit.job_id)
                        {
                            v.add("h", "replay_not_recognised", "same Idempotency-Key + same body must return 200 replayed:true with the original job_id", &id,
                                  format!("{id}: got status={:?} replayed={} job_id={:?}", res.status, res.replayed, res.job_id));
                        }
                    }
                    ReplayPlan::Conflict => {
                        if !(res.status == Some(409)
                            && res.error_code.as_deref() == Some("idempotency_conflict"))
                        {
                            v.add("h", "conflict_not_detected", "same Idempotency-Key + different body must return 409 idempotency_conflict", &id,
                                  format!("{id}: got status={:?} code={:?}", res.status, res.error_code));
                        }
                    }
                    ReplayPlan::None => {}
                }
            }
        }

        // ---- a: terminal status.
        let Some(fs) = j.final_status.as_ref().filter(|s| s.is_terminal()) else {
            let seen = j
                .final_status
                .as_ref()
                .map(|s| s.status.clone())
                .or_else(|| j.final_poll_error.clone())
                .unwrap_or_else(|| "never polled".into());
            v.add(
                "a",
                "not_terminal",
                "an accepted job never reached confirmed/failed",
                &id,
                format!("{id}: last seen {seen}; on-chain txs: {}", ex.len()),
            );
            continue;
        };

        let confirmed = fs.status == "confirmed";
        // ---- b: exactly once / zero.
        if confirmed {
            if ex.is_empty() {
                v.add(
                    "b",
                    "confirmed_but_not_on_chain",
                    "job is confirmed but no transaction for it exists on-chain",
                    &id,
                    format!("{id}: api tx_hash={:?}", fs.tx_hash),
                );
            } else {
                let tx = ex[0];
                let api_success = fs.outcome.as_deref() == Some("success");
                if fs.outcome.is_none() {
                    v.add(
                        "b",
                        "confirmed_without_outcome",
                        "confirmed job has a null outcome",
                        &id,
                        "",
                    );
                } else if ex.len() == 1 && api_success != tx.success {
                    v.add(
                        "b",
                        "outcome_mismatch",
                        "API outcome disagrees with the receipt status on-chain",
                        &id,
                        format!("{id}: api={:?} chain_success={}", fs.outcome, tx.success),
                    );
                }
                // ---- c: API's view of the tx agrees with the chain.
                let api_hash = fs.tx_hash.as_deref().unwrap_or_default().to_lowercase();
                match ex.iter().find(|t| format!("{:#x}", t.hash) == api_hash) {
                    None => v.add(
                        "c",
                        "tx_hash_mismatch",
                        "API tx_hash is not the transaction that executed the job",
                        &id,
                        format!("{id}: api={:?} chain={}", fs.tx_hash, describe()),
                    ),
                    Some(t) => {
                        if fs.nonce != Some(t.nonce) {
                            v.add(
                                "c",
                                "nonce_mismatch",
                                "API nonce differs from the on-chain transaction's nonce",
                                &id,
                                format!("{id}: api={:?} chain={}", fs.nonce, t.nonce),
                            );
                        }
                        if !fs
                            .signer
                            .as_deref()
                            .map(|s| s.eq_ignore_ascii_case(&format!("{:#x}", t.from)))
                            .unwrap_or(false)
                        {
                            v.add(
                                "c",
                                "signer_mismatch",
                                "API signer differs from the on-chain sender",
                                &id,
                                format!("{id}: api={:?} chain={:#x}", fs.signer, t.from),
                            );
                        }
                        if fs.block_number != Some(t.block) {
                            v.add(
                                "c",
                                "block_mismatch",
                                "API block_number differs from the chain",
                                &id,
                                format!("{id}: api={:?} chain={}", fs.block_number, t.block),
                            );
                        }
                    }
                }
            }
            if let (Some(s), Some(n)) = (&fs.signer, fs.nonce) {
                nonce_claims
                    .entry((j.spec.chain_id, s.to_lowercase(), n))
                    .or_default()
                    .push(id.clone());
            }
        } else if !ex.is_empty() {
            v.add(
                "b",
                "failed_but_executed",
                "job is failed (\"never executed on-chain\") yet a transaction for it exists",
                &id,
                describe(),
            );
        }

        // ---- g: expectations of the mix.
        let (want_status, want_outcome, want_err) = match j.spec.kind {
            JobKind::FailGas => ("confirmed", Some("reverted"), None),
            JobKind::FailNoGas => ("failed", None, Some("simulation_reverted")),
            _ => ("confirmed", Some("success"), None),
        };
        let got_err = fs.error.as_ref().map(|e| e.code.as_str());
        if fs.status != want_status
            || (confirmed && fs.outcome.as_deref() != want_outcome)
            || (want_err.is_some() && got_err != want_err)
        {
            v.add("g", &format!("unexpected_result_{}", j.spec.kind.label()),
                  "job did not end the way its kind requires", &id,
                  format!("{id}: want {want_status}/{want_outcome:?}/{want_err:?}, got {}/{:?}/{got_err:?}", fs.status, fs.outcome));
        }

        // ---- d: webhooks for this job.
        let ds = deliveries_by_job
            .get(id.as_str())
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let has = |ev: &str| ds.iter().any(|d| d.event == ev);
        if j.spec.webhook_mode.coverage_required() {
            if confirmed {
                if !has("transaction.included") {
                    v.add(
                        "d",
                        "missing_included_webhook",
                        "confirmed job without a transaction.included delivery",
                        &id,
                        "",
                    );
                }
                if !has("transaction.confirmed") {
                    v.add(
                        "d",
                        "missing_confirmed_webhook",
                        "confirmed job without a transaction.confirmed delivery",
                        &id,
                        "",
                    );
                }
            } else if !has("transaction.failed") {
                v.add(
                    "d",
                    "missing_failed_webhook",
                    "failed job without a transaction.failed delivery",
                    &id,
                    "",
                );
            }
        }
        if confirmed && has("transaction.failed") {
            v.add(
                "d",
                "contradictory_webhook",
                "webhook event contradicts the job's final status",
                &id,
                format!("{id}: confirmed but got transaction.failed"),
            );
        }
        if !confirmed && (has("transaction.included") || has("transaction.confirmed")) {
            v.add(
                "d",
                "contradictory_webhook",
                "webhook event contradicts the job's final status",
                &id,
                format!("{id}: failed but got included/confirmed"),
            );
        }
        if confirmed && ex.len() == 1 {
            for d in ds
                .iter()
                .filter(|d| d.event == "transaction.included" || d.event == "transaction.confirmed")
            {
                let want = if ex[0].success { "success" } else { "reverted" };
                if d.outcome.as_deref() != Some(want) {
                    v.add(
                        "d",
                        "webhook_outcome_mismatch",
                        "webhook outcome disagrees with the chain",
                        &id,
                        format!("{id}: {} says {:?}, chain says {want}", d.event, d.outcome),
                    );
                    break;
                }
            }
        }
        // sequence consistency
        let mut seq_of_event: HashMap<&str, u64> = HashMap::new();
        let mut event_of_seq: HashMap<u64, &str> = HashMap::new();
        let mut bad_seq = None;
        for d in ds.iter().filter(|d| d.malformed.is_none()) {
            if let Some(prev) = seq_of_event.insert(d.event_id.as_str(), d.sequence) {
                if prev != d.sequence {
                    bad_seq = Some(format!(
                        "event {} redelivered with sequence {} then {}",
                        d.event_id, prev, d.sequence
                    ));
                }
            }
            if let Some(prev) = event_of_seq.insert(d.sequence, d.event_id.as_str()) {
                if prev != d.event_id {
                    bad_seq = Some(format!(
                        "sequence {} used by two events ({prev}, {})",
                        d.sequence, d.event_id
                    ));
                }
            }
        }
        let first_seq = |ev: &str| {
            ds.iter()
                .filter(|d| d.event == ev)
                .map(|d| d.sequence)
                .min()
        };
        if let (Some(i), Some(c)) = (
            first_seq("transaction.included"),
            first_seq("transaction.confirmed"),
        ) {
            if i >= c {
                bad_seq = Some(format!("included has sequence {i}, confirmed has {c}"));
            }
        }
        if j.spec.webhook_mode.coverage_required()
            && !ds.is_empty()
            && ds.iter().map(|d| d.sequence).min() != Some(1)
            && bad_seq.is_none()
        {
            let complete = if confirmed {
                has("transaction.included") && has("transaction.confirmed")
            } else {
                has("transaction.failed")
            };
            if complete {
                bad_seq = Some(format!(
                    "lowest sequence is {:?}, expected 1",
                    ds.iter().map(|d| d.sequence).min()
                ));
            }
        }
        if let Some(why) = bad_seq {
            v.add(
                "d",
                "sequence_inconsistent",
                "webhook sequence numbers are inconsistent for a job",
                &id,
                format!("{id}: {why}"),
            );
        }
    }

    // ---- d: per-delivery checks (independent of whether we know the job).
    for d in input.deliveries {
        if !d.signature_valid {
            v.add(
                "d",
                "invalid_signature",
                "webhook delivery with a missing/invalid X-Gum-Signature",
                &d.job_id,
                format!(
                    "{}: {}",
                    d.job_id,
                    d.signature_error.clone().unwrap_or_default()
                ),
            );
        }
        if let Some(m) = &d.malformed {
            v.add(
                "d",
                "malformed_webhook",
                "webhook body does not match the contract",
                &d.job_id,
                m.clone(),
            );
            continue;
        }
        if d.header_event_id.as_deref() != Some(d.event_id.as_str())
            || d.header_job_id.as_deref() != Some(d.job_id.as_str())
        {
            v.add(
                "d",
                "webhook_header_mismatch",
                "X-Gum-Event-Id / X-Gum-Job-Id do not match the body",
                &d.job_id,
                format!(
                    "headers=({:?},{:?}) body=({},{})",
                    d.header_event_id, d.header_job_id, d.event_id, d.job_id
                ),
            );
        }
    }

    // ---- c: a (chain, signer, nonce) can only belong to one confirmed job.
    for ((chain, signer, nonce), ids) in nonce_claims {
        if ids.len() > 1 {
            for id in &ids {
                v.add(
                    "c",
                    "nonce_claimed_twice",
                    "two confirmed jobs report the same (chain, signer, nonce)",
                    id,
                    format!(
                        "chain {chain} signer {signer} nonce {nonce}: {}",
                        ids.join(", ")
                    ),
                );
            }
        }
    }

    // ---- c: nothing may be stuck in the mempool at rest.
    for f in input.facts {
        for a in input
            .signer_addrs
            .iter()
            .chain(std::iter::once(&input.treasury))
        {
            let (l, p) = (
                f.nonce_latest.get(a).copied().unwrap_or(0),
                f.nonce_pending.get(a).copied().unwrap_or(0),
            );
            if l != p {
                v.add(
                    "c",
                    "pending_nonce_gap",
                    "at rest a signer still has pending (unmined) transactions",
                    format!("{a:#x}"),
                    format!(
                        "chain {} {a:#x}: latest nonce {l}, pending nonce {p}",
                        f.chain_id
                    ),
                );
            }
        }
    }
    v
}

/// Checks against the at-rest observability routes (c: next_nonce, e, f, i). Retried by the caller until
/// they pass or the settle timeout expires.
pub fn evaluate_at_rest(input: &Input, rest: &AtRest) -> Violations {
    let mut v = Violations::default();
    let (execs, _) = attribute(input.jobs, input.facts);

    // ---- e: analytics totals == ground truth.
    match &rest.analytics {
        Err(e) => v.add(
            "i",
            "analytics_route_failed",
            "GET /v1/analytics/transactions failed or did not match the contract",
            "",
            e.clone(),
        ),
        Ok(a) => {
            let got = a.totals.minus(&rest.analytics_baseline);
            let mut succeeded = 0u64;
            let mut reverted = 0u64;
            let mut failed = 0u64;
            let mut slack = 0u64; // indeterminate POSTs the engine may or may not have stored as failed
            for j in input.jobs {
                let ex = execs.get(&j.spec.idx).map(Vec::as_slice).unwrap_or(&[]);
                if ex.iter().any(|t| t.success) {
                    succeeded += 1;
                } else if !ex.is_empty() {
                    reverted += 1;
                } else if j.accepted() {
                    if j.final_status
                        .as_ref()
                        .map(|s| s.status == "failed")
                        .unwrap_or(false)
                    {
                        failed += 1;
                    }
                } else if j.indeterminate {
                    slack += 1;
                }
            }
            let mut diffs = Vec::new();
            if got.succeeded != succeeded {
                diffs.push(format!(
                    "succeeded: api {} vs chain {succeeded}",
                    got.succeeded
                ));
            }
            if got.reverted != reverted {
                diffs.push(format!(
                    "reverted: api {} vs chain {reverted}",
                    got.reverted
                ));
            }
            if got.failed < failed || got.failed > failed + slack {
                diffs.push(format!(
                    "failed: api {} vs observed {failed} (+{slack} indeterminate)",
                    got.failed
                ));
            }
            let want_total = succeeded + reverted + failed;
            let in_progress = got.queued + got.active();
            if in_progress != 0 {
                diffs.push(format!(
                    "not at rest: queued {} processing {} in_flight {} included {}",
                    got.queued, got.processing, got.in_flight, got.included
                ));
            }
            if got.total < want_total || got.total > want_total + slack + in_progress {
                diffs.push(format!(
                    "total: api {} vs ground truth {want_total} (+{slack} indeterminate)",
                    got.total
                ));
            }
            // per-chain breakdown must add up to the totals
            if !a.by_chain.is_empty() {
                let sum: u64 = a.by_chain.iter().map(|c| c.counts.total).sum();
                if sum != a.totals.total {
                    diffs.push(format!(
                        "by_chain totals sum to {sum}, totals.total is {}",
                        a.totals.total
                    ));
                }
            }
            for d in diffs {
                v.add(
                    "e",
                    "analytics_mismatch",
                    "analytics totals differ from ground truth at rest",
                    "",
                    d,
                );
            }
        }
    }

    // ---- f: balances route == chain.
    match &rest.balances {
        Err(e) => v.add(
            "i",
            "balances_route_failed",
            "GET /v1/signers/balances failed or did not match the contract",
            "",
            e.clone(),
        ),
        Ok(b) => {
            for f in input.facts {
                let Some(chain) = b.chains.iter().find(|c| c.chain_id == f.chain_id) else {
                    v.add(
                        "f",
                        "balances_chain_missing",
                        "a chain is missing from /v1/signers/balances",
                        "",
                        format!("chain {}", f.chain_id),
                    );
                    continue;
                };
                let mut reported: HashMap<Address, String> = HashMap::new();
                for bal in chain.signers.iter().chain(chain.treasury.iter()) {
                    if let Ok(a) = bal.address.parse::<Address>() {
                        reported.insert(a, bal.confirmed.clone());
                    }
                }
                for a in input
                    .signer_addrs
                    .iter()
                    .chain(std::iter::once(&input.treasury))
                {
                    let chain_bal = f.balances.get(a).copied().unwrap_or_default();
                    match reported.get(a) {
                        None => v.add(
                            "f",
                            "balance_missing",
                            "an account is missing from /v1/signers/balances",
                            format!("{a:#x}"),
                            format!("chain {} {a:#x}", f.chain_id),
                        ),
                        Some(s) if s.parse::<U256>().ok() != Some(chain_bal) => v.add(
                            "f",
                            "balance_mismatch",
                            "`confirmed` balance differs from eth_getBalance at rest",
                            format!("{a:#x}"),
                            format!("chain {} {a:#x}: api {s} vs chain {chain_bal}", f.chain_id),
                        ),
                        Some(_) => {}
                    }
                }
            }
        }
    }

    // ---- c: engine's next_nonce == chain nonce.
    match &rest.signers {
        Err(e) => v.add(
            "i",
            "signers_route_failed",
            "GET /v1/signers failed or did not match the contract",
            "",
            e.clone(),
        ),
        Ok(s) => {
            let mut seen: HashSet<(u64, Address)> = HashSet::new();
            for p in &s.pairs {
                let Ok(addr) = p.signer.parse::<Address>() else {
                    continue;
                };
                seen.insert((p.chain_id, addr));
                let Some(f) = input.facts.iter().find(|f| f.chain_id == p.chain_id) else {
                    continue;
                };
                if let (Some(api), Some(chain)) = (p.next_nonce, f.nonce_latest.get(&addr)) {
                    if api != *chain {
                        v.add("c", "next_nonce_mismatch", "/v1/signers next_nonce differs from the on-chain nonce at rest (gap or reuse ahead)", format!("{addr:#x}"),
                              format!("chain {} {addr:#x}: api {api} vs chain {chain}", p.chain_id));
                    }
                }
                if p.state == "paused" || p.state == "busy" {
                    v.add(
                        "c",
                        "pair_not_idle_at_rest",
                        "a pair is busy/paused although all jobs are terminal",
                        format!("{addr:#x}"),
                        format!(
                            "chain {} {addr:#x}: state {} pause {:?}",
                            p.chain_id,
                            p.state,
                            p.pause.as_ref().map(|x| &x.reason)
                        ),
                    );
                }
            }
            for f in input.facts {
                for a in input.signer_addrs {
                    if !seen.contains(&(f.chain_id, *a)) {
                        v.add(
                            "i",
                            "pair_missing",
                            "a configured (chain, signer) pair is missing from /v1/signers",
                            format!("{a:#x}"),
                            format!("chain {}", f.chain_id),
                        );
                    }
                }
            }
        }
    }
    v
}
