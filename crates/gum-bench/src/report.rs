//! The run report: one JSON document (the regression-gate input) plus a markdown summary.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::loadgen::{JobRecord, Segment};
use crate::proxy::ProxySnapshot;
use crate::rig::ProcSample;
use crate::sink::Delivery;
use crate::tracker::{DrainStats, Sample};
use crate::util::{new_hist, percentiles_ms, record, round3, Clock, GitInfo, Machine, Percentiles};
use crate::verdict::Violation;

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct EngineInfo {
    /// "binary" | "fake" | "target"
    pub mode: String,
    pub label: String,
    /// "release" | "debug" | "fake" | "unknown"
    pub build_profile: String,
    pub restarts: u32,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LoadSummary {
    pub offered_jobs: usize,
    pub load_duration_s: f64,
    pub offered_rate: f64,
    pub accepted_jobs: usize,
    /// accepted / load duration
    pub achieved_accept_rate: f64,
    pub segments: Vec<Segment>,
    /// How late the harness fired requests relative to the schedule. Large values = harness bottleneck.
    pub schedule_lag_ms: Percentiles,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Throughput {
    pub confirmed_jobs: usize,
    /// confirmed jobs / (last confirmation − load start)
    pub confirmed_per_s: f64,
    pub window_s: f64,
    /// Where "last confirmation" came from: "api_confirmed_at" | "confirmed_webhook" | "drain_end".
    pub basis: String,
    /// Finished (succeeded+reverted+failed) jobs/s inside the load window, from the analytics time series.
    pub finished_per_s_in_load_window: Option<f64>,
    pub on_chain_per_s_in_load_window: Option<f64>,
    pub on_chain_per_s_in_load_window_by_chain: BTreeMap<u64, f64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Latencies {
    /// POST response − intended start (202s only).
    pub accept: Percentiles,
    /// `included_at − created_at` from `GET /v1/transactions/{id}` (engine clock on both sides).
    pub accept_to_included: Percentiles,
    /// first `transaction.included` arrival − intended start (harness monotonic clock).
    pub accept_to_included_webhook: Percentiles,
    /// first `transaction.confirmed` arrival − intended start.
    pub accept_to_confirmed_webhook: Percentiles,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RpcMethod {
    pub calls: u64,
    pub calls_per_tx: f64,
    pub faulted: u64,
    pub upstream_errors: u64,
    pub latency_ms: Percentiles,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct RpcSummary {
    /// "proxy" (measured externally) or "engine_reported" (--target mode: from /v1/chains).
    pub source: String,
    /// Calls between load start and the moment the last job became terminal.
    pub total_calls: u64,
    /// Calls the engine made while booting (before load start); not part of calls/tx.
    pub boot_calls: u64,
    /// Denominator of calls/tx: jobs that reached a terminal state.
    pub tx_count: usize,
    pub calls_per_tx: f64,
    /// Only attributable when the whole run used one kind of job.
    pub calls_per_tx_with_gas_limit: Option<f64>,
    pub calls_per_tx_without_gas_limit: Option<f64>,
    pub credits_per_tx_at_20: f64,
    pub credits_per_tx_at_30: f64,
    pub by_method: BTreeMap<String, RpcMethod>,
    pub faults_injected: BTreeMap<String, u64>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ProcSummary {
    pub samples: usize,
    pub cpu_pct_avg: f64,
    pub cpu_pct_max: f64,
    pub rss_mb_avg: f64,
    pub rss_mb_max: f64,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JobsSummary {
    pub by_kind: BTreeMap<String, usize>,
    pub confirmed_success: usize,
    pub confirmed_reverted: usize,
    pub failed: usize,
    pub non_terminal: usize,
    pub not_accepted: usize,
    pub indeterminate_posts: usize,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct WebhookSummary {
    pub deliveries: usize,
    pub unique_events: usize,
    pub redeliveries: usize,
    pub invalid_signatures: usize,
    pub by_event: BTreeMap<String, usize>,
    pub by_receiver: BTreeMap<String, usize>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Verdict {
    pub pass: bool,
    pub violations: Vec<Violation>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Report {
    pub schema_version: u32,
    pub scenario: String,
    pub params: Value,
    pub started_at: String,
    pub finished_at: String,
    pub duration_s: f64,
    pub git: Option<GitInfo>,
    pub machine: Option<Machine>,
    pub engine: EngineInfo,
    pub load: LoadSummary,
    pub throughput: Throughput,
    pub latency_ms: Latencies,
    pub rpc: RpcSummary,
    pub engine_process: ProcSummary,
    /// POST outcomes: HTTP status or `transport:<kind>`.
    pub http: BTreeMap<String, usize>,
    pub jobs: JobsSummary,
    pub webhooks: WebhookSummary,
    pub drain: DrainStats,
    pub scenario_metrics: BTreeMap<String, Value>,
    pub notes: Vec<String>,
    /// Signs that the harness itself may have distorted the measurement.
    pub harness_warnings: Vec<String>,
    pub verdict: Verdict,
    pub series: Vec<Sample>,
}

pub struct Inputs<'a> {
    pub clock: Clock,
    pub jobs: &'a [JobRecord],
    pub deliveries: &'a [Delivery],
    pub samples: Vec<Sample>,
    pub segments: Vec<Segment>,
    pub load_start: Duration,
    pub load_end: Duration,
    pub drain_end: Duration,
    pub lag: hdrhistogram::Histogram<u64>,
    pub rpc_at_load_start: Option<ProxySnapshot>,
    pub rpc_at_drain_end: Option<ProxySnapshot>,
    pub rpc_source: &'static str,
    pub proc_samples: Vec<ProcSample>,
}

/// Sum several proxies' snapshots (multi-chain).
pub fn merge_snapshots(snaps: &[ProxySnapshot]) -> ProxySnapshot {
    let mut out = ProxySnapshot::default();
    for s in snaps {
        out.http_requests += s.http_requests;
        out.total_calls += s.total_calls;
        for (m, ms) in &s.by_method {
            let e = out.by_method.entry(m.clone()).or_default();
            e.calls += ms.calls;
            e.faulted += ms.faulted;
            e.upstream_errors += ms.upstream_errors;
            // Percentiles cannot be merged exactly; keep the busiest chain's view.
            if ms.latency_ms.count > e.latency_ms.count {
                e.latency_ms = ms.latency_ms.clone();
            }
        }
        for (k, n) in &s.faults_injected {
            *out.faults_injected.entry(k.clone()).or_insert(0) += n;
        }
    }
    out
}

pub fn summarise(r: &mut Report, inp: Inputs) {
    let jobs = inp.jobs;
    // ---- load
    let load_duration_s = (inp.load_end.saturating_sub(inp.load_start)).as_secs_f64();
    let offered_jobs: usize = inp.segments.iter().map(|s| s.scheduled).sum();
    let accepted: Vec<&JobRecord> = jobs.iter().filter(|j| j.accepted()).collect();
    let seg_time: f64 = inp.segments.iter().map(|s| s.duration_s).sum();
    r.load = LoadSummary {
        offered_jobs,
        load_duration_s: round3(load_duration_s),
        offered_rate: if seg_time > 0.0 {
            round3(offered_jobs as f64 / seg_time)
        } else {
            0.0
        },
        accepted_jobs: accepted.len(),
        achieved_accept_rate: if seg_time > 0.0 {
            round3(accepted.len() as f64 / seg_time)
        } else {
            0.0
        },
        segments: inp.segments.clone(),
        schedule_lag_ms: percentiles_ms(&inp.lag),
    };
    if r.load.schedule_lag_ms.p99 > 50.0 {
        r.harness_warnings.push(format!(
            "schedule lag p99 = {} ms: the load generator fired requests late; offered-rate fidelity is reduced",
            r.load.schedule_lag_ms.p99
        ));
    }
    if jobs.len() != offered_jobs {
        r.harness_warnings.push(format!(
            "{} arrivals scheduled but {} job records collected",
            offered_jobs,
            jobs.len()
        ));
    }

    // ---- http
    for j in jobs {
        let key = match (&j.submit.status, &j.submit.transport_error) {
            (Some(s), None) => s.to_string(),
            (Some(s), Some(e)) => format!("{s}+transport:{e}"),
            (None, Some(e)) => format!("transport:{e}"),
            (None, None) => "unknown".into(),
        };
        *r.http.entry(key).or_insert(0) += 1;
    }

    // ---- jobs
    let mut js = JobsSummary::default();
    for j in jobs {
        *js.by_kind
            .entry(j.spec.kind.label().to_string())
            .or_insert(0) += 1;
        js.indeterminate_posts += j.indeterminate as usize;
        if !j.accepted() {
            js.not_accepted += 1;
            continue;
        }
        match j.final_status.as_ref().filter(|s| s.is_terminal()) {
            Some(s) if s.status == "failed" => js.failed += 1,
            Some(s) if s.outcome.as_deref() == Some("reverted") => js.confirmed_reverted += 1,
            Some(_) => js.confirmed_success += 1,
            None => js.non_terminal += 1,
        }
    }
    r.jobs = js;

    // ---- webhooks + latencies
    let mut first_included: std::collections::HashMap<&str, Duration> = Default::default();
    let mut first_confirmed: std::collections::HashMap<&str, Duration> = Default::default();
    let mut events = std::collections::HashSet::new();
    let mut ws = WebhookSummary {
        deliveries: inp.deliveries.len(),
        ..Default::default()
    };
    for d in inp.deliveries {
        let at = inp.clock.at(d.arrived);
        let slot = match d.event.as_str() {
            "transaction.included" => Some(&mut first_included),
            "transaction.confirmed" => Some(&mut first_confirmed),
            _ => None,
        };
        if let Some(map) = slot {
            let e = map.entry(d.job_id.as_str()).or_insert(at);
            if at < *e {
                *e = at;
            }
        }
        events.insert(d.event_id.as_str());
        ws.invalid_signatures += (!d.signature_valid) as usize;
        *ws.by_event.entry(d.event.clone()).or_insert(0) += 1;
        *ws.by_receiver
            .entry(d.mode.label().to_string())
            .or_insert(0) += 1;
    }
    ws.unique_events = events.len();
    ws.redeliveries = ws.deliveries - ws.unique_events.min(ws.deliveries);
    r.webhooks = ws;

    let (mut h_accept, mut h_incl_api, mut h_incl_wh, mut h_conf_wh) =
        (new_hist(), new_hist(), new_hist(), new_hist());
    let mut last_confirmed_api: Option<Duration> = None;
    for j in &accepted {
        if j.submit.status == Some(202) {
            record(&mut h_accept, j.responded.saturating_sub(j.intended));
        }
        let id = j.job_id().unwrap_or_default();
        if let Some(t) = first_included.get(id) {
            record(&mut h_incl_wh, t.saturating_sub(j.intended));
        }
        if let Some(t) = first_confirmed.get(id) {
            record(&mut h_conf_wh, t.saturating_sub(j.intended));
        }
        if let Some(fs) = &j.final_status {
            if let (Some(c), Some(i)) = (fs.timestamps.created_at, fs.timestamps.included_at) {
                record(&mut h_incl_api, (i - c).to_std().unwrap_or_default());
            }
            if let (true, Some(c)) = (fs.status == "confirmed", fs.timestamps.confirmed_at) {
                let off = (c - inp.clock.start_wall).to_std().unwrap_or_default();
                last_confirmed_api = Some(last_confirmed_api.map_or(off, |p: Duration| p.max(off)));
            }
        }
    }
    r.latency_ms = Latencies {
        accept: percentiles_ms(&h_accept),
        accept_to_included: percentiles_ms(&h_incl_api),
        accept_to_included_webhook: percentiles_ms(&h_incl_wh),
        accept_to_confirmed_webhook: percentiles_ms(&h_conf_wh),
    };

    // ---- throughput
    let confirmed_jobs = r.jobs.confirmed_success + r.jobs.confirmed_reverted;
    let last_wh = first_confirmed.values().max().copied();
    let (end, basis) = match (last_confirmed_api, last_wh) {
        (Some(t), _) if t > inp.load_start => (t, "api_confirmed_at"),
        (_, Some(t)) if t > inp.load_start => (t, "confirmed_webhook"),
        _ => (inp.drain_end, "drain_end"),
    };
    let window = end.saturating_sub(inp.load_start).as_secs_f64();
    let (ls, le) = (inp.load_start.as_secs_f64(), inp.load_end.as_secs_f64());
    let finished = crate::tracker::finished_rate(&inp.samples, ls, le, false);
    let on_chain = crate::tracker::finished_rate(&inp.samples, ls, le, true);
    r.throughput = Throughput {
        confirmed_jobs,
        confirmed_per_s: if window > 0.0 {
            round3(confirmed_jobs as f64 / window)
        } else {
            0.0
        },
        window_s: round3(window),
        basis: basis.into(),
        finished_per_s_in_load_window: finished.as_ref().map(|(o, _)| round3(*o)),
        on_chain_per_s_in_load_window: on_chain.as_ref().map(|(o, _)| round3(*o)),
        on_chain_per_s_in_load_window_by_chain: on_chain
            .map(|(_, c)| c.into_iter().map(|(k, v)| (k, round3(v))).collect())
            .unwrap_or_default(),
    };

    // ---- rpc
    if let (Some(a), Some(b)) = (&inp.rpc_at_load_start, &inp.rpc_at_drain_end) {
        let tx_count = r.jobs.confirmed_success + r.jobs.confirmed_reverted + r.jobs.failed;
        let denom = tx_count.max(1) as f64;
        let mut by_method = BTreeMap::new();
        let mut total = 0;
        for (m, s) in &b.by_method {
            let before = a.by_method.get(m).map(|x| x.calls).unwrap_or(0);
            let calls = s.calls.saturating_sub(before);
            if calls == 0 {
                continue; // only called while booting
            }
            total += calls;
            by_method.insert(
                m.clone(),
                RpcMethod {
                    calls,
                    calls_per_tx: round3(calls as f64 / denom),
                    faulted: s.faulted,
                    upstream_errors: s.upstream_errors,
                    latency_ms: s.latency_ms.clone(),
                },
            );
        }
        let per_tx = round3(total as f64 / denom);
        let kinds: Vec<bool> = jobs.iter().map(|j| j.spec.kind.has_gas_limit()).collect();
        let all_gas = !kinds.is_empty() && kinds.iter().all(|g| *g);
        let all_nogas = !kinds.is_empty() && kinds.iter().all(|g| !*g);
        r.rpc = RpcSummary {
            source: inp.rpc_source.into(),
            total_calls: total,
            boot_calls: a.total_calls,
            tx_count,
            calls_per_tx: per_tx,
            calls_per_tx_with_gas_limit: all_gas.then_some(per_tx),
            calls_per_tx_without_gas_limit: all_nogas.then_some(per_tx),
            credits_per_tx_at_20: round3(per_tx * 20.0),
            credits_per_tx_at_30: round3(per_tx * 30.0),
            by_method,
            faults_injected: b.faults_injected.clone(),
        };
    } else {
        r.rpc.source = "unavailable".into();
    }

    // ---- engine process
    if !inp.proc_samples.is_empty() {
        let n = inp.proc_samples.len() as f64;
        let mb = |b: u64| b as f64 / (1024.0 * 1024.0);
        r.engine_process = ProcSummary {
            samples: inp.proc_samples.len(),
            cpu_pct_avg: round3(
                inp.proc_samples
                    .iter()
                    .map(|s| s.cpu_pct as f64)
                    .sum::<f64>()
                    / n,
            ),
            cpu_pct_max: round3(
                inp.proc_samples
                    .iter()
                    .map(|s| s.cpu_pct as f64)
                    .fold(0.0, f64::max),
            ),
            rss_mb_avg: round3(
                inp.proc_samples
                    .iter()
                    .map(|s| mb(s.rss_bytes))
                    .sum::<f64>()
                    / n,
            ),
            rss_mb_max: round3(
                inp.proc_samples
                    .iter()
                    .map(|s| mb(s.rss_bytes))
                    .fold(0.0, f64::max),
            ),
        };
    }
    r.series = inp.samples;
}

fn pct_row(name: &str, p: &Percentiles) -> String {
    if p.count == 0 {
        return format!("| {name} | 0 | – | – | – | – | – | – |\n");
    }
    format!(
        "| {name} | {} | {} | {} | {} | {} | {} | {} |\n",
        p.count, p.p50, p.p90, p.p95, p.p99, p.p999, p.max
    )
}

pub fn markdown(r: &Report) -> String {
    let mut s = String::new();
    let verdict = if r.verdict.pass { "PASS" } else { "FAIL" };
    s += &format!("# gum-bench `{}` — verdict: **{verdict}**\n\n", r.scenario);
    let git = r
        .git
        .as_ref()
        .map(|g| format!("{}{}", g.sha, if g.dirty { " (dirty)" } else { "" }))
        .unwrap_or_else(|| "nogit".into());
    s += &format!(
        "- engine: {} · build profile: {} · restarts: {}\n",
        r.engine.label, r.engine.build_profile, r.engine.restarts
    );
    s += &format!(
        "- git: {git} · started: {} · total duration: {} s\n",
        r.started_at, r.duration_s
    );
    if let Some(m) = &r.machine {
        s += &format!("- machine: {}\n", m.fingerprint);
    }
    s += &format!("- params: `{}`\n\n", r.params);

    s += "## Load and throughput\n\n";
    s += &format!(
        "- offered: {} jobs over {} s = **{} /s** · accepted: {} ({} /s)\n",
        r.load.offered_jobs,
        r.load.load_duration_s,
        r.load.offered_rate,
        r.load.accepted_jobs,
        r.load.achieved_accept_rate
    );
    s += &format!(
        "- throughput: **{} confirmed jobs/s** ({} jobs in {} s, basis: {})\n",
        r.throughput.confirmed_per_s,
        r.throughput.confirmed_jobs,
        r.throughput.window_s,
        r.throughput.basis
    );
    if let Some(x) = r.throughput.on_chain_per_s_in_load_window {
        s += &format!(
            "- inside the load window: {x} on-chain jobs/s; per chain: {:?}\n",
            r.throughput.on_chain_per_s_in_load_window_by_chain
        );
    }
    s += &format!(
        "- jobs: {} success · {} reverted · {} failed · {} non-terminal · {} not accepted · by kind {:?}\n",
        r.jobs.confirmed_success, r.jobs.confirmed_reverted, r.jobs.failed, r.jobs.non_terminal, r.jobs.not_accepted, r.jobs.by_kind
    );
    s += &format!("- POST outcomes: {:?}\n", r.http);
    s += &format!(
        "- schedule lag (harness health): p50 {} ms · p99 {} ms · max {} ms\n\n",
        r.load.schedule_lag_ms.p50, r.load.schedule_lag_ms.p99, r.load.schedule_lag_ms.max
    );

    s += "## Latency (ms)\n\n| metric | n | p50 | p90 | p95 | p99 | p99.9 | max |\n|---|---|---|---|---|---|---|---|\n";
    s += &pct_row("accept (POST, from intended start)", &r.latency_ms.accept);
    s += &pct_row(
        "accept → included (status API timestamps)",
        &r.latency_ms.accept_to_included,
    );
    s += &pct_row(
        "accept → `included` webhook",
        &r.latency_ms.accept_to_included_webhook,
    );
    s += &pct_row(
        "accept → `confirmed` webhook",
        &r.latency_ms.accept_to_confirmed_webhook,
    );

    s += &format!("\n## RPC (source: {})\n\n", r.rpc.source);
    s += &format!(
        "- **{} calls/tx** ({} calls / {} terminal jobs; {} boot calls excluded) → **{} credits/tx @20**, **{} @30**\n",
        r.rpc.calls_per_tx, r.rpc.total_calls, r.rpc.tx_count, r.rpc.boot_calls, r.rpc.credits_per_tx_at_20, r.rpc.credits_per_tx_at_30
    );
    if let Some(x) = r.rpc.calls_per_tx_with_gas_limit {
        s += &format!("- with gas_limit: {x} calls/tx\n");
    }
    if let Some(x) = r.rpc.calls_per_tx_without_gas_limit {
        s += &format!("- without gas_limit: {x} calls/tx\n");
    }
    if !r.rpc.faults_injected.is_empty() {
        s += &format!("- faults injected: {:?}\n", r.rpc.faults_injected);
    }
    s += "\n| method | calls | calls/tx | faulted | p50 ms | p99 ms |\n|---|---|---|---|---|---|\n";
    for (m, x) in &r.rpc.by_method {
        s += &format!(
            "| {m} | {} | {} | {} | {} | {} |\n",
            x.calls, x.calls_per_tx, x.faulted, x.latency_ms.p50, x.latency_ms.p99
        );
    }

    s += "\n## Engine process, webhooks\n\n";
    s += &format!(
        "- CPU avg {}% / max {}% (of one core) · RSS avg {} MB / max {} MB ({} samples)\n",
        r.engine_process.cpu_pct_avg,
        r.engine_process.cpu_pct_max,
        r.engine_process.rss_mb_avg,
        r.engine_process.rss_mb_max,
        r.engine_process.samples
    );
    s += &format!(
        "- webhooks: {} deliveries · {} unique events · {} redeliveries · {} invalid signatures · by receiver {:?}\n",
        r.webhooks.deliveries, r.webhooks.unique_events, r.webhooks.redeliveries, r.webhooks.invalid_signatures, r.webhooks.by_receiver
    );
    let ok: Vec<&Sample> = r.series.iter().filter(|x| x.ok).collect();
    if !ok.is_empty() {
        let maxq = ok.iter().map(|x| x.counts.queued).max().unwrap_or(0);
        let util = ok.iter().map(|x| x.utilisation).sum::<f64>() / ok.len() as f64;
        s += &format!("- time series: {} samples ({} failed) · max queued {} · mean signer utilisation {:.2}\n", r.series.len(), r.series.len() - ok.len(), maxq, util);
    }
    s += &format!(
        "- drain: waited {:.1} s in {} poll rounds{}\n",
        r.drain.waited_s,
        r.drain.poll_rounds,
        r.drain
            .reason
            .as_ref()
            .map(|x| format!(" — TIMED OUT: {x}"))
            .unwrap_or_default()
    );

    if !r.scenario_metrics.is_empty() {
        s += "\n## Scenario metrics\n\n";
        for (k, v) in &r.scenario_metrics {
            s += &format!("- {k}: {v}\n");
        }
    }
    if !r.notes.is_empty() {
        s += "\n## Notes\n\n";
        for n in &r.notes {
            s += &format!("- {n}\n");
        }
    }
    if !r.harness_warnings.is_empty() {
        s += "\n## Harness warnings\n\n";
        for n in &r.harness_warnings {
            s += &format!("- {n}\n");
        }
    }
    s += &format!("\n## Correctness verdict: {verdict}\n\n");
    if r.verdict.violations.is_empty() {
        s += "No violations.\n";
    }
    for v in &r.verdict.violations {
        s += &format!(
            "- **[{}] {}** ×{} — {}\n",
            v.check, v.code, v.count, v.message
        );
        for e in &v.examples {
            s += &format!("  - {e}\n");
        }
        if !v.job_ids.is_empty() {
            let shown: Vec<&str> = v.job_ids.iter().take(10).map(String::as_str).collect();
            s += &format!(
                "  - job ids: {}{}\n",
                shown.join(", "),
                if v.job_ids.len() > 10 {
                    ", … (see JSON)"
                } else {
                    ""
                }
            );
        }
    }
    s
}

/// Write `<dir>/<sha>-<scenario>-<timestamp>.{json,md}`; returns the JSON path.
pub fn write(r: &Report, dir: &Path) -> Result<PathBuf> {
    std::fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let sha = r
        .git
        .as_ref()
        .map(|g| g.sha.clone())
        .unwrap_or_else(|| "nogit".into());
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let base = dir.join(format!("{sha}-{}-{stamp}", r.scenario));
    let json = base.with_extension("json");
    std::fs::write(&json, serde_json::to_vec_pretty(r)?)
        .with_context(|| format!("writing {}", json.display()))?;
    let md = base.with_extension("md");
    std::fs::write(&md, markdown(r)).with_context(|| format!("writing {}", md.display()))?;
    Ok(json)
}

pub fn load(path: &Path) -> Result<Report> {
    let bytes = std::fs::read(path).with_context(|| format!("reading {}", path.display()))?;
    let r: Report = serde_json::from_slice(&bytes)
        .with_context(|| format!("{} is not a gum-bench report", path.display()))?;
    if r.schema_version != SCHEMA_VERSION {
        anyhow::bail!(
            "{}: schema_version {} (this gum-bench understands {SCHEMA_VERSION})",
            path.display(),
            r.schema_version
        );
    }
    Ok(r)
}
