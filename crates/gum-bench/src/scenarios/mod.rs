//! Scenario framework. A scenario only decides *what load and what faults happen when*; the framework
//! owns the rig, the drain, the oracle, the verdict and the report — so every scenario gets the full
//! correctness verdict for free.
//!
//! Adding a scenario: create `src/scenarios/<name>.rs` with a unit struct implementing [`Scenario`], and
//! add it to [`all`].

mod burst;
mod chaos;
mod funds;
mod mixed;
mod rpc_faults;
mod saturation;
mod smoke;
mod soak;
mod steady;
mod webhook_hostile;

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures::future::BoxFuture;
use parking_lot::Mutex;
use serde::Serialize;
use serde_json::Value;

use crate::api::{AnalyticsResp, BalancesResp, SignersResp};
use crate::loadgen::{ChainTarget, JobMix, LoadConfig, Loadgen};
use crate::report::{self, EngineInfo, Report, Verdict};
use crate::rig::{eth, EngineMode, Rig, RigConfig};
use crate::sink::{Sink, SinkMode};
use crate::tracker::{self, Sampler};
use crate::util::{git_info, machine, round3, workspace_root, Clock};
use crate::verdict::{self, AtRest, Violations};
use crate::{oracle, RunArgs};

/// Resolved parameters of a run (scenario defaults + CLI overrides). Serialised into the report.
#[derive(Clone, Debug, Serialize)]
pub struct Params {
    pub chains: usize,
    pub signers: usize,
    /// Offered jobs/s (meaning is scenario specific for ramp/burst profiles: the base/start rate).
    pub rate: f64,
    pub duration_s: f64,
    pub mix: JobMix,
    pub idem_fraction: f64,
    pub replay_fraction: f64,
    pub retry_with_idem: bool,
    pub webhook_modes: Vec<(SinkMode, u32)>,
    pub slow_ms: u64,
    pub flaky_p: f64,
    pub burn_iterations: u64,
    pub signer_min_balance_eth: f64,
    pub topup_amount_eth: f64,
    pub treasury_min_balance_eth: f64,
    pub initial_signer_balance_eth: Option<f64>,
    pub block_time_s: u64,
    pub rpc_rtt_ms: u64,
    pub account_rps: u32,
    pub seed: u64,
    /// Scenario specific knobs.
    pub extra: BTreeMap<String, Value>,
}

impl Default for Params {
    fn default() -> Self {
        Params {
            chains: 1,
            signers: 5,
            rate: 5.0,
            duration_s: 30.0,
            mix: JobMix::MIXED,
            idem_fraction: 0.5,
            replay_fraction: 0.2,
            retry_with_idem: false,
            webhook_modes: vec![(SinkMode::Ok, 1)],
            slow_ms: 2000,
            flaky_p: 0.5,
            burn_iterations: 200,
            signer_min_balance_eth: 1.0,
            topup_amount_eth: 5.0,
            treasury_min_balance_eth: 100.0,
            initial_signer_balance_eth: None,
            block_time_s: 1,
            rpc_rtt_ms: 0,
            account_rps: 500,
            seed: 42,
            extra: BTreeMap::new(),
        }
    }
}

impl Params {
    pub fn extra_f64(&self, key: &str, default: f64) -> f64 {
        self.extra
            .get(key)
            .and_then(|v| v.as_f64())
            .unwrap_or(default)
    }
    /// Theoretical on-chain capacity per chain: one in-flight tx per (signer, chain) per block.
    pub fn capacity_per_chain(&self) -> f64 {
        self.signers as f64 / self.block_time_s as f64
    }
}

pub trait Scenario: Send + Sync {
    fn name(&self) -> &'static str;
    fn about(&self) -> &'static str;
    fn defaults(&self) -> Params;
    /// Needs kill/restart of the engine or Anvil (unavailable with `--target`).
    fn needs_process_control(&self) -> bool {
        false
    }
    /// Generate load / inject faults. Returns when the load phase is over.
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>>;
    /// Scenario-specific assertions over the finished report (use check id "s").
    fn assess(&self, _ctx: &Ctx, _report: &Report, _v: &mut Violations) {}
}

pub fn all() -> Vec<Box<dyn Scenario>> {
    vec![
        Box::new(smoke::Smoke),
        Box::new(steady::Steady),
        Box::new(saturation::Saturation),
        Box::new(burst::Burst),
        Box::new(mixed::Mixed),
        Box::new(webhook_hostile::WebhookHostile),
        Box::new(funds::Funds),
        Box::new(rpc_faults::RpcFaults),
        Box::new(chaos::Chaos),
        Box::new(soak::Soak),
    ]
}

pub struct Ctx {
    pub rig: Arc<Rig>,
    pub load: Arc<Loadgen>,
    pub sampler: Sampler,
    pub clock: Clock,
    pub params: Params,
    pub stop: AtomicBool,
    notes: Mutex<Vec<String>>,
    metrics: Mutex<BTreeMap<String, Value>>,
    violations: Mutex<Violations>,
}

impl Ctx {
    pub async fn constant(&self, rate: f64, secs: f64) -> usize {
        self.load
            .run_segment(rate, Duration::from_secs_f64(secs), None, &self.stop)
            .await
    }
    pub fn note(&self, s: impl Into<String>) {
        let s = s.into();
        eprintln!("[{:>7.1}s] {s}", self.clock.now().as_secs_f64());
        self.notes.lock().push(s);
    }
    pub fn metric(&self, k: &str, v: impl Into<Value>) {
        self.metrics.lock().insert(k.to_string(), v.into());
    }
    pub fn violation(&self, code: &str, message: &str, detail: impl Into<String>) {
        self.violations.lock().add("s", code, message, "", detail);
    }
    /// Wait until the engine reports no queued or active jobs (and the generator is idle).
    pub async fn wait_engine_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        loop {
            if self.load.inflight() == 0 {
                if let Ok(a) = self
                    .rig
                    .client
                    .get_json::<AnalyticsResp>("/v1/analytics/transactions", Duration::from_secs(2))
                    .await
                {
                    if a.totals.queued + a.totals.active() == 0 {
                        return true;
                    }
                }
            }
            if Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
    }
}

fn resolve_params(s: &dyn Scenario, a: &RunArgs) -> Result<Params> {
    let mut p = s.defaults();
    if let Some(v) = a.rate {
        p.rate = v;
    }
    if let Some(v) = a.duration {
        p.duration_s = v;
    }
    if let Some(v) = a.signers {
        p.signers = v;
    }
    if let Some(v) = a.chains {
        p.chains = v;
    }
    if let Some(m) = &a.mix {
        p.mix = JobMix::parse(m)?;
    }
    if let Some(v) = a.idem_fraction {
        p.idem_fraction = v;
    }
    if let Some(v) = a.replay_fraction {
        p.replay_fraction = v;
    }
    if let Some(v) = a.seed {
        p.seed = v;
    } else if a.target.is_some() {
        // A long-lived target chain has seen the default seed's ids before.
        p.seed = rand::random();
    }
    p.rpc_rtt_ms = a.rpc_rtt_ms;
    p.account_rps = a.account_rps;
    for kv in &a.set {
        let (k, v) = kv
            .split_once('=')
            .ok_or_else(|| anyhow!("--set expects key=value, got {kv:?}"))?;
        let val = serde_json::from_str(v).unwrap_or_else(|_| Value::String(v.to_string()));
        p.extra.insert(k.to_string(), val);
    }
    if p.chains == 0 || p.chains > 3 {
        bail!("--chains must be 1..=3 (chain ids 31337..31339)");
    }
    if p.signers == 0 {
        bail!("--signers must be at least 1");
    }
    if p.rate <= 0.0 || p.duration_s <= 0.0 {
        bail!("--rate and --duration must be positive");
    }
    if p.rate > 2000.0 {
        bail!(
            "--rate {} is beyond what this harness is validated for (2000/s)",
            p.rate
        );
    }
    Ok(p)
}

/// Run one scenario end to end. `Ok(report)` even when the verdict fails; `Err` = the harness itself failed.
pub async fn run(args: &RunArgs) -> Result<(Report, PathBuf)> {
    let scenarios = all();
    let scenario = scenarios
        .iter()
        .find(|s| s.name() == args.scenario)
        .ok_or_else(|| {
            anyhow!(
                "unknown scenario {:?}; available: {}",
                args.scenario,
                scenarios
                    .iter()
                    .map(|s| s.name())
                    .collect::<Vec<_>>()
                    .join(", ")
            )
        })?;
    let params = resolve_params(scenario.as_ref(), args)?;
    let root = workspace_root();
    let results_dir = args
        .results_dir
        .clone()
        .unwrap_or_else(|| root.join("bench/results"));
    let stamp = chrono::Utc::now().format("%Y%m%dT%H%M%SZ");
    let run_dir = results_dir
        .join("runs")
        .join(format!("{stamp}-{}", scenario.name()));

    let engine = if let Some(t) = &args.target {
        if scenario.needs_process_control() {
            bail!(
                "scenario `{}` needs process control and cannot run against --target",
                scenario.name()
            );
        }
        let mut rpc_urls = Vec::new();
        for kv in &args.target_rpc {
            let (id, url) = kv
                .split_once('=')
                .ok_or_else(|| anyhow!("--target-rpc expects <chain_id>=<url>, got {kv:?}"))?;
            rpc_urls.push((
                id.parse()
                    .with_context(|| format!("bad chain id in --target-rpc {kv:?}"))?,
                url.to_string(),
            ));
        }
        EngineMode::Target {
            base_url: t.clone(),
            rpc_urls,
            deployer_key: args.deployer_key.clone(),
        }
    } else if args.fake_engine
        || args.fake_bug.is_some()
        || args.engine_bin.as_deref() == Some(std::path::Path::new("fake"))
    {
        EngineMode::Fake {
            bug: args.fake_bug.clone(),
        }
    } else {
        let (path, is_default) = match &args.engine_bin {
            Some(p) => (p.clone(), false),
            None => (root.join("target/release/gum-engine"), true),
        };
        crate::rig::ensure_engine_binary(&path, is_default).await?;
        EngineMode::Binary(std::fs::canonicalize(&path).unwrap_or(path))
    };

    let secret = match (&args.webhook_secret, &args.target) {
        (Some(s), _) => s.clone(),
        (None, Some(_)) => bail!(
            "--target requires --webhook-secret <the engine's webhook.signing_secret>, otherwise every webhook signature would be reported invalid"
        ),
        (None, None) => format!("bench-{}", uuid::Uuid::new_v4().simple()),
    };
    let sink = Sink::start(Some(&secret), None, false).await?;
    let rig_cfg = RigConfig {
        engine,
        chains: params.chains,
        signers: params.signers,
        block_time_s: params.block_time_s,
        anvil_balance_eth: 10_000,
        initial_signer_balance: params.initial_signer_balance_eth.map(eth),
        signer_min_balance: eth(params.signer_min_balance_eth),
        topup_amount: eth(params.topup_amount_eth),
        treasury_min_balance: eth(params.treasury_min_balance_eth),
        account_rps: params.account_rps,
        rpc_rtt_ms: params.rpc_rtt_ms,
        postgres_url: args.postgres_url.clone(),
        keep_db: args.keep_db,
        postgres_container: args.postgres_container.clone(),
        ready_timeout: Duration::from_secs(args.ready_timeout),
        webhook_secret: secret,
        run_dir: run_dir.clone(),
    };

    eprintln!(
        "[bench] scenario `{}`: {}",
        scenario.name(),
        scenario.about()
    );
    eprintln!("[bench] run dir: {}", run_dir.display());
    let started_wall = chrono::Utc::now();
    let rig = Arc::new(Rig::start(rig_cfg).await.context("starting the rig")?);
    eprintln!(
        "[bench] engine ready at {} ({})",
        rig.base_url, rig.engine_label
    );

    let result = run_inner(
        scenario.as_ref(),
        args,
        params,
        rig.clone(),
        sink,
        started_wall,
    )
    .await;
    let shutdown = rig.shutdown().await;
    let mut report = match result {
        Ok(r) => r,
        Err(e) => {
            return Err(e.context(format!(
                "scenario aborted; engine log tail ({}):\n{}",
                rig.engine_log.display(),
                rig.tail_engine_log(30)
            )))
        }
    };
    shutdown.context("rig shutdown failed (report not written: cleanup must be reliable)")?;

    report.finished_at = chrono::Utc::now().to_rfc3339();
    let path = report::write(&report, &results_dir)?;
    if args.save_baseline {
        if !report.verdict.pass {
            bail!(
                "refusing to save a baseline from a run with correctness violations (report: {})",
                path.display()
            );
        }
        let dir = args
            .baselines_dir
            .clone()
            .unwrap_or_else(|| root.join("bench/baselines"));
        std::fs::create_dir_all(&dir)?;
        let dst = dir.join(format!("{}.json", report.scenario));
        std::fs::copy(&path, &dst).with_context(|| format!("writing {}", dst.display()))?;
        eprintln!("[bench] baseline saved: {}", dst.display());
    }
    if !report.verdict.pass {
        eprintln!("[bench] engine log: {}", rig.engine_log.display());
    }
    Ok((report, path))
}

async fn run_inner(
    scenario: &dyn Scenario,
    args: &RunArgs,
    params: Params,
    rig: Arc<Rig>,
    sink: Sink,
    started_wall: chrono::DateTime<chrono::Utc>,
) -> Result<Report> {
    let clock = Clock::new();
    let client = rig.client.clone();
    let load_cfg = LoadConfig {
        mix: params.mix,
        chains: rig
            .chains
            .iter()
            .map(|c| ChainTarget {
                chain_id: c.chain_id,
                target: c.target,
            })
            .collect(),
        webhook_modes: params.webhook_modes.clone(),
        slow_ms: params.slow_ms,
        flaky_p: params.flaky_p,
        burn_iterations: params.burn_iterations,
        idem_fraction: params.idem_fraction,
        replay_fraction: params.replay_fraction,
        retry_with_idem: params.retry_with_idem,
        seed: params.seed,
    };

    let to = Duration::from_secs(5);
    let baseline = client
        .get_json::<AnalyticsResp>("/v1/analytics/transactions", to)
        .await
        .map(|a| a.totals);
    let mut pre_notes = Vec::new();
    let analytics_baseline = match baseline {
        Ok(c) => c,
        Err(e) => {
            pre_notes.push(format!("analytics baseline unavailable before load: {e:#}"));
            Default::default()
        }
    };

    let ctx = Ctx {
        load: Loadgen::new(client.clone(), sink.clone(), clock, load_cfg),
        sampler: Sampler::start(client.clone(), clock),
        rig: rig.clone(),
        clock,
        params: params.clone(),
        stop: AtomicBool::new(false),
        notes: Mutex::new(pre_notes),
        metrics: Mutex::new(BTreeMap::new()),
        violations: Mutex::new(Violations::default()),
    };

    let proxies = || -> Option<crate::proxy::ProxySnapshot> {
        let snaps: Vec<_> = rig
            .chains
            .iter()
            .filter_map(|c| c.proxy.as_ref().map(|p| p.snapshot()))
            .collect();
        (!snaps.is_empty()).then(|| report::merge_snapshots(&snaps))
    };

    // Without a proxy (--target) fall back to the engine's own meter, clearly labelled as such.
    let has_proxy = rig.chains.iter().any(|c| c.proxy.is_some());
    let engine_meter = || async {
        let c = client
            .get_json::<crate::api::ChainsResp>("/v1/chains", to)
            .await
            .ok()?;
        let mut snap = crate::proxy::ProxySnapshot::default();
        for chain in c.chains {
            for (m, n) in chain.rpc.by_method {
                snap.by_method.entry(m).or_default().calls += n;
                snap.total_calls += n;
            }
        }
        Some(snap)
    };

    // ---- load phase
    let rpc_at_load_start = if has_proxy {
        proxies()
    } else {
        engine_meter().await
    };
    let load_start = clock.now();
    scenario
        .drive(&ctx)
        .await
        .with_context(|| format!("scenario `{}` drive phase", scenario.name()))?;
    let load_end = clock.now();
    for c in &rig.chains {
        if let Some(p) = &c.proxy {
            p.clear_faults();
        }
    }
    if !ctx.load.wait_idle(Duration::from_secs(45)).await {
        bail!("{} POST request(s) still outstanding 45s after the load phase — harness HTTP timeouts are 30s, this should be impossible", ctx.load.inflight());
    }
    let mut jobs = ctx.load.take_records();
    eprintln!(
        "[bench] load phase done: {} scheduled, {} accepted; draining…",
        jobs.len(),
        jobs.iter().filter(|j| j.accepted()).count()
    );

    // ---- drain
    let drain = tracker::drain(
        &client,
        &mut jobs,
        Duration::from_secs(args.drain_stall_timeout),
        Duration::from_secs(args.drain_hard_timeout),
    )
    .await;
    let drain_end = clock.now();
    let rpc_at_drain_end = if has_proxy {
        proxies()
    } else {
        engine_meter().await
    };
    if let Some(r) = &drain.reason {
        eprintln!("[bench] drain gave up: {r}");
    }

    // ---- wait for webhook coverage (deliveries trail the status API, retries take time)
    let wh_deadline = Instant::now() + Duration::from_secs(args.webhook_wait);
    loop {
        let ds = sink.deliveries();
        let have: std::collections::HashSet<(&str, &str)> = ds
            .iter()
            .map(|d| (d.job_id.as_str(), d.event.as_str()))
            .collect();
        let missing = jobs
            .iter()
            .filter(|j| j.spec.webhook_mode.coverage_required())
            .filter_map(|j| {
                Some((
                    j.job_id()?,
                    j.final_status.as_ref().filter(|s| s.is_terminal())?,
                ))
            })
            .filter(|(id, fs)| {
                if fs.status == "confirmed" {
                    !have.contains(&(*id, "transaction.included"))
                        || !have.contains(&(*id, "transaction.confirmed"))
                } else {
                    !have.contains(&(*id, "transaction.failed"))
                }
            })
            .count();
        if missing == 0 || Instant::now() > wh_deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }

    // ---- ground truth + at-rest routes, retried until settled
    let signer_addrs = rig.accounts.signer_addresses();
    let treasury = rig.accounts.treasury.address();
    let mut accounts = signer_addrs.clone();
    accounts.push(treasury);
    let settle_deadline = Instant::now() + Duration::from_secs(args.settle_timeout);
    let (facts, at_rest_violations) = loop {
        let mut facts = Vec::new();
        for c in &rig.chains {
            facts.push(
                oracle::collect(&c.direct, c.chain_id, c.target, c.start_block, &accounts)
                    .await
                    .with_context(|| {
                        format!("oracle: reading ground truth from chain {}", c.chain_id)
                    })?,
            );
        }
        let rest = AtRest {
            analytics: client
                .get_json::<AnalyticsResp>("/v1/analytics/transactions", to)
                .await
                .map_err(|e| format!("{e:#}")),
            analytics_baseline,
            signers: client
                .get_json::<SignersResp>("/v1/signers", to)
                .await
                .map_err(|e| format!("{e:#}")),
            balances: client
                .get_json::<BalancesResp>("/v1/signers/balances", to)
                .await
                .map_err(|e| format!("{e:#}")),
        };
        // Re-read balances/nonces after the API call so both sides are from the same quiet period.
        let input = verdict::Input {
            jobs: &jobs,
            deliveries: &[],
            facts: &facts,
            signer_addrs: &signer_addrs,
            treasury,
        };
        let v = verdict::evaluate_at_rest(&input, &rest);
        if v.is_empty() || Instant::now() > settle_deadline {
            break (facts, v);
        }
        tokio::time::sleep(Duration::from_secs(1)).await;
    };
    ctx.sampler.stop();

    // ---- verdict
    let deliveries = sink.deliveries();
    let input = verdict::Input {
        jobs: &jobs,
        deliveries: &deliveries,
        facts: &facts,
        signer_addrs: &signer_addrs,
        treasury,
    };
    let mut violations = verdict::evaluate(&input);
    violations.extend(at_rest_violations);
    if let Some(msg) = rig.engine_exited().await {
        violations.add(
            "i",
            "engine_exited",
            "the engine process died during the run",
            "",
            msg,
        );
    }
    if !jobs.iter().any(|j| j.accepted()) {
        violations.add(
            "i",
            "nothing_accepted",
            "the engine did not accept a single job",
            "",
            format!("POST outcomes of {} jobs were all failures", jobs.len()),
        );
    }

    // ---- report
    let (_, unattributed) = verdict::attribute(&jobs, &facts);
    let signer_set: std::collections::HashSet<_> = signer_addrs.iter().collect();
    let extra_signer_txs = unattributed
        .iter()
        .filter(|t| signer_set.contains(&t.from))
        .count();
    let treasury_txs = unattributed.iter().filter(|t| t.from == treasury).count();
    let mut report = Report {
        schema_version: report::SCHEMA_VERSION,
        scenario: scenario.name().to_string(),
        params: serde_json::to_value(&params)?,
        started_at: started_wall.to_rfc3339(),
        git: Some(git_info()),
        machine: Some(machine()),
        engine: EngineInfo {
            mode: match &rig.cfg.engine {
                EngineMode::Binary(_) => "binary",
                EngineMode::Fake { .. } => "fake",
                EngineMode::Target { .. } => "target",
            }
            .into(),
            label: rig.engine_label.clone(),
            build_profile: rig.engine_profile.clone(),
            restarts: rig
                .engine_restarts
                .load(std::sync::atomic::Ordering::SeqCst),
        },
        drain,
        ..Default::default()
    };
    report::summarise(
        &mut report,
        report::Inputs {
            clock,
            jobs: &jobs,
            deliveries: &deliveries,
            samples: ctx.sampler.samples(),
            segments: ctx.load.segments(),
            load_start,
            load_end,
            drain_end,
            lag: ctx.load.lag_hist(),
            rpc_at_load_start,
            rpc_at_drain_end,
            rpc_source: if has_proxy {
                "proxy"
            } else {
                "engine_reported"
            },
            proc_samples: rig.proc_samples(),
        },
    );
    report.duration_s = round3(clock.now().as_secs_f64());
    report.notes = std::mem::take(&mut *ctx.notes.lock());
    report.notes.push(format!(
        "on-chain transactions not attributable to a bench job: {extra_signer_txs} from signers (cancels/no-ops), {treasury_txs} from the treasury (top-ups)"
    ));
    for f in &facts {
        report.notes.push(format!(
            "oracle: chain {} scanned blocks {}..={} — {} transactions, {} Hit logs, BenchTarget.total() = {}",
            f.chain_id,
            f.start_block,
            f.end_block,
            f.txs.len(),
            f.hit_logs.values().sum::<u64>(),
            f.contract_total
        ));
    }
    report.scenario_metrics = std::mem::take(&mut *ctx.metrics.lock());
    let failed_samples = report.series.iter().filter(|s| !s.ok).count();
    if failed_samples * 5 > report.series.len().max(1) && !scenario.needs_process_control() {
        report.harness_warnings.push(format!(
            "{failed_samples}/{} observability samples failed",
            report.series.len()
        ));
    }

    violations.extend(std::mem::take(&mut *ctx.violations.lock()));
    let mut scenario_v = Violations::default();
    scenario.assess(&ctx, &report, &mut scenario_v);
    violations.extend(scenario_v);
    let violations = violations.into_vec();
    report.verdict = Verdict {
        pass: violations.is_empty(),
        violations,
    };
    Ok(report)
}
