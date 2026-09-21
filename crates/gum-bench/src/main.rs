//! gum-bench — black-box load-testing, benchmarking and correctness harness for gum-engine.
//! It relies on nothing but docs/api-contract.md: the HTTP API, webhooks, the config file, and the chain.

mod api;
mod compare;
mod fake_engine;
mod keys;
mod loadgen;
mod oracle;
mod proxy;
mod report;
mod rig;
mod scenarios;
mod sink;
mod tracker;
mod util;
mod verdict;

use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "gum-bench",
    version,
    about = "Black-box benchmark + correctness harness for gum-engine"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Run a scenario end to end (rig → load → drain → oracle → verdict → report).
    Run(Box<RunArgs>),
    /// Compare a new report against a baseline; non-zero exit on regression.
    Compare(CompareArgs),
    /// List scenarios.
    Scenarios,
    /// Standalone webhook receiver for manual development (prints each event as a JSON line).
    Sink(SinkArgs),
    /// Minimal in-memory engine used to validate the harness itself.
    #[command(hide = true)]
    FakeEngine {
        /// double-send | skip-webhook | lose-job | bad-signature | wrong-analytics
        #[arg(long)]
        bug: Option<String>,
    },
}

#[derive(Args, Clone, Debug)]
pub struct RunArgs {
    /// smoke | steady | saturation | burst | mixed | webhook-hostile | funds | rpc-faults | chaos | soak
    pub scenario: String,

    /// Offered jobs per second (scenario specific meaning for ramps/bursts).
    #[arg(long)]
    pub rate: Option<f64>,
    /// Load duration in seconds.
    #[arg(long)]
    pub duration: Option<f64>,
    /// Number of engine signers (Anvil dev accounts 1..=N).
    #[arg(long)]
    pub signers: Option<usize>,
    /// Number of Anvil chains (1..=3 → chain ids 31337..).
    #[arg(long)]
    pub chains: Option<usize>,
    /// Job mix: `mixed`, `gas`, `nogas`, or weights like `hit_gas=3,hit_nogas=1,fail_gas=1,fail_nogas=1,transfer=1,burn_gas=0`.
    #[arg(long)]
    pub mix: Option<String>,
    /// Fraction of jobs sent with an Idempotency-Key.
    #[arg(long)]
    pub idem_fraction: Option<f64>,
    /// Fraction of keyed jobs that are deliberately replayed.
    #[arg(long)]
    pub replay_fraction: Option<f64>,
    /// RNG seed for the job mix (default 42; random with --target).
    #[arg(long)]
    pub seed: Option<u64>,
    /// Scenario specific knob, repeatable: --set kills=2 --set step_secs=15
    #[arg(long = "set", value_name = "KEY=VALUE")]
    pub set: Vec<String>,

    /// Engine binary to spawn (default: target/release/gum-engine, built if missing). `fake` = --fake-engine.
    #[arg(long)]
    pub engine_bin: Option<PathBuf>,
    /// Run against the built-in fake engine (harness self-validation).
    #[arg(long)]
    pub fake_engine: bool,
    /// Make the fake engine misbehave (implies --fake-engine): double-send | skip-webhook | lose-job | bad-signature | wrong-analytics
    #[arg(long)]
    pub fake_bug: Option<String>,
    /// Use an already running engine instead of spawning one (no process control, no counting proxy).
    #[arg(long)]
    pub target: Option<String>,
    /// With --target: `<chain_id>=<rpc url>` the oracle may use (repeatable).
    #[arg(long)]
    pub target_rpc: Vec<String>,
    /// With --target: funded key used to deploy BenchTarget (default: Anvil dev account signers+1).
    #[arg(long)]
    pub deployer_key: Option<String>,
    /// Webhook signing secret. Generated per run when the engine is spawned; required with --target.
    #[arg(long, env = "GUM_WEBHOOK_SECRET")]
    pub webhook_secret: Option<String>,

    /// Postgres server on which a fresh `gum_bench_<id>` database is created per run.
    #[arg(
        long,
        env = "GUM_BENCH_POSTGRES_URL",
        default_value = "postgres://localhost:5432/postgres"
    )]
    pub postgres_url: String,
    /// Do not drop the run database afterwards.
    #[arg(long)]
    pub keep_db: bool,
    /// Docker container running Postgres; enables the Postgres restart in `chaos`.
    #[arg(long)]
    pub postgres_container: Option<String>,

    /// Emulated WAN round trip between engine and RPC, in ms. Default 0 = no artificial latency anywhere.
    #[arg(long, default_value_t = 0)]
    pub rpc_rtt_ms: u64,
    /// `[rpc] account_rps` written into the engine config.
    #[arg(long, default_value_t = 500)]
    pub account_rps: u32,

    /// Seconds to wait for /healthz + /readyz.
    #[arg(long, default_value_t = 60)]
    pub ready_timeout: u64,
    /// Give up draining when no job made progress for this many seconds.
    #[arg(long, default_value_t = 60)]
    pub drain_stall_timeout: u64,
    /// Absolute limit for the drain phase, seconds.
    #[arg(long, default_value_t = 1800)]
    pub drain_hard_timeout: u64,
    /// Seconds to wait for outstanding webhook deliveries after all jobs are terminal.
    #[arg(long, default_value_t = 20)]
    pub webhook_wait: u64,
    /// Seconds the at-rest checks (analytics, balances, nonces) may take to settle.
    #[arg(long, default_value_t = 15)]
    pub settle_timeout: u64,

    /// Where reports go (default: <workspace>/bench/results).
    #[arg(long)]
    pub results_dir: Option<PathBuf>,
    /// Copy the report to bench/baselines/<scenario>.json (refused if the verdict fails).
    #[arg(long)]
    pub save_baseline: bool,
    #[arg(long)]
    pub baselines_dir: Option<PathBuf>,
    /// Do not print the markdown summary to stdout.
    #[arg(long)]
    pub quiet: bool,
}

#[derive(Args)]
struct CompareArgs {
    baseline: PathBuf,
    new: PathBuf,
    /// Allowed throughput drop (fraction).
    #[arg(long, default_value_t = 0.05)]
    throughput_tolerance: f64,
    /// Allowed p99 growth (fraction) for accept latency and accept→included.
    #[arg(long, default_value_t = 0.10)]
    p99_tolerance: f64,
    /// Allowed growth of RPC calls/tx (fraction). Default 0 = strict, compared to 3 decimals.
    #[arg(long, default_value_t = 0.0)]
    rpc_tolerance: f64,
    /// Skip the throughput and latency gates.
    #[arg(long)]
    ignore_latency: bool,
    /// Apply throughput/latency gates even when machine fingerprints differ.
    #[arg(long)]
    force_latency: bool,
}

#[derive(Args)]
struct SinkArgs {
    #[arg(long, default_value_t = 9900)]
    port: u16,
    /// Verify X-Gum-Signature with this secret (the engine's webhook.signing_secret).
    #[arg(long, env = "GUM_WEBHOOK_SECRET")]
    secret: Option<String>,
}

fn main() -> ExitCode {
    util::raise_nofile_limit();
    rig::install_panic_hook();
    let cli = Cli::parse();
    let rt = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("error: could not start the async runtime: {e}");
            return ExitCode::from(2);
        }
    };
    let code = rt.block_on(async {
        tokio::select! {
            code = dispatch(cli) => code,
            _ = shutdown_signal() => {
                eprintln!("\n[bench] interrupted — killing child processes and dropping the run database");
                rig::emergency_cleanup().await;
                ExitCode::from(130)
            }
        }
    });
    rig::kill_all_children();
    // Detached tasks (proxy, sink, held connections) must not keep the process alive.
    rt.shutdown_background();
    code
}

async fn shutdown_signal() {
    use tokio::signal::unix::{signal, SignalKind};
    let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
    let mut int = signal(SignalKind::interrupt()).expect("SIGINT handler");
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }
}

/// Exit codes: 0 = clean, 1 = correctness violations / regression, 2 = the harness itself failed.
async fn dispatch(cli: Cli) -> ExitCode {
    match cli.cmd {
        Cmd::Run(args) => match scenarios::run(&args).await {
            Ok((report, path)) => {
                if !args.quiet {
                    println!("{}", report::markdown(&report));
                }
                eprintln!("[bench] report: {}", path.display());
                if report.verdict.pass {
                    eprintln!("[bench] verdict: PASS");
                    ExitCode::SUCCESS
                } else {
                    eprintln!(
                        "[bench] verdict: FAIL ({} violation class(es))",
                        report.verdict.violations.len()
                    );
                    ExitCode::from(1)
                }
            }
            Err(e) => {
                eprintln!("error: harness failure (no report written): {e:#}");
                rig::emergency_cleanup().await;
                ExitCode::from(2)
            }
        },
        Cmd::Compare(a) => {
            let loaded = report::load(&a.baseline).and_then(|b| Ok((b, report::load(&a.new)?)));
            match loaded {
                Ok((base, new)) => {
                    let c = compare::compare(
                        &base,
                        &new,
                        &compare::CompareOpts {
                            throughput_tolerance: a.throughput_tolerance,
                            p99_tolerance: a.p99_tolerance,
                            rpc_tolerance: a.rpc_tolerance,
                            ignore_latency: a.ignore_latency,
                            force_latency: a.force_latency,
                        },
                    );
                    print!("{}", c.render());
                    if c.failed() {
                        ExitCode::from(1)
                    } else {
                        ExitCode::SUCCESS
                    }
                }
                Err(e) => {
                    eprintln!("error: {e:#}");
                    ExitCode::from(2)
                }
            }
        }
        Cmd::Scenarios => {
            for s in scenarios::all() {
                println!("{:<16} {}", s.name(), s.about());
            }
            ExitCode::SUCCESS
        }
        Cmd::Sink(a) => match sink::Sink::start(a.secret.as_deref(), Some(a.port), true).await {
            Ok(s) => {
                eprintln!(
                    "[sink] listening on http://{} — paths: /ok  /slow?ms=  /fail  /flaky?p=  (signature check: {})",
                    s.addr(sink::SinkMode::Ok).expect("listener"),
                    if a.secret.is_some() { "on" } else { "off, pass --secret" }
                );
                std::future::pending::<()>().await;
                ExitCode::SUCCESS
            }
            Err(e) => {
                eprintln!("error: {e:#}");
                ExitCode::from(2)
            }
        },
        Cmd::FakeEngine { bug } => match fake_engine::run(bug).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(e) => {
                eprintln!("error: fake-engine: {e:#}");
                ExitCode::from(2)
            }
        },
    }
}
