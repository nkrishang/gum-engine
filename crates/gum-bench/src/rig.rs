//! Run orchestration: Anvil processes, the per-run Postgres database, the generated engine config, the
//! engine subprocess (spawn / kill -9 / restart), process sampling and cleanup.
//!
//! Nothing here adds latency: Anvil is started with exactly
//! `--port --block-time --chain-id --accounts --balance --state` (+ `--silent`).

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, U256};
use anyhow::{anyhow, bail, Context, Result};
use parking_lot::Mutex;
use serde::Serialize;
use tokio::process::{Child, Command};

use crate::api::EngineClient;
use crate::keys::{private_key_hex, DevAccounts};
use crate::oracle::{self, DirectProvider};
use crate::proxy::Proxy;
use crate::util::{free_port, tail_file};

// ---------------------------------------------------------------------------------------------------
// Global child registry so a panic or ctrl-c never leaves Anvil / the engine running.
// ---------------------------------------------------------------------------------------------------

static CHILD_PIDS: Mutex<Vec<u32>> = Mutex::new(Vec::new());
static DB_CLEANUP: Mutex<Option<(String, String)>> = Mutex::new(None); // (admin url, db name)

fn register_pid(pid: u32) {
    CHILD_PIDS.lock().push(pid);
}

fn unregister_pid(pid: u32) {
    CHILD_PIDS.lock().retain(|p| *p != pid);
}

/// SIGKILL every child we started. Safe to call from a panic hook / signal path.
pub fn kill_all_children() {
    // try_lock: never deadlock inside a panic hook.
    if let Some(pids) = CHILD_PIDS.try_lock() {
        for pid in pids.iter() {
            unsafe {
                libc::kill(*pid as i32, libc::SIGKILL);
            }
        }
    }
}

pub fn install_panic_hook() {
    let prev = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        kill_all_children();
        prev(info);
    }));
}

/// Emergency cleanup for ctrl-c: kill children and drop the run database.
pub async fn emergency_cleanup() {
    kill_all_children();
    let db = DB_CLEANUP.lock().take();
    if let Some((admin_url, name)) = db {
        if let Err(e) = drop_database(&admin_url, &name).await {
            eprintln!("[rig] could not drop database {name}: {e:#}");
        } else {
            eprintln!("[rig] dropped database {name}");
        }
    }
}

// ---------------------------------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize)]
pub enum EngineMode {
    /// Spawn this binary with `GUM_CONFIG` + `PORT`.
    Binary(PathBuf),
    /// Spawn `gum-bench fake-engine` (harness self-validation).
    Fake { bug: Option<String> },
    /// Use an engine somebody else runs. No process control, no counting proxy.
    Target {
        base_url: String,
        rpc_urls: Vec<(u64, String)>,
        deployer_key: Option<String>,
    },
}

#[derive(Clone, Debug, Serialize)]
pub struct RigConfig {
    pub engine: EngineMode,
    pub chains: usize,
    pub signers: usize,
    pub block_time_s: u64,
    pub anvil_balance_eth: u64,
    /// If set, every signer's balance is forced to this value (wei) before the engine boots.
    pub initial_signer_balance: Option<U256>,
    pub signer_min_balance: U256,
    pub topup_amount: U256,
    pub treasury_min_balance: U256,
    pub account_rps: u32,
    pub rpc_rtt_ms: u64,
    pub postgres_url: String,
    pub keep_db: bool,
    pub postgres_container: Option<String>,
    pub ready_timeout: Duration,
    pub webhook_secret: String,
    pub run_dir: PathBuf,
}

pub struct ChainRig {
    pub chain_id: u64,
    pub name: String,
    pub anvil_url: String,
    pub direct: DirectProvider,
    pub proxy: Option<Proxy>,
    pub target: Address,
    pub start_block: u64,
    anvil_port: u16,
    state_file: PathBuf,
    anvil: tokio::sync::Mutex<Option<Child>>,
}

struct EngineProc {
    child: Child,
    pid: u32,
}

pub struct Rig {
    pub cfg: RigConfig,
    pub accounts: DevAccounts,
    pub chains: Vec<ChainRig>,
    pub client: EngineClient,
    pub base_url: String,
    pub engine_log: PathBuf,
    pub engine_profile: String,
    pub engine_label: String,
    engine_port: u16,
    config_path: PathBuf,
    db: Option<(String, String, String)>, // (admin url, db name, db url)
    engine: tokio::sync::Mutex<Option<EngineProc>>,
    engine_pid: Arc<AtomicU32>,
    sampler_stop: Arc<AtomicBool>,
    samples: Arc<Mutex<Vec<ProcSample>>>,
    pub engine_restarts: AtomicU32,
}

#[derive(Clone, Copy, Debug, Serialize)]
pub struct ProcSample {
    pub t_s: f64,
    /// Percent of one core (can exceed 100 on multi-core).
    pub cpu_pct: f32,
    pub rss_bytes: u64,
}

impl Rig {
    pub fn has_process_control(&self) -> bool {
        !matches!(self.cfg.engine, EngineMode::Target { .. })
    }

    pub async fn start(cfg: RigConfig) -> Result<Rig> {
        std::fs::create_dir_all(&cfg.run_dir)
            .with_context(|| format!("creating {}", cfg.run_dir.display()))?;
        let accounts = DevAccounts::derive(cfg.signers)?;
        let mut chains = Vec::new();

        match &cfg.engine {
            EngineMode::Target {
                rpc_urls,
                deployer_key,
                ..
            } => {
                if rpc_urls.is_empty() {
                    bail!("--target requires at least one --target-rpc <chain_id>=<url> so the oracle can read the chain");
                }
                let deployer = match deployer_key {
                    Some(k) => k
                        .parse()
                        .context("--deployer-key is not a valid private key")?,
                    None => accounts.deployer.clone(),
                };
                for (chain_id, url) in rpc_urls {
                    let direct = oracle::provider(url)?;
                    let target = oracle::deploy(&direct, *chain_id, &deployer)
                        .await
                        .with_context(|| {
                            format!("deploying BenchTarget to chain {chain_id} at {url}")
                        })?;
                    let start_block = oracle::block_number(&direct).await?;
                    chains.push(ChainRig {
                        chain_id: *chain_id,
                        name: format!("chain{chain_id}"),
                        anvil_url: url.clone(),
                        direct,
                        proxy: None,
                        target,
                        start_block,
                        anvil_port: 0,
                        state_file: PathBuf::new(),
                        anvil: tokio::sync::Mutex::new(None),
                    });
                }
            }
            _ => {
                which("anvil").context(
                    "`anvil` not found on PATH (install foundry: https://getfoundry.sh)",
                )?;
                for i in 0..cfg.chains {
                    let chain_id = 31337 + i as u64;
                    let port = free_port()?;
                    let state_file = cfg.run_dir.join(format!("anvil-{chain_id}.state.json"));
                    let child = spawn_anvil(&cfg, &accounts, chain_id, port, &state_file)?;
                    let anvil_url = format!("http://127.0.0.1:{port}");
                    let direct = oracle::provider(&anvil_url)?;
                    wait_anvil(&direct, chain_id, Duration::from_secs(20)).await?;
                    let target = oracle::deploy(&direct, chain_id, &accounts.deployer).await?;
                    if let Some(bal) = cfg.initial_signer_balance {
                        for s in accounts.signer_addresses() {
                            oracle::set_balance(&direct, s, bal).await?;
                        }
                    }
                    let start_block = oracle::block_number(&direct).await?;
                    let proxy =
                        Proxy::start(&anvil_url, Duration::from_millis(cfg.rpc_rtt_ms)).await?;
                    chains.push(ChainRig {
                        chain_id,
                        name: format!("anvil{chain_id}"),
                        anvil_url,
                        direct,
                        proxy: Some(proxy),
                        target,
                        start_block,
                        anvil_port: port,
                        state_file,
                        anvil: tokio::sync::Mutex::new(Some(child)),
                    });
                }
            }
        }

        let engine_log = cfg.run_dir.join("engine.log");
        let config_path = cfg.run_dir.join("engine.toml");
        let (base_url, engine_port, db, engine_profile, engine_label) = match &cfg.engine {
            EngineMode::Target { base_url, .. } => (
                base_url.trim_end_matches('/').to_string(),
                0,
                None,
                "unknown (external target)".to_string(),
                format!("target {base_url}"),
            ),
            mode => {
                let db = create_database(&cfg.postgres_url).await?;
                let port = free_port()?;
                let toml = engine_config_toml(&cfg, &accounts, &chains, port, &db.2);
                std::fs::write(&config_path, toml)
                    .with_context(|| format!("writing {}", config_path.display()))?;
                let (profile, label) = match mode {
                    EngineMode::Binary(p) => (profile_of(p), p.display().to_string()),
                    EngineMode::Fake { bug } => (
                        "fake".to_string(),
                        format!(
                            "fake-engine{}",
                            bug.as_ref()
                                .map(|b| format!(" (bug: {b})"))
                                .unwrap_or_default()
                        ),
                    ),
                    EngineMode::Target { .. } => unreachable!(),
                };
                (
                    format!("http://127.0.0.1:{port}"),
                    port,
                    Some(db),
                    profile,
                    label,
                )
            }
        };

        let rig = Rig {
            client: EngineClient::new(&base_url)?,
            base_url,
            cfg,
            accounts,
            chains,
            engine_log,
            engine_profile,
            engine_label,
            engine_port,
            config_path,
            db,
            engine: tokio::sync::Mutex::new(None),
            engine_pid: Arc::new(AtomicU32::new(0)),
            sampler_stop: Arc::new(AtomicBool::new(false)),
            samples: Arc::new(Mutex::new(Vec::new())),
            engine_restarts: AtomicU32::new(0),
        };
        if rig.has_process_control() {
            rig.start_engine().await?;
            rig.start_sampler();
        } else {
            rig.wait_ready().await?;
        }
        Ok(rig)
    }

    /// Spawn the engine and wait for `/healthz` then `/readyz`.
    pub async fn start_engine(&self) -> Result<()> {
        let mut guard = self.engine.lock().await;
        if guard.is_some() {
            bail!("engine is already running");
        }
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.engine_log)
            .with_context(|| format!("opening {}", self.engine_log.display()))?;
        let mut cmd = match &self.cfg.engine {
            EngineMode::Binary(p) => Command::new(p),
            EngineMode::Fake { bug } => {
                let mut c =
                    Command::new(std::env::current_exe().context("locating the gum-bench binary")?);
                c.arg("fake-engine");
                if let Some(b) = bug {
                    c.arg("--bug").arg(b);
                }
                c
            }
            EngineMode::Target { .. } => bail!("no process control in --target mode"),
        };
        // The contract lets GUM_* / DATABASE_URL override the file: scrub the inherited environment so the
        // generated config is what the engine actually runs with.
        for (k, _) in std::env::vars() {
            if k.starts_with("GUM_") {
                cmd.env_remove(k);
            }
        }
        let db_url = &self
            .db
            .as_ref()
            .expect("spawned engine always has a database")
            .2;
        cmd.env("GUM_CONFIG", &self.config_path)
            .env("PORT", self.engine_port.to_string())
            .env("DATABASE_URL", db_url)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone()?))
            .stderr(Stdio::from(log))
            .kill_on_drop(true);
        let child = cmd
            .spawn()
            .with_context(|| format!("spawning the engine ({})", self.engine_label))?;
        let pid = child
            .id()
            .ok_or_else(|| anyhow!("engine exited immediately"))?;
        register_pid(pid);
        self.engine_pid.store(pid, Ordering::SeqCst);
        *guard = Some(EngineProc { child, pid });
        drop(guard);
        self.wait_ready().await
    }

    async fn wait_ready(&self) -> Result<()> {
        let deadline = Instant::now() + self.cfg.ready_timeout;
        let mut healthy = false;
        let mut last;
        loop {
            if self.has_process_control() {
                let mut g = self.engine.lock().await;
                if let Some(p) = g.as_mut() {
                    if let Ok(Some(status)) = p.child.try_wait() {
                        unregister_pid(p.pid);
                        *g = None;
                        bail!(
                            "engine exited during startup with {status}\n--- tail of {} ---\n{}",
                            self.engine_log.display(),
                            tail_file(&self.engine_log, 40)
                        );
                    }
                }
            }
            let path = if healthy { "/readyz" } else { "/healthz" };
            match self
                .client
                .get_status_code(path, Duration::from_secs(2))
                .await
            {
                Ok(200) if healthy => return Ok(()),
                Ok(200) => {
                    healthy = true;
                    continue;
                }
                Ok(code) => last = format!("GET {path} -> {code}"),
                Err(e) => last = format!("GET {path}: {e}"),
            }
            if Instant::now() > deadline {
                bail!(
                    "engine at {} not ready after {:?} (last: {last})\n--- tail of {} ---\n{}",
                    self.base_url,
                    self.cfg.ready_timeout,
                    self.engine_log.display(),
                    tail_file(&self.engine_log, 40)
                );
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// `kill -9` the engine. No graceful anything.
    pub async fn kill_engine(&self) -> Result<()> {
        let mut g = self.engine.lock().await;
        let mut p = g.take().ok_or_else(|| anyhow!("engine is not running"))?;
        p.child.start_kill().context("SIGKILL engine")?;
        let _ = p.child.wait().await;
        unregister_pid(p.pid);
        self.engine_pid.store(0, Ordering::SeqCst);
        Ok(())
    }

    pub async fn restart_engine(&self) -> Result<()> {
        self.start_engine().await?;
        self.engine_restarts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    /// Returns an error if the engine process died on its own (always a finding worth surfacing).
    pub async fn engine_exited(&self) -> Option<String> {
        let mut g = self.engine.lock().await;
        let p = g.as_mut()?;
        match p.child.try_wait() {
            Ok(Some(status)) => Some(format!("engine process exited unexpectedly: {status}")),
            _ => None,
        }
    }

    /// Stop one Anvil gracefully (SIGTERM → it dumps `--state`), keeping its port reserved for restart.
    pub async fn stop_anvil(&self, chain_idx: usize) -> Result<()> {
        let chain = &self.chains[chain_idx];
        let mut g = chain.anvil.lock().await;
        let mut child = g
            .take()
            .ok_or_else(|| anyhow!("anvil for chain {} is not running", chain.chain_id))?;
        let pid = child.id();
        if let Some(pid) = pid {
            unsafe {
                libc::kill(pid as i32, libc::SIGTERM);
            }
        }
        match tokio::time::timeout(Duration::from_secs(15), child.wait()).await {
            Ok(_) => {}
            Err(_) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                bail!("anvil (chain {}) did not exit on SIGTERM within 15s; state may not have been dumped", chain.chain_id);
            }
        }
        if let Some(pid) = pid {
            unregister_pid(pid);
        }
        if !chain.state_file.exists() {
            bail!(
                "anvil (chain {}) exited without writing {}",
                chain.chain_id,
                chain.state_file.display()
            );
        }
        Ok(())
    }

    /// Start a previously stopped Anvil on the same port, loading the dumped state.
    pub async fn start_anvil(&self, chain_idx: usize) -> Result<()> {
        let chain = &self.chains[chain_idx];
        let mut g = chain.anvil.lock().await;
        if g.is_some() {
            bail!("anvil for chain {} is already running", chain.chain_id);
        }
        let child = spawn_anvil(
            &self.cfg,
            &self.accounts,
            chain.chain_id,
            chain.anvil_port,
            &chain.state_file,
        )?;
        *g = Some(child);
        drop(g);
        wait_anvil(&chain.direct, chain.chain_id, Duration::from_secs(20)).await?;
        let code = alloy::providers::Provider::get_code_at(&chain.direct, chain.target).await?;
        if code.is_empty() {
            bail!(
                "anvil (chain {}) restarted without its state: BenchTarget is gone",
                chain.chain_id
            );
        }
        Ok(())
    }

    /// Restart Postgres — only possible when it runs in a container we were told about.
    pub async fn restart_postgres(&self) -> Result<bool> {
        match &self.cfg.postgres_container {
            None => {
                eprintln!("[rig] postgres restart skipped: host Postgres is not ours to restart (pass --postgres-container <name>)");
                Ok(false)
            }
            Some(name) => {
                let out = Command::new("docker")
                    .args(["restart", name])
                    .output()
                    .await
                    .context("running `docker restart`")?;
                if !out.status.success() {
                    bail!(
                        "docker restart {name} failed: {}",
                        String::from_utf8_lossy(&out.stderr)
                    );
                }
                Ok(true)
            }
        }
    }

    fn start_sampler(&self) {
        let pid_cell = self.engine_pid.clone();
        let stop = self.sampler_stop.clone();
        let samples = self.samples.clone();
        std::thread::Builder::new()
            .name("proc-sampler".into())
            .spawn(move || {
                use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System};
                let mut sys = System::new();
                let started = Instant::now();
                let kind = ProcessRefreshKind::nothing().with_cpu().with_memory();
                let mut last_pid = 0;
                while !stop.load(Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_secs(1));
                    let pid = pid_cell.load(Ordering::SeqCst);
                    if pid == 0 {
                        continue;
                    }
                    let spid = Pid::from_u32(pid);
                    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[spid]), true, kind);
                    if pid != last_pid {
                        // First refresh of a new process has no CPU delta yet.
                        last_pid = pid;
                        continue;
                    }
                    if let Some(p) = sys.process(spid) {
                        samples.lock().push(ProcSample {
                            t_s: started.elapsed().as_secs_f64(),
                            cpu_pct: p.cpu_usage(),
                            rss_bytes: p.memory(),
                        });
                    }
                }
            })
            .expect("spawning the sampler thread");
    }

    pub fn proc_samples(&self) -> Vec<ProcSample> {
        self.samples.lock().clone()
    }

    /// Tear everything down. Returns errors instead of panicking so the report can still be written.
    pub async fn shutdown(&self) -> Result<()> {
        self.sampler_stop.store(true, Ordering::Relaxed);
        if let Some(mut p) = self.engine.lock().await.take() {
            // SIGTERM first (the contract promises a graceful drain), SIGKILL after 5s.
            unsafe {
                libc::kill(p.pid as i32, libc::SIGTERM);
            }
            if tokio::time::timeout(Duration::from_secs(5), p.child.wait())
                .await
                .is_err()
            {
                let _ = p.child.start_kill();
                let _ = p.child.wait().await;
            }
            unregister_pid(p.pid);
        }
        for c in &self.chains {
            if let Some(mut child) = c.anvil.lock().await.take() {
                let pid = child.id();
                let _ = child.start_kill();
                let _ = child.wait().await;
                if let Some(pid) = pid {
                    unregister_pid(pid);
                }
            }
        }
        if let Some((admin, name, _)) = &self.db {
            if self.cfg.keep_db {
                eprintln!("[rig] keeping database {name} (--keep-db)");
                DB_CLEANUP.lock().take();
            } else {
                drop_database(admin, name).await?;
                DB_CLEANUP.lock().take();
            }
        }
        Ok(())
    }

    pub fn tail_engine_log(&self, n: usize) -> String {
        tail_file(&self.engine_log, n)
    }
}

fn which(bin: &str) -> Result<PathBuf> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    std::env::split_paths(&path)
        .map(|d| d.join(bin))
        .find(|p| p.is_file())
        .ok_or_else(|| anyhow!("{bin} not found on PATH"))
}

fn spawn_anvil(
    cfg: &RigConfig,
    accounts: &DevAccounts,
    chain_id: u64,
    port: u16,
    state_file: &Path,
) -> Result<Child> {
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(cfg.run_dir.join(format!("anvil-{chain_id}.log")))?;
    let child = Command::new("anvil")
        .args(["--port", &port.to_string()])
        .args(["--block-time", &cfg.block_time_s.to_string()])
        .args(["--chain-id", &chain_id.to_string()])
        .args(["--accounts", &accounts.anvil_accounts().to_string()])
        .args(["--balance", &cfg.anvil_balance_eth.to_string()])
        .arg("--state")
        .arg(state_file)
        .arg("--silent")
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log))
        .kill_on_drop(true)
        .spawn()
        .context("spawning anvil")?;
    if let Some(pid) = child.id() {
        register_pid(pid);
    }
    Ok(child)
}

async fn wait_anvil(p: &DirectProvider, chain_id: u64, timeout: Duration) -> Result<()> {
    use alloy::providers::Provider;
    let deadline = Instant::now() + timeout;
    loop {
        match p.get_chain_id().await {
            Ok(id) if id == chain_id => return Ok(()),
            Ok(id) => {
                bail!("anvil answered with chain id {id}, expected {chain_id} (port collision?)")
            }
            Err(e) if Instant::now() > deadline => {
                bail!("anvil (chain {chain_id}) did not come up within {timeout:?}: {e}")
            }
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

fn profile_of(bin: &Path) -> String {
    let s = bin.to_string_lossy();
    if s.contains("/release/") {
        "release".into()
    } else if s.contains("/debug/") {
        "debug".into()
    } else {
        "unknown".into()
    }
}

/// The engine config, exactly per the "Engine process contract" section of docs/api-contract.md.
pub fn engine_config_toml(
    cfg: &RigConfig,
    accounts: &DevAccounts,
    chains: &[ChainRig],
    port: u16,
    db_url: &str,
) -> String {
    let keys: Vec<String> = accounts
        .signers
        .iter()
        .map(|s| format!("\"{}\"", private_key_hex(s)))
        .collect();
    let mut out = format!(
        "# generated by gum-bench — do not edit\n\
         [server]\nport = {port}\n\n\
         [database]\nurl = \"{db_url}\"\nauto_migrate = true\n\n\
         [webhook]\nsigning_secret = \"{secret}\"\nallow_private_hosts = true\n\n\
         [rpc]\naccount_rps = {rps}\n\n\
         [signers]\nmode = \"local\"\nlocal_private_keys = [{keys}]\nkms_key_ids = []\n",
        secret = cfg.webhook_secret,
        rps = cfg.account_rps,
        keys = keys.join(", "),
    );
    for c in chains {
        let rpc = c
            .proxy
            .as_ref()
            .map(|p| p.url())
            .unwrap_or_else(|| c.anvil_url.clone());
        out.push_str(&format!(
            "\n[chains.{name}]\nchain_id = {id}\nkind = \"geth\"\nrpc_url = \"{rpc}\"\n\
             treasury_private_key = \"{tkey}\"\nsigner_min_balance = \"{min}\"\ntopup_amount = \"{topup}\"\n\
             treasury_min_balance = \"{tmin}\"\n",
            name = c.name,
            id = c.chain_id,
            tkey = private_key_hex(&accounts.treasury),
            min = cfg.signer_min_balance,
            topup = cfg.topup_amount,
            tmin = cfg.treasury_min_balance,
        ));
    }
    out
}

// ---------------------------------------------------------------------------------------------------
// Postgres
// ---------------------------------------------------------------------------------------------------

async fn pg_connect(url: &str) -> Result<tokio_postgres::Client> {
    let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
        .await
        .with_context(|| format!("connecting to Postgres at {}", redact(url)))?;
    tokio::spawn(async move {
        let _ = conn.await;
    });
    Ok(client)
}

fn redact(url: &str) -> String {
    match url::Url::parse(url) {
        Ok(mut u) => {
            if u.password().is_some() {
                let _ = u.set_password(Some("***"));
            }
            u.to_string()
        }
        Err(_) => "<unparseable url>".into(),
    }
}

/// Make the user explicit so the engine's Postgres driver and ours agree on who connects.
fn with_explicit_user(url: &str) -> Result<url::Url> {
    let mut u =
        url::Url::parse(url).with_context(|| "--postgres-url is not a valid URL".to_string())?;
    if u.username().is_empty() {
        let user = std::env::var("PGUSER")
            .or_else(|_| std::env::var("USER"))
            .unwrap_or_else(|_| "postgres".into());
        u.set_username(&user)
            .map_err(|_| anyhow!("cannot set a username on --postgres-url"))?;
    }
    Ok(u)
}

/// Create `gum_bench_<id>`; returns (admin url, db name, db url).
async fn create_database(postgres_url: &str) -> Result<(String, String, String)> {
    let admin = with_explicit_user(postgres_url)?;
    let name = format!(
        "gum_bench_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..10]
    );
    let client = pg_connect(admin.as_str()).await?;
    client
        .batch_execute(&format!("CREATE DATABASE \"{name}\""))
        .await
        .with_context(|| format!("CREATE DATABASE {name}"))?;
    let mut db = admin.clone();
    db.set_path(&format!("/{name}"));
    *DB_CLEANUP.lock() = Some((admin.to_string(), name.clone()));
    Ok((admin.to_string(), name, db.to_string()))
}

async fn drop_database(admin_url: &str, name: &str) -> Result<()> {
    let client = pg_connect(admin_url).await?;
    client
        .batch_execute(&format!("DROP DATABASE IF EXISTS \"{name}\" WITH (FORCE)"))
        .await
        .with_context(|| format!("DROP DATABASE {name}"))?;
    Ok(())
}

/// Default engine binary; built on demand.
pub async fn ensure_engine_binary(path: &Path, is_default: bool) -> Result<()> {
    if path.is_file() {
        return Ok(());
    }
    if !is_default {
        bail!("--engine-bin {} does not exist", path.display());
    }
    eprintln!(
        "[rig] {} not found — running `cargo build --release -p gum-engine`",
        path.display()
    );
    let status = Command::new("cargo")
        .args(["build", "--release", "-p", "gum-engine"])
        .status()
        .await
        .context("running cargo build")?;
    if !status.success() || !path.is_file() {
        bail!(
            "could not build the engine (cargo build --release -p gum-engine → {status}); \
             pass --engine-bin <path>, --fake-engine, or --target <url>"
        );
    }
    Ok(())
}

pub fn eth(n: f64) -> U256 {
    U256::from((n * 1e9) as u128) * U256::from(1_000_000_000u64)
}
