//! gum-engine entry point.
//!
//! Boot order: config → logging → database → HTTP API (serving immediately, so jobs are accepted and
//! durable even while this instance is only a standby) → leader lease → signers → state rebuild → workers.
//!
//! Shutdown (SIGTERM): stop taking new work → let in-flight sends settle (bounded) → flush write-behind →
//! release the lease → exit. A SIGKILL is equally safe — every broadcast was persisted first — it only
//! makes the next leader wait for Postgres to notice the dead session.

use std::{
    collections::BTreeMap,
    net::{Ipv6Addr, SocketAddr},
    sync::atomic::{AtomicBool, AtomicI64},
    sync::Arc,
    time::Duration,
};

use gum_engine::{
    api,
    chain::{registry, ChainCtx},
    config::Config,
    engine::Engine,
    leader, pipeline,
    rpc::{Limiter, RpcClient, RpcClientParams},
    signer::SignerPool,
    stats::Stats,
    store::{batcher::IngestBatcher, lease::Lease, Store},
    telemetry, webhook,
};
use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;

#[tokio::main]
async fn main() {
    telemetry::init_tracing();
    if let Err(e) = run().await {
        tracing::error!(alert = true, event = "engine.fatal", error = %format!("{e:#}"), "gum-engine cannot continue");
        // Give Railway's restart policy something to back off against instead of a tight crash loop.
        tokio::time::sleep(Duration::from_secs(2)).await;
        std::process::exit(1);
    }
}

async fn run() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    if std::env::args().nth(1).as_deref() == Some("migrate") {
        let store = Store::connect_with_retry(&cfg.database, Duration::from_secs(60)).await?;
        store.migrate().await?;
        tracing::info!(event = "migrate.done", "database migrations applied");
        return Ok(());
    }

    let prometheus = telemetry::init_metrics()?;
    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .connect_timeout(Duration::from_secs(5))
        .pool_idle_timeout(Duration::from_secs(60))
        .tcp_keepalive(Duration::from_secs(30))
        .build()?;
    telemetry::init_alert_sink(cfg.alerts.webhook_url.clone(), http.clone());

    let store = Store::connect_with_retry(&cfg.database, Duration::from_secs(120)).await?;
    if cfg.database.auto_migrate {
        store.migrate().await?;
    }

    // Chains: adapter + tunables + RPC client each. No network calls yet.
    let limiter = Limiter::spawn(cfg.rpc.account_rps, cfg.rpc.reserved_p1_rps, cfg.rpc.reserved_p2_rps);
    let mut chains = BTreeMap::new();
    for (name, chain_cfg) in &cfg.chains {
        let adapter = registry::adapter_for(&chain_cfg.kind)
            .ok_or_else(|| anyhow::anyhow!("chain `{name}`: unknown kind `{}` (known kinds: {})", chain_cfg.kind, registry::known_kinds().join(", ")))?;
        let tunables = adapter.defaults().with_overrides(&chain_cfg.overrides(name)?);
        let rpc = RpcClient::new(RpcClientParams {
            chain_id: chain_cfg.chain_id,
            chain_name: name.clone(),
            url: chain_cfg.resolve_rpc_url(name)?,
            token: chain_cfg.resolve_rpc_token(),
            limiter: limiter.clone(),
            credits_per_call: tunables.credits_per_call,
            request_timeout: Duration::from_millis(cfg.rpc.request_timeout_ms),
            connect_timeout: Duration::from_millis(cfg.rpc.connect_timeout_ms),
            failure_threshold: tunables.rpc_failure_threshold,
        })?;
        let ctx = ChainCtx::new(name.clone(), chain_cfg.clone(), adapter, tunables, Arc::new(rpc), cfg.queue.memory_window);
        chains.insert(chain_cfg.chain_id, Arc::new(ctx));
    }

    let shutdown = CancellationToken::new();
    let engine = Arc::new(Engine {
        ingest: IngestBatcher::spawn(store.clone()),
        store,
        stats: Stats::default(),
        chains,
        pairs: RwLock::new(BTreeMap::new()),
        treasuries: RwLock::new(BTreeMap::new()),
        settlers: RwLock::new(BTreeMap::new()),
        confirmers: RwLock::new(BTreeMap::new()),
        leader: AtomicBool::new(false),
        epoch: AtomicI64::new(-1),
        shutdown: shutdown.clone(),
        stop_sending: CancellationToken::new(),
        prometheus,
        http,
        global_pause: AtomicBool::new(false),
        webhook_wake: tokio::sync::Notify::new(),
        ingest_gate: tokio::sync::RwLock::new(()),
        cfg,
    });

    // API first. `[::]` accepts IPv4 and IPv6, which Railway's private network requires.
    let addr = SocketAddr::from((Ipv6Addr::UNSPECIFIED, engine.cfg.server.port));
    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| anyhow::anyhow!("cannot bind {addr}: {e}"))?;
    tracing::info!(event = "engine.listening", port = engine.cfg.server.port, chains = engine.chains.len(), "http api is up; waiting for the leader lease");
    let server_shutdown = shutdown.clone();
    let router = api::router(engine.clone());
    let server = tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, router).with_graceful_shutdown(async move { server_shutdown.cancelled().await }).await {
            tracing::error!(event = "engine.http_failed", error = %e, "http server stopped unexpectedly");
        }
    });

    // Webhooks are delivered by whichever instances are running: row leases make that safe.
    tokio::spawn(leader::supervise(engine.clone(), "webhook-dispatcher".into(), webhook::run));
    tokio::spawn(wait_for_signal(shutdown.clone()));

    let lease = Lease::acquire(&engine.cfg.database.url, &shutdown).await?;
    if let Some(lease) = &lease {
        // KMS: one GetPublicKey per key, in parallel. Done only by the leader — a standby needs no keys.
        let (signers, factory) = SignerPool::load(&engine.cfg.signers).await?;
        tracing::info!(event = "signers.loaded", count = signers.len(), mode = ?engine.cfg.signers.mode, "signer pool ready");
        leader::start(engine.clone(), lease.epoch, Arc::new(signers), Arc::new(factory)).await?;

        tokio::select! {
            _ = shutdown.cancelled() => {}
            _ = lease.lost.cancelled() => {
                // Without the lease nothing may be sent. Stop first, then leave; the next boot recovers.
                engine.stop_sending.cancel();
                shutdown.cancel();
            }
        }
    }

    drain(&engine).await;
    if let Some(lease) = lease {
        lease.release().await;
    }
    let _ = tokio::time::timeout(Duration::from_secs(5), server).await;
    engine.store.close().await;
    tracing::info!(event = "engine.stopped", "shutdown complete");
    Ok(())
}

/// Lets in-flight work finish, bounded by `server.drain_timeout_ms` (keep Railway's `drainingSeconds`
/// above it). Workers stopped taking jobs when `shutdown` fired; what is still busy is mid-send.
async fn drain(engine: &Arc<Engine>) {
    let deadline = std::time::Instant::now() + Duration::from_millis(engine.cfg.server.drain_timeout_ms);
    loop {
        let busy = engine.pairs.read().values().filter(|p| p.view().current.is_some()).count();
        if busy == 0 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            tracing::warn!(event = "engine.drain_timeout", busy, "in-flight transactions did not settle before the drain deadline; the next leader recovers them");
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    engine.stop_sending.cancel();
    pipeline::settle::flush_all(engine, Duration::from_secs(5)).await;
}

async fn wait_for_signal(shutdown: CancellationToken) {
    use tokio::signal::unix::{signal, SignalKind};
    let (mut term, mut int) = match (signal(SignalKind::terminate()), signal(SignalKind::interrupt())) {
        (Ok(t), Ok(i)) => (t, i),
        _ => {
            tracing::error!(event = "engine.signal_setup_failed", "cannot install signal handlers; graceful shutdown is unavailable");
            return;
        }
    };
    tokio::select! {
        _ = term.recv() => tracing::info!(event = "engine.sigterm", "SIGTERM received; draining"),
        _ = int.recv() => tracing::info!(event = "engine.sigint", "SIGINT received; draining"),
    }
    shutdown.cancel();
}
