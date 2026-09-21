//! What happens when this instance wins the lease: build the pairs, rebuild in-memory state from
//! Postgres, and start every task that may touch a signer.
//!
//! Each pair comes up on its own: it starts paused (`booting`), its worker runs recovery against the
//! chain, and it resumes as soon as that succeeds. A chain that is down at boot therefore delays only its
//! own pairs.

use std::{sync::atomic::Ordering, sync::Arc, time::Duration};

use crate::{
    chain::{confirm, monitor, ChainCtx},
    domain::{addr_hex, PairKey, PairRole},
    engine::{Engine, Pair, TreasuryHandle},
    funds::{self, treasury},
    pipeline::{settle, worker},
    queue::Admit,
    signer::{SignerFactory, SignerPool},
    stats::Bucket,
    store::parse_addr,
    telemetry,
};

pub async fn start(engine: Arc<Engine>, epoch: i64, signers: Arc<SignerPool>, factory: Arc<SignerFactory>) -> anyhow::Result<()> {
    engine.epoch.store(epoch, Ordering::SeqCst);

    // Hold the ingest gate while counters are rebuilt, so a request accepted in that instant is counted
    // exactly once.
    let gate = engine.ingest_gate.write().await;
    engine.stats.reset();

    for rollup in engine.store.rollups().await? {
        let signer = if rollup.signer.is_empty() { None } else { Some(parse_addr(&rollup.signer)?) };
        let bucket = match rollup.bucket.as_str() {
            "succeeded" => Bucket::Succeeded,
            "reverted" => Bucket::Reverted,
            "failed" => Bucket::Failed,
            other => {
                tracing::warn!(event = "unexpected.rollup_bucket", bucket = other, "ignoring unknown stats bucket");
                continue;
            }
        };
        engine.stats.add(rollup.chain_id, signer, bucket, rollup.count.max(0) as u64);
    }

    for chain in engine.chains.values() {
        engine.settlers.write().insert(chain.chain_id, settle::spawn(engine.clone(), chain.chain_id));
        engine.confirmers.write().insert(chain.chain_id, confirm::spawn(engine.clone(), chain.clone()));

        for job in engine.store.load_live_jobs(chain.chain_id).await? {
            let signer = job.signer.as_deref().map(parse_addr).transpose()?;
            let bucket = if job.status == "included" { Bucket::Included } else { Bucket::InFlight };
            engine.stats.add(chain.chain_id, signer, bucket, 1);
        }
        let unbound = engine.store.fail_unbound_topups(chain.chain_id).await?;
        if unbound > 0 {
            tracing::info!(event = "boot.topups_reset", chain = chain.chain_id, count = unbound, "top-ups that were never sent are re-evaluated from scratch");
        }
        let loaded = admit_from_store(&engine, chain).await?;
        tracing::info!(event = "boot.queue_loaded", chain = chain.chain_id, jobs = loaded, "queued jobs loaded from the store");
    }
    engine.leader.store(true, Ordering::SeqCst);
    drop(gate);

    for chain in engine.chains.values().cloned() {
        let (engine, signers, factory) = (engine.clone(), signers.clone(), factory.clone());
        // Chains come up independently; one unreachable RPC must not hold the others back.
        tokio::spawn(async move {
            if let Err(e) = start_chain(engine.clone(), chain.clone(), signers, factory).await {
                telemetry::alert(
                    &format!("boot.chain_failed:{}", chain.chain_id),
                    "boot.chain_failed",
                    "chain could not be started; its jobs stay queued",
                    Some(chain.chain_id),
                    None,
                    serde_json::json!({"error": e.to_string()}),
                );
                chain.operator_paused.store(true, Ordering::Relaxed);
            }
        });
    }
    Ok(())
}

async fn start_chain(engine: Arc<Engine>, chain: Arc<ChainCtx>, signers: Arc<SignerPool>, factory: Arc<SignerFactory>) -> anyhow::Result<()> {
    // Identity and capabilities. Retried: at boot the RPC (or the private network) may not be up yet.
    let mut delay = Duration::from_millis(500);
    loop {
        match chain.rpc.eth_chain_id().await {
            Ok(id) if id == chain.chain_id => break,
            Ok(id) => anyhow::bail!("rpc endpoint reports chain id {id}, but `{}` is configured as {}", chain.name, chain.chain_id),
            Err(e) => {
                if let Some(suppressed) = telemetry::throttled(&format!("boot.rpc:{}", chain.chain_id), Duration::from_secs(30)) {
                    tracing::warn!(event = "boot.rpc_unreachable", chain = chain.chain_id, error = %e, suppressed, "rpc endpoint not reachable yet; retrying");
                }
                tokio::select! {
                    _ = tokio::time::sleep(delay) => {}
                    _ = engine.shutdown.cancelled() => return Ok(()),
                }
                delay = (delay * 2).min(Duration::from_secs(10));
            }
        }
    }
    match chain.rpc.supports_sync_send().await {
        Ok(true) => chain.sync_send.store(true, Ordering::Relaxed),
        Ok(false) => {
            // Never silent: the async path costs roughly three times the RPC calls per transaction.
            telemetry::alert(
                &format!("boot.no_sync_send:{}", chain.chain_id),
                "rpc.sync_send_unavailable",
                "eth_sendRawTransactionSync is not available on this endpoint; falling back to send + receipt polling (higher RPC cost)",
                Some(chain.chain_id),
                None,
                serde_json::json!({}),
            );
        }
        Err(e) => tracing::warn!(event = "boot.sync_probe_failed", chain = chain.chain_id, error = %e, "could not probe sync-send support; using send + receipt polling"),
    }

    // Treasury first: the signers' first act may be asking it for funds.
    let treasury_key = chain.cfg.treasury_private_key.clone().or(chain.cfg.treasury_key_id.clone()).ok_or_else(|| anyhow::anyhow!("no treasury key configured"))?;
    let treasury_signer = Arc::new(factory.build(&treasury_key).await?);
    if signers.get(&treasury_signer.address).is_some() {
        anyhow::bail!("treasury {} is also configured as a job signer; they must be distinct keys", addr_hex(&treasury_signer.address));
    }
    engine.store.register_signer(&treasury_signer.address, &treasury_signer.key_ref, PairRole::Treasury).await?;
    let key = PairKey { chain_id: chain.chain_id, signer: treasury_signer.address };
    let record = engine.store.ensure_pair(chain.chain_id, &treasury_signer.address, PairRole::Treasury).await?;
    let treasury_pair = Pair::new(key, PairRole::Treasury, treasury_signer, chain.clone(), record.manual_pause);
    engine.pairs.write().insert(key, treasury_pair.clone());
    let requests = treasury::spawn(engine.clone(), treasury_pair.clone());
    engine.treasuries.write().insert(chain.chain_id, TreasuryHandle { pair: treasury_pair, requests });

    let mut configured = std::collections::BTreeSet::new();
    for signer in signers.all() {
        configured.insert(signer.address);
        engine.store.register_signer(&signer.address, &signer.key_ref, PairRole::Signer).await?;
        let key = PairKey { chain_id: chain.chain_id, signer: signer.address };
        let record = engine.store.ensure_pair(chain.chain_id, &signer.address, PairRole::Signer).await?;
        let pair = Pair::new(key, PairRole::Signer, signer.clone(), chain.clone(), record.manual_pause);
        engine.pairs.write().insert(key, pair.clone());
        tokio::spawn(supervise(engine.clone(), format!("worker:{key}"), move |engine| {
            // Every (re)start begins with recovery: a worker that died mid-send left a live attempt behind.
            pair.request_recovery();
            worker::run(engine, pair.clone())
        }));
    }

    // A signer dropped from the config while its transactions are still in flight cannot be recovered
    // without its key. Say so loudly instead of abandoning them.
    for orphan in engine.store.signers_with_live_attempts(chain.chain_id).await? {
        if !configured.contains(&orphan) && orphan != key.signer {
            telemetry::alert(
                &format!("boot.signer_missing:{}:{}", chain.chain_id, addr_hex(&orphan)),
                "signer.missing_with_live_transactions",
                "a signer with in-flight transactions is no longer configured; re-add its key until they settle",
                Some(chain.chain_id),
                Some(&addr_hex(&orphan)),
                serde_json::json!({}),
            );
        }
    }

    let (e, c) = (engine.clone(), chain.clone());
    tokio::spawn(supervise(engine.clone(), format!("chain-monitor:{}", chain.chain_id), move |_| monitor::run(e.clone(), c.clone())));
    let (e, c) = (engine.clone(), chain.clone());
    tokio::spawn(supervise(engine.clone(), format!("balance-monitor:{}", chain.chain_id), move |_| funds::monitor::run(e.clone(), c.clone())));
    let (e, c) = (engine.clone(), chain.clone());
    tokio::spawn(supervise(engine.clone(), format!("queue-sweep:{}", chain.chain_id), move |_| sweep(e.clone(), c.clone())));

    chain.started.store(true, Ordering::SeqCst);
    tracing::info!(event = "chain.started", chain = chain.chain_id, name = %chain.name, kind = chain.adapter.kind(), signers = signers.len(), send_mode = chain.send_mode(), confirmation_delay_ms = chain.tunables.confirmation_delay_ms, "chain is live");
    Ok(())
}

/// Pages queued rows from Postgres into the in-memory window. `admit` de-duplicates, so jobs this process
/// already tracks are skipped; what remains are rows written by another instance (deploy overlap) or
/// deferred because the window was full.
async fn admit_from_store(engine: &Engine, chain: &ChainCtx) -> anyhow::Result<usize> {
    let jobs = engine.store.load_queued(chain.chain_id, chain.queue.window()).await?;
    let mut admitted = 0;
    for job in jobs {
        if chain.queue.admit(job) == Admit::Queued {
            engine.stats.add(chain.chain_id, None, Bucket::Queued, 1);
            admitted += 1;
        }
    }
    chain.queue.reset_overflow();
    Ok(admitted)
}

async fn sweep(engine: Arc<Engine>, chain: Arc<ChainCtx>) {
    let every = Duration::from_millis(engine.cfg.queue.sweep_interval_ms.max(500));
    loop {
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            _ = engine.shutdown.cancelled() => return,
        }
        if !chain.queue.has_room() {
            continue;
        }
        let gate = engine.ingest_gate.read().await;
        match admit_from_store(&engine, &chain).await {
            Ok(0) => {}
            Ok(n) => tracing::debug!(event = "queue.swept", chain = chain.chain_id, admitted = n, "paged queued jobs in from the store"),
            Err(e) => {
                if let Some(suppressed) = telemetry::throttled(&format!("queue.sweep:{}", chain.chain_id), Duration::from_secs(30)) {
                    tracing::warn!(event = "queue.sweep_failed", chain = chain.chain_id, error = %e, suppressed, "could not read the durable queue");
                }
            }
        }
        drop(gate);
    }
}

/// Restarts a long-lived task if it ever panics or returns, with backoff, and says so.
pub async fn supervise<F, Fut>(engine: Arc<Engine>, name: String, mut task: F)
where
    F: FnMut(Arc<Engine>) -> Fut + Send + 'static,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let mut delay = Duration::from_millis(500);
    loop {
        let started = std::time::Instant::now();
        let result = tokio::spawn(task(engine.clone())).await;
        if engine.shutdown.is_cancelled() {
            return;
        }
        let detail = match result {
            Ok(()) => "task returned unexpectedly".to_string(),
            Err(e) if e.is_panic() => format!("task panicked: {e}"),
            Err(e) => format!("task was cancelled: {e}"),
        };
        telemetry::alert(
            &format!("task.crashed:{name}"),
            "task.crashed",
            "a background task stopped and is being restarted",
            None,
            None,
            serde_json::json!({"task": name, "detail": detail}),
        );
        if started.elapsed() > Duration::from_secs(60) {
            delay = Duration::from_millis(500);
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}
