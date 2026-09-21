//! Chain health monitor: detects outages, pauses the chain's pairs while one lasts, and hands them to
//! recovery when it ends.
//!
//! Observation is passive first: every receipt and every block anyone fetches is a sighting of the head,
//! so a busy chain costs the monitor nothing. Only after `idle_probe_interval_ms` of silence does it spend
//! one call (`eth_getBlockByNumber(latest)`, which also refreshes the fee oracle), and it probes faster
//! only while something looks wrong.
//!
//! Two liveness modes (a per-chain tunable):
//! - `head_advance`: the chain always makes blocks, so a head that stops moving is an outage;
//! - `rpc_responsive`: blocks appear only when there are transactions (or the chain is a local dev node),
//!   so the only meaningful signal is whether the RPC answers.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use super::{adapter::LivenessMode, ChainCtx, ChainStatus};
use crate::{
    domain::{PauseReason, RecoveryStep},
    engine::Engine,
    telemetry,
};

pub async fn run(engine: Arc<Engine>, chain: Arc<ChainCtx>) {
    let t = chain.tunables.clone();
    let idle = Duration::from_millis(t.idle_probe_interval_ms.max(1_000));
    let fast = Duration::from_millis(t.degraded_probe_interval_ms.max(500));
    let warn_after = Duration::from_millis(t.stall_warn_ms);
    let outage_after = Duration::from_millis(t.stall_outage_ms);
    // When a probe first failed / first saw an unmoving head; cleared by any good news.
    let mut trouble_since: Option<Instant> = None;

    loop {
        let status = chain.status();
        let wait = if status == ChainStatus::Healthy && trouble_since.is_none() {
            // Sleep only as long as the chain stays silent: recent passive observations push the probe out.
            let since_observed = chain.health().last_observed.map(|o| o.elapsed()).unwrap_or(idle);
            idle.saturating_sub(since_observed).max(Duration::from_millis(250))
        } else {
            fast
        };
        tokio::select! {
            _ = tokio::time::sleep(wait) => {}
            _ = chain.probe_now.notified() => {}
            _ = engine.shutdown.cancelled() => return,
        }

        let before = chain.health();
        let quiet_for = before.last_observed.map(|o| o.elapsed()).unwrap_or(Duration::MAX);
        let breaker_open = chain.rpc.is_circuit_open();
        if status == ChainStatus::Healthy && trouble_since.is_none() && !breaker_open && quiet_for < idle {
            continue; // traffic is telling us everything we need
        }

        let probe = chain.rpc.get_block("latest", chain.rpc.probe_opts()).await;
        let good = match probe {
            Ok(Some(block)) => {
                let advanced = block.number_u64() > before.head_number || before.last_advance.is_none();
                chain.observe_block(&block);
                match t.liveness {
                    LivenessMode::RpcResponsive => true,
                    LivenessMode::HeadAdvance => {
                        // An unchanged head is only suspicious once it has been unchanged for long enough.
                        advanced || before.last_advance.map(|a| a.elapsed()).unwrap_or_default() < warn_after
                    }
                }
            }
            Ok(None) => false,
            Err(_) => false,
        };

        if good {
            trouble_since = None;
            if status != ChainStatus::Healthy {
                on_recovered(&engine, &chain, status);
            }
            continue;
        }

        let since = *trouble_since.get_or_insert_with(Instant::now);
        // For a stalled head, the clock started when the head last moved, not when we noticed.
        let stalled_for = match t.liveness {
            LivenessMode::HeadAdvance => chain.health().last_advance.map(|a| a.elapsed()).unwrap_or_else(|| since.elapsed()).max(since.elapsed()),
            LivenessMode::RpcResponsive => since.elapsed(),
        };
        if stalled_for >= outage_after && status != ChainStatus::Down {
            on_outage(&engine, &chain, stalled_for);
        } else if stalled_for >= warn_after && status == ChainStatus::Healthy {
            chain.set_status(ChainStatus::Degraded);
            tracing::warn!(
                event = "chain.degraded",
                chain = chain.chain_id,
                head = chain.head(),
                stalled_ms = stalled_for.as_millis() as u64,
                rpc_circuit_open = breaker_open,
                "chain looks unhealthy; probing more often"
            );
        }
    }
}

fn on_outage(engine: &Arc<Engine>, chain: &Arc<ChainCtx>, stalled_for: Duration) {
    chain.set_status(ChainStatus::Down);
    telemetry::alert(
        &format!("chain.down:{}", chain.chain_id),
        "chain.down",
        "chain outage detected; signers on this chain are paused until it recovers",
        Some(chain.chain_id),
        None,
        serde_json::json!({"head": chain.head(), "stalled_ms": stalled_for.as_millis() as u64, "rpc_circuit_open": chain.rpc.is_circuit_open()}),
    );
    metrics::counter!("gum_chain_outages_total", "chain" => chain.name.clone()).increment(1);
    for pair in engine.pairs_on(chain.chain_id) {
        // Never mask a pause that needs an operator or funds; those outlive the outage.
        if !pair.is_paused() {
            pair.pause(&engine.store, PauseReason::ChainOutage, RecoveryStep::AwaitingChainHead, "chain outage: waiting for the chain to produce blocks / the RPC to answer");
        }
    }
}

fn on_recovered(engine: &Arc<Engine>, chain: &Arc<ChainCtx>, previous: ChainStatus) {
    chain.set_status(ChainStatus::Healthy);
    tracing::info!(event = "chain.recovered", chain = chain.chain_id, head = chain.head(), was = ?previous, "chain is healthy again");
    if previous == ChainStatus::Down {
        // After an outage the local picture may be stale (dropped transactions, a rewound sequencer):
        // every pair reconciles before it takes new work.
        for pair in engine.pairs_on(chain.chain_id) {
            pair.request_recovery();
        }
    }
}
