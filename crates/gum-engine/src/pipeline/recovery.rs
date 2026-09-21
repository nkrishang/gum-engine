//! Pair recovery: bring one (signer, chain) pair back in line with the chain.
//!
//! Runs at boot for every pair and whenever something suggests the local picture is wrong: a chain
//! outage ended, a confirmation check failed (re-org), the node reported a nonce problem, or an operator
//! asked for it. It always runs on the pair's own worker task, so it can never race a send.
//!
//! The routine is total and ordered. It walks every attempt the store still considers live, lowest nonce
//! first, and does not move to the next nonce until the current one is mined — a later nonce can never be
//! mined before an earlier one, so any other order just produces "nonce too high". A null receipt is
//! never taken as a verdict on its own: the stored bytes are rebroadcast and the node's answer decides.

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use crate::{
    chain::{adapter::ConfirmCheck, confirm::PendingConfirmation},
    domain::{addr_hex, Attempt, AttemptStatus, PauseReason, RecoveryStep},
    engine::{Engine, Pair},
    funds::treasury,
    pipeline::{
        accounting,
        settle::SettleOp,
        tx::{inclusion_from, Outcome, TxDriver},
    },
    telemetry,
};

pub async fn recover_pair(engine: &Arc<Engine>, pair: &Arc<Pair>) {
    let chain = pair.chain.clone();
    let signer = addr_hex(&pair.key.signer);
    let started = std::time::Instant::now();

    // A pair that was running gets an explicit pause so observers can see why it stopped taking jobs.
    if !pair.is_paused() {
        pair.pause(&engine.store, PauseReason::NonceDrift, RecoveryStep::ReconcilingNonce, "reconciling nonce and in-flight transactions with the chain");
    }

    // Inclusions are written behind; make sure the store reflects everything this process already knows.
    crate::pipeline::settle::flush_chain(engine, chain.chain_id).await;

    let live = loop {
        match engine.store.load_live_attempts_for(chain.chain_id, &pair.key.signer).await {
            Ok(live) => break live,
            Err(e) => {
                if let Some(suppressed) = telemetry::throttled(&format!("recovery.store:{}", pair.key), Duration::from_secs(15)) {
                    tracing::warn!(event = "recovery.store_unavailable", chain = chain.chain_id, signer = %signer, code = e.code(), error = %e, suppressed, "cannot load live attempts; retrying");
                }
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {}
                    _ = engine.shutdown.cancelled() => return,
                }
            }
        }
    };

    let mut by_nonce: BTreeMap<u64, Vec<Attempt>> = BTreeMap::new();
    for attempt in live {
        by_nonce.entry(attempt.nonce).or_default().push(attempt);
    }
    let had_live = !by_nonce.is_empty();
    let driver = TxDriver::new(engine, pair);

    for (nonce, attempts) in by_nonce {
        if engine.stop_sending.is_cancelled() {
            return;
        }
        // An attempt recorded as included: is it still on the canonical chain?
        if let Some(included) = attempts.iter().find(|a| a.status == AttemptStatus::Included).cloned() {
            match chain.rpc.get_receipt(included.tx_hash, chain.rpc.read_opts()).await {
                Ok(Some(receipt)) => {
                    if let Some(inclusion) = inclusion_from(pair, &included, &receipt) {
                        chain.observe_head(inclusion.block_number, Some(inclusion.block_hash));
                        if chain.confirm_check() == ConfirmCheck::Immediate {
                            if let Some(settler) = engine.settler(chain.chain_id) {
                                let _ = settler.send(SettleOp::Confirmation { attempt: included.clone() }).await;
                            }
                            accounting::confirmed(engine, pair, &included, &inclusion);
                        } else if let Some(confirmer) = engine.confirmers.read().get(&chain.chain_id) {
                            let _ = confirmer.send(PendingConfirmation {
                                attempt: included.clone(),
                                inclusion,
                                worst_case: Default::default(),
                                due: std::time::Instant::now(),
                                misses: 0,
                            });
                        }
                        continue;
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::warn!(event = "recovery.rpc_failed", chain = chain.chain_id, signer = %signer, nonce, error = %e, "cannot verify an included transaction; recovery will be retried");
                    pair.request_recovery();
                    tokio::time::sleep(Duration::from_secs(3)).await;
                    return;
                }
            }
            // Gone from the chain: undo the inclusion durably before anything else touches this nonce.
            tracing::warn!(event = "recovery.inclusion_vanished", chain = chain.chain_id, signer = %signer, nonce, tx_hash = %crate::domain::hash_hex(&included.tx_hash), "a recorded inclusion is no longer on the chain; rebroadcasting");
            if let Err(e) = engine.store.demote_inclusion(&included).await {
                tracing::error!(event = "recovery.demote_failed", chain = chain.chain_id, signer = %signer, nonce, code = e.code(), error = %e, "cannot demote vanished inclusion; recovery will be retried");
                pair.request_recovery();
                tokio::time::sleep(Duration::from_secs(3)).await;
                return;
            }
            accounting::demoted(engine, pair, &included);
        }

        pair.pause(&engine.store, pair.pause_reason().unwrap_or(PauseReason::NonceDrift), RecoveryStep::Rebroadcasting, format!("resolving nonce {nonce}"));
        let mut attempts: Vec<Attempt> = attempts.into_iter().filter(|a| !a.raw_tx.is_empty()).collect();
        for a in &mut attempts {
            a.status = AttemptStatus::Broadcast;
        }
        if attempts.is_empty() {
            continue;
        }
        let owner = attempts[0].owner;
        pair.set_current(Some(owner.id()));
        let outcome = driver.resume(attempts).await;
        pair.set_current(None);
        match outcome {
            Outcome::Mined { attempt, inclusion } => accounting::mined(engine, pair, &attempt, &inclusion, None),
            Outcome::Voided { .. } | Outcome::NotBound(_) => {}
            Outcome::Aborted => return,
            Outcome::Unresolved => return, // paused for an operator by the driver
            Outcome::NeedsRecovery => {
                // Lower nonces were just resolved in order, so a gap below this one means a transaction
                // the store considers final has disappeared: deeper than anything we can repair alone.
                pair.pause(&engine.store, PauseReason::Reorg, RecoveryStep::NeedsOperator, format!("chain nonce is below {nonce} although every earlier transaction was settled"));
                telemetry::alert(
                    &format!("recovery.deep_reorg:{}", pair.key),
                    "recovery.deep_reorg",
                    "a settled transaction vanished from the chain; operator attention needed",
                    Some(chain.chain_id),
                    Some(&signer),
                    serde_json::json!({"nonce": nonce}),
                );
                return;
            }
        }
    }

    // Nonce: the chain is a floor, the highest nonce we ever bound is the other. A fresh signer (nothing
    // ever bound) must ask the chain; a known one with nothing in flight needs no call at boot.
    let hwm = match engine.store.ensure_pair(chain.chain_id, &pair.key.signer, pair.role).await {
        Ok(record) => record.nonce_hwm,
        Err(e) => {
            tracing::warn!(event = "recovery.store_unavailable", chain = chain.chain_id, signer = %signer, code = e.code(), error = %e, "cannot read nonce high-water mark; recovery will be retried");
            pair.request_recovery();
            tokio::time::sleep(Duration::from_secs(2)).await;
            return;
        }
    };
    let booting = pair.pause_reason() == Some(PauseReason::Booting);
    let need_chain_nonce = hwm.is_none() || had_live || !booting;
    let chain_nonce = if need_chain_nonce {
        match chain.rpc.get_transaction_count(pair.key.signer, chain.rpc.read_opts()).await {
            Ok(n) => n,
            Err(e) => {
                tracing::warn!(event = "recovery.rpc_failed", chain = chain.chain_id, signer = %signer, error = %e, "cannot read chain nonce; recovery will be retried");
                pair.request_recovery();
                tokio::time::sleep(Duration::from_secs(3)).await;
                return;
            }
        }
    } else {
        0
    };
    let next = chain_nonce.max(hwm.map(|h| h + 1).unwrap_or(0));
    pair.set_next_nonce(next);

    // Nothing of ours is in flight now, so the chain balance is the whole truth.
    match chain.rpc.get_balance(pair.key.signer, chain.rpc.read_opts()).await {
        Ok(balance) => pair.ledger.initialise(balance, chain.head()),
        Err(e) => {
            tracing::warn!(event = "recovery.rpc_failed", chain = chain.chain_id, signer = %signer, error = %e, "cannot read balance; recovery will be retried");
            pair.request_recovery();
            tokio::time::sleep(Duration::from_secs(3)).await;
            return;
        }
    }
    if pair.role == crate::domain::PairRole::Treasury {
        treasury::check_treasury_level(pair);
    }

    tracing::info!(event = "pair.recovered", chain = chain.chain_id, signer = %signer, role = pair.role.as_str(), next_nonce = next, had_live, elapsed_ms = started.elapsed().as_millis() as u64, "pair reconciled with the chain");
    for reason in [PauseReason::Booting, PauseReason::NonceDrift, PauseReason::Reorg] {
        pair.resume_if(&engine.store, reason);
    }
    // An outage pause is only lifted once the chain monitor agrees the chain is back.
    if chain.status() != crate::chain::ChainStatus::Down {
        pair.resume_if(&engine.store, PauseReason::ChainOutage);
    }
}
