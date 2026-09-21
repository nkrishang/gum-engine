//! Confirmation verifier: re-checks each inclusion after the chain's `confirmation_delay_ms` and then
//! lets `transaction.confirmed` fire.
//!
//! This is a soft confirmation — "the transaction is still where we saw it, a sane while later" — not
//! L1 finality. It never runs for chains that finalize on inclusion (delay 0), and it never holds up a
//! signer: the signer was freed when the receipt arrived.
//!
//! Cost: one block fetch per distinct inclusion block (shared by every transaction in it), plus one
//! `finalized` fetch per tick on chains that confirm by tag. A null or mismatching answer is a retry
//! signal, not a verdict — load-balanced RPC backends lag each other — so a failure needs two misses
//! *and* a receipt lookup before the inclusion is declared gone.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
use tokio::sync::mpsc;

use super::{adapter::ConfirmCheck, ChainCtx, ChainStatus};
use crate::{
    domain::{addr_hex, hash_hex, Attempt, Inclusion, PairKey, PauseReason, RecoveryStep},
    engine::Engine,
    pipeline::{accounting, settle::SettleOp, tx::inclusion_from},
    rpc::types::Block,
    telemetry,
};

#[derive(Debug, Clone)]
pub struct PendingConfirmation {
    pub attempt: Attempt,
    pub inclusion: Inclusion,
    pub worst_case: U256,
    pub due: Instant,
    pub misses: u32,
}

enum Verdict {
    Confirmed,
    /// Not decidable yet (tag has not reached the block, node lagging): look again shortly.
    NotYet,
    Missing,
}

pub fn spawn(engine: Arc<Engine>, chain: Arc<ChainCtx>) -> mpsc::UnboundedSender<PendingConfirmation> {
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(run(engine, chain, rx));
    tx
}

async fn run(engine: Arc<Engine>, chain: Arc<ChainCtx>, mut rx: mpsc::UnboundedReceiver<PendingConfirmation>) {
    let mut pending: VecDeque<PendingConfirmation> = VecDeque::new();
    let tick = Duration::from_millis((chain.tunables.confirmation_delay_ms / 2).clamp(100, 1_000));
    loop {
        let next_due = pending.iter().map(|p| p.due).min();
        tokio::select! {
            item = rx.recv() => match item {
                Some(p) => { pending.push_back(p); continue; }
                None => return,
            },
            _ = async { match next_due { Some(due) => tokio::time::sleep_until(due.into()).await, None => std::future::pending().await } } => {}
            _ = engine.shutdown.cancelled() => return,
        }
        // While the chain is unhealthy nothing can be concluded; keep everything and look again later.
        if chain.status() == ChainStatus::Down {
            tokio::time::sleep(tick).await;
            continue;
        }
        let now = Instant::now();
        let (due, later): (Vec<_>, Vec<_>) = pending.drain(..).partition(|p| p.due <= now);
        pending.extend(later);
        if due.is_empty() {
            continue;
        }
        metrics::gauge!("gum_pending_confirmations", "chain" => chain.name.clone()).set((pending.len() + due.len()) as f64);

        let finalized = if chain.confirm_check() == ConfirmCheck::FinalizedTag {
            match chain.rpc.get_block("finalized", chain.rpc.read_opts()).await {
                Ok(Some(b)) => Some(b),
                _ => None,
            }
        } else {
            None
        };

        let mut blocks: BTreeMap<u64, Option<Block>> = BTreeMap::new();
        for mut item in due {
            let verdict = check(&chain, &item, finalized.as_ref(), &mut blocks).await;
            match verdict {
                Verdict::Confirmed => confirm(&engine, &chain, &item).await,
                Verdict::NotYet => {
                    item.due = Instant::now() + tick;
                    pending.push_back(item);
                }
                Verdict::Missing => {
                    item.misses += 1;
                    if item.misses < 2 {
                        item.due = Instant::now() + tick;
                        pending.push_back(item);
                    } else if let Some(requeue) = on_missing(&engine, &chain, item).await {
                        pending.push_back(requeue);
                    }
                }
            }
        }
    }
}

async fn block_at(chain: &ChainCtx, number: u64, cache: &mut BTreeMap<u64, Option<Block>>) -> Option<Block> {
    if let Some(cached) = cache.get(&number) {
        return cached.clone();
    }
    let block = chain.rpc.get_block_by_number(number, chain.rpc.read_opts()).await.ok().flatten();
    if let Some(b) = &block {
        chain.observe_block(b);
    }
    cache.insert(number, block.clone());
    block
}

async fn check(chain: &ChainCtx, item: &PendingConfirmation, finalized: Option<&Block>, cache: &mut BTreeMap<u64, Option<Block>>) -> Verdict {
    let n = item.inclusion.block_number;
    let hash = item.inclusion.tx_hash;
    match chain.confirm_check() {
        ConfirmCheck::Immediate => Verdict::Confirmed,
        ConfirmCheck::BlockHash => match block_at(chain, n, cache).await {
            Some(b) if b.hash == item.inclusion.block_hash && b.transactions.contains(&hash) => Verdict::Confirmed,
            Some(_) => Verdict::Missing,
            None => Verdict::NotYet,
        },
        ConfirmCheck::TxMembership { window } => {
            // A pre-confirmation receipt may carry a provisional block hash, so membership decides.
            for number in std::iter::once(n).chain((1..=window).flat_map(|d| [n.saturating_sub(d), n + d])) {
                if let Some(b) = block_at(chain, number, cache).await {
                    if b.transactions.contains(&hash) {
                        return Verdict::Confirmed;
                    }
                } else if number == n {
                    return Verdict::NotYet;
                }
            }
            Verdict::Missing
        }
        ConfirmCheck::FinalizedTag => {
            let Some(f) = finalized else { return Verdict::NotYet };
            if f.number_u64() < n {
                return Verdict::NotYet;
            }
            let block = if f.number_u64() == n { Some(f.clone()) } else { block_at(chain, n, cache).await };
            match block {
                Some(b) if b.hash == item.inclusion.block_hash && b.transactions.contains(&hash) => Verdict::Confirmed,
                Some(_) => Verdict::Missing,
                None => Verdict::NotYet,
            }
        }
    }
}

async fn confirm(engine: &Arc<Engine>, chain: &Arc<ChainCtx>, item: &PendingConfirmation) {
    if let Some(settler) = engine.settler(chain.chain_id) {
        let _ = settler.send(SettleOp::Confirmation { attempt: item.attempt.clone() }).await;
    }
    if let Some(pair) = engine.pair(&PairKey { chain_id: chain.chain_id, signer: item.attempt.signer }) {
        accounting::confirmed(engine, &pair, &item.attempt, &item.inclusion);
    }
}

/// Two checks failed. Ask for the receipt: the transaction either moved to another block (fine — follow
/// it) or is gone (re-org): then the inclusion is undone durably and the pair recovers, rebroadcasting
/// the stored bytes. Later nonces of the pair may already be live; recovery handles them in order.
async fn on_missing(engine: &Arc<Engine>, chain: &Arc<ChainCtx>, mut item: PendingConfirmation) -> Option<PendingConfirmation> {
    let key = PairKey { chain_id: chain.chain_id, signer: item.attempt.signer };
    let pair = engine.pair(&key)?;
    let tick = Duration::from_millis(chain.tunables.confirmation_delay_ms.max(200));

    match chain.rpc.get_receipt(item.attempt.tx_hash, chain.rpc.read_opts()).await {
        Ok(Some(receipt)) => {
            if let Some(moved) = inclusion_from(&pair, &item.attempt, &receipt) {
                if moved.block_hash != item.inclusion.block_hash || moved.block_number != item.inclusion.block_number {
                    tracing::warn!(event = "confirm.moved", chain = chain.chain_id, signer = %addr_hex(&key.signer), tx_hash = %hash_hex(&item.attempt.tx_hash), from_block = item.inclusion.block_number, to_block = moved.block_number, "transaction moved to a different block; re-checking there");
                    item.inclusion.block_number = moved.block_number;
                    item.inclusion.block_hash = moved.block_hash;
                    crate::pipeline::settle::flush_chain(engine, chain.chain_id).await;
                    if let Err(e) = engine.store.update_inclusion_block(&item.attempt, moved.block_number, &moved.block_hash).await {
                        tracing::warn!(event = "confirm.block_update_failed", chain = chain.chain_id, code = e.code(), error = %e, "could not record the transaction's new block; will retry with the next check");
                    }
                }
            }
            item.misses = 0;
            item.due = Instant::now() + tick;
            Some(item)
        }
        Ok(None) => {
            telemetry::alert(
                &format!("confirm.vanished:{key}"),
                "confirm.vanished",
                "an included transaction is no longer on the chain (re-org); recovering the signer",
                Some(chain.chain_id),
                Some(&addr_hex(&key.signer)),
                serde_json::json!({"tx_hash": hash_hex(&item.attempt.tx_hash), "nonce": item.attempt.nonce, "block": item.inclusion.block_number}),
            );
            crate::pipeline::settle::flush_chain(engine, chain.chain_id).await;
            match engine.store.demote_inclusion(&item.attempt).await {
                Ok(crate::store::attempts::Settled::Applied) => {
                    let transferred = if item.inclusion.outcome == crate::domain::Outcome::Success { item.attempt.value } else { U256::ZERO };
                    pair.ledger.unsettle(item.attempt.nonce, item.inclusion.fee_paid.saturating_add(transferred), item.worst_case);
                    accounting::demoted(engine, &pair, &item.attempt);
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::error!(event = "confirm.demote_failed", chain = chain.chain_id, code = e.code(), error = %e, "could not undo a vanished inclusion; will retry");
                    item.due = Instant::now() + tick;
                    return Some(item);
                }
            }
            pair.pause(
                &engine.store,
                PauseReason::Reorg,
                RecoveryStep::ReconcilingNonce,
                format!("transaction at nonce {} vanished from block {}", item.attempt.nonce, item.inclusion.block_number),
            );
            pair.request_recovery();
            None
        }
        Err(_) => {
            item.due = Instant::now() + tick;
            Some(item)
        }
    }
}
