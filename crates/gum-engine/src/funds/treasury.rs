//! The per-chain treasury: a signer whose only job is to keep the other signers funded.
//!
//! It is a pair like any other (own nonce, own ledger, attempts persisted before broadcast, recovered at
//! boot), driven by top-up requests instead of the job queue. Requests are processed one at a time, which
//! at chain speeds funds a whole pool in seconds and keeps the treasury's nonce handling trivial.

use std::{sync::Arc, time::Duration};

use alloy::primitives::{Address, Bytes, U256};
use tokio::sync::{mpsc, oneshot};

use crate::{
    chain::adapter::GasPlan,
    domain::{addr_hex, AttemptPurpose, Outcome as TxOutcome, PairKey, PauseReason, SlotOwner},
    engine::{Engine, Pair},
    pipeline::{
        recovery,
        tx::{NotBound, Outcome, TxDriver, TxIntent},
    },
    telemetry,
};

pub struct TopupRequest {
    pub signer: Address,
    /// Spendable balance the signer must end up with, at least.
    pub needed: U256,
    /// Native value the signer's pending transaction transfers (zero for a plain call). Some chains
    /// only let value come out of part of a balance, which changes how much counts as spendable.
    pub value: U256,
    pub reply: oneshot::Sender<Result<(), String>>,
}

/// What `pair` can put towards a transaction transferring `value`, per its ledger and the chain's rules.
pub fn spendable_for(pair: &Pair, value: U256) -> U256 {
    pair.ledger.mature();
    let view = pair.ledger.view();
    pair.chain.adapter.spendable(view.confirmed, value).saturating_sub(view.reserved)
}

/// Headline availability of a pair for the work it typically does: a treasury only ever transfers
/// value, a signer is judged by what it can spend on gas.
pub fn spendable(pair: &Pair) -> U256 {
    let typical_value = if pair.role == crate::domain::PairRole::Treasury { U256::from(1) } else { U256::ZERO };
    spendable_for(pair, typical_value)
}

/// Asks the chain's treasury to fund `pair` so it can spend `needed` and still sit above its floor.
/// Returns once the funds are spendable, or with the reason they are not coming.
pub async fn request_topup(engine: &Arc<Engine>, pair: &Arc<Pair>, needed: U256, value: U256) -> Result<(), String> {
    let chain = &pair.chain;
    let treasury = engine.treasury(chain.chain_id).ok_or_else(|| "no treasury is running for this chain".to_string())?;
    let (reply, done) = oneshot::channel();
    let target = needed.saturating_add(chain.cfg.signer_min_balance);
    treasury.requests.send(TopupRequest { signer: pair.key.signer, needed: target, value, reply }).await.map_err(|_| "treasury worker has stopped".to_string())?;

    let wait = Duration::from_millis(chain.tunables.topup_wait_timeout_ms);
    match tokio::time::timeout(wait, done).await {
        Ok(Ok(result)) => result?,
        Ok(Err(_)) => return Err("treasury dropped the request".into()),
        Err(_) => return Err(format!("treasury did not deliver within {} ms", wait.as_millis())),
    }

    // On chains with delayed execution the credit is on-chain but not yet spendable.
    let maturity = Duration::from_millis(chain.adapter.credit_maturity_blocks() * chain.tunables.block_time_ms);
    if !maturity.is_zero() {
        tokio::time::sleep(maturity + Duration::from_millis(50)).await;
    }
    if spendable_for(pair, value) >= needed {
        Ok(())
    } else {
        Err("top-up arrived but the signer still cannot cover the transaction".into())
    }
}

pub fn spawn(engine: Arc<Engine>, pair: Arc<Pair>) -> mpsc::Sender<TopupRequest> {
    let (tx, rx) = mpsc::channel(1_024);
    tokio::spawn(run(engine, pair, rx));
    tx
}

async fn run(engine: Arc<Engine>, treasury: Arc<Pair>, mut rx: mpsc::Receiver<TopupRequest>) {
    loop {
        if treasury.take_recovery_request() {
            recovery::recover_pair(&engine, &treasury).await;
            continue;
        }
        let request = tokio::select! {
            r = rx.recv() => match r { Some(r) => r, None => return },
            _ = treasury.wake.notified() => continue,
            _ = engine.shutdown.cancelled() => return,
        };
        let result = fund(&engine, &treasury, &request).await;
        if let Err(reason) = &result {
            if let Some(suppressed) = telemetry::throttled(&format!("topup.refused:{}", treasury.key), Duration::from_secs(30)) {
                tracing::warn!(event = "topup.refused", chain = treasury.key.chain_id, signer = %addr_hex(&request.signer), reason = %reason, suppressed, "top-up not sent");
            }
        }
        let _ = request.reply.send(result);
        check_treasury_level(&treasury);
    }
}

async fn fund(engine: &Arc<Engine>, treasury: &Arc<Pair>, request: &TopupRequest) -> Result<(), String> {
    let chain = &treasury.chain;
    if treasury.is_paused() {
        return Err(format!("treasury is paused ({})", treasury.pause_reason().map(|r| r.as_str()).unwrap_or("unknown")));
    }
    let target = engine.pair(&PairKey { chain_id: chain.chain_id, signer: request.signer }).ok_or_else(|| "unknown signer".to_string())?;

    // Requests queue up while one is being served; an earlier one may already have covered this signer.
    let have = spendable_for(&target, request.value).saturating_add(target.ledger.maturing());
    if have >= request.needed {
        return Ok(());
    }
    // A signer's first funding on a chain may be larger than the routine top-up that follows.
    let first_funding = !engine.store.ever_topped_up(chain.chain_id, &request.signer).await.map_err(|e| format!("store: {e}"))?;
    let base = if first_funding { chain.cfg.initial_topup_amount() } else { chain.cfg.topup_amount };
    let amount = base.max(request.needed.saturating_sub(have));

    let sent_recently = engine.store.recent_topups(chain.chain_id, &request.signer).await.map_err(|e| format!("store: {e}"))?;
    if sent_recently >= chain.cfg.max_topups_per_signer_per_hour {
        telemetry::alert(
            &format!("topup.rate_cap:{}:{}", chain.chain_id, addr_hex(&request.signer)),
            "topup.rate_capped",
            "a signer hit its hourly top-up cap; something is draining it",
            Some(chain.chain_id),
            Some(&addr_hex(&request.signer)),
            serde_json::json!({"topups_last_hour": sent_recently}),
        );
        return Err(format!("signer already received {sent_recently} top-ups in the last hour"));
    }

    let me = treasury.key.signer;
    let fees = chain.fee_quote(false).await.map_err(|e| format!("fee quote: {e}"))?;
    let gas_limit = match chain.adapter.transfer_gas() {
        GasPlan::Fixed(g) => g,
        GasPlan::Estimate => {
            let g = chain.rpc.estimate_gas(me, request.signer, &Bytes::new(), amount, chain.rpc.read_opts()).await.map_err(|e| format!("gas estimate: {e}"))?;
            g.saturating_mul(chain.tunables.estimate_multiplier_bps as u64) / 10_000
        }
    };

    let driver = TxDriver::new(engine, treasury);
    let topup_id = engine.store.create_topup(chain.chain_id, &me, &request.signer, amount).await.map_err(|e| format!("store: {e}"))?;
    let intent = TxIntent { owner: SlotOwner::Topup(topup_id), purpose: AttemptPurpose::Topup, to: request.signer, data: Bytes::new(), value: amount, gas_limit };
    let cost = driver.cost_of(&intent, &fees);
    if spendable(treasury) < cost {
        // Before refusing, ask the chain: an operator may have refilled the treasury since the last sweep.
        let ticket = treasury.ledger.begin_reconcile();
        if let Ok(balance) = chain.rpc.get_balance(me, chain.rpc.read_opts()).await {
            treasury.ledger.reconcile(ticket, balance, chain.head());
        }
    }
    if spendable(treasury) < cost {
        let _ = engine.store.fail_topup(topup_id, "treasury balance too low").await;
        telemetry::alert(
            &format!("treasury.insufficient:{}", chain.chain_id),
            "treasury.insufficient",
            "treasury cannot cover a signer top-up; signers on this chain will pause until it is refilled",
            Some(chain.chain_id),
            Some(&addr_hex(&me)),
            serde_json::json!({"needed": cost.to_string(), "spendable": spendable(treasury).to_string(), "for_signer": addr_hex(&request.signer)}),
        );
        return Err("treasury balance is too low to fund this signer".into());
    }

    treasury.set_current(Some(topup_id));
    let outcome = driver.execute(intent, fees).await;
    treasury.set_current(None);
    match outcome {
        Outcome::Mined { inclusion, .. } if inclusion.outcome == TxOutcome::Success => {
            let maturity = Duration::from_millis(chain.adapter.credit_maturity_blocks() * chain.tunables.block_time_ms);
            target.ledger.credit(amount, inclusion.block_number, maturity);
            tracing::info!(event = "topup.sent", chain = chain.chain_id, signer = %addr_hex(&request.signer), amount = %amount, tx_hash = %crate::domain::hash_hex(&inclusion.tx_hash), "signer topped up from treasury");
            metrics::counter!("gum_topups_total", "chain" => chain.name.clone()).increment(1);
            Ok(())
        }
        Outcome::Mined { .. } => Err("top-up transaction reverted".into()),
        Outcome::NotBound(reason) => {
            let _ = engine.store.fail_topup(topup_id, "could not be sent").await;
            Err(match reason {
                NotBound::Signer(e) => format!("treasury signer: {e}"),
                NotBound::Store(e) => format!("store: {e}"),
                NotBound::Rpc(e) => format!("rpc: {e}"),
                NotBound::OwnerGone => "top-up was already handled".into(),
            })
        }
        Outcome::Voided { reason, .. } => Err(format!("top-up rejected: {reason}")),
        Outcome::NeedsRecovery => {
            treasury.request_recovery();
            Err("treasury needs nonce recovery".into())
        }
        Outcome::Unresolved => Err("treasury transaction is stuck; operator attention needed".into()),
        Outcome::Aborted => Err("engine is shutting down".into()),
    }
}

/// Alerts while the treasury sits below its configured floor.
pub fn check_treasury_level(treasury: &Pair) {
    let chain = &treasury.chain;
    let view = treasury.ledger.view();
    if !view.initialised {
        return;
    }
    let available = spendable(treasury);
    metrics::gauge!("gum_treasury_balance_wei", "chain" => chain.name.clone()).set(u256_to_f64(available));
    if available < chain.cfg.treasury_min_balance {
        telemetry::alert(
            &format!("treasury.low:{}", chain.chain_id),
            "treasury.low",
            "treasury balance is below its configured floor; refill it",
            Some(chain.chain_id),
            Some(&addr_hex(&treasury.key.signer)),
            serde_json::json!({"available": available.to_string(), "treasury_min_balance": chain.cfg.treasury_min_balance.to_string()}),
        );
    } else if treasury.pause_reason() == Some(PauseReason::InsufficientFunds) {
        // Refilled by an operator: nothing else holds the treasury back.
        // (resume happens in the balance monitor, which owns the store handle)
    }
}

pub fn u256_to_f64(v: U256) -> f64 {
    v.to_string().parse::<f64>().unwrap_or(f64::MAX)
}
