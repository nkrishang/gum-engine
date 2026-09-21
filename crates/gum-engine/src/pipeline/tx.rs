//! Driving one nonce from signature to receipt.
//!
//! Used by job workers, the treasury and recovery alike. The rules encoded here:
//!
//! 1. **Bind before broadcast.** The signed bytes and the nonce binding are committed (under the lease
//!    epoch) before anything is sent. If the commit is ambiguous, the store is asked whether the attempt
//!    exists before deciding.
//! 2. **Never re-sign to recreate a transaction.** Rebroadcasts send the stored bytes. A new signature is
//!    a new attempt, persisted through the same bind.
//! 3. **A send error is a classification, not a verdict.** Only an explicit stateless rejection (on
//!    chains where that is trustworthy) or a nonce provably consumed by someone else's transaction lets a
//!    job leave its nonce. Everything uncertain is resolved by watching the chain.
//! 4. **Stuck is measured in blocks.** A stalled chain never triggers escalation, and escalation
//!    diagnoses (nonce gap? funds? fees?) before it spends anything.

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{Address, Bytes, U256};
use chrono::Utc;
use uuid::Uuid;

use crate::{
    chain::{
        adapter::{ConfirmCheck, FeeQuote, GasPlan, SendErrorClass, StuckStep, TxShape},
        confirm::PendingConfirmation,
        ChainStatus,
    },
    domain::{addr_hex, hash_hex, Attempt, AttemptPurpose, AttemptStatus, Inclusion, Outcome as TxOutcome, PauseReason, RecoveryStep, SlotOwner},
    engine::{Engine, Pair},
    error::{JobFailure, RpcError, SignerError, StoreError},
    funds::treasury,
    pipeline::settle::SettleOp,
    rpc::types::Receipt,
    signer::{estimate_encoded_len, TxFields},
    store::attempts::{BindKind, VoidDisposition, VoidMode},
    telemetry,
};

/// What should be sent; how (nonce, fees) is decided here.
#[derive(Debug, Clone)]
pub struct TxIntent {
    pub owner: SlotOwner,
    pub purpose: AttemptPurpose,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
    pub gas_limit: u64,
}

#[derive(Debug)]
pub enum Outcome {
    /// One of the nonce's attempts was mined (it may be the cancel rather than the original).
    Mined { attempt: Box<Attempt>, inclusion: Box<Inclusion> },
    /// Nothing was bound; the caller still owns the work item and may requeue it.
    NotBound(NotBound),
    /// The binding was undone because the transaction provably never executed. When `requeued`, the job
    /// is `queued` again in the store and the caller should put it back at the front of the queue.
    Voided { requeued: bool, reason: String },
    /// The engine cannot determine or fix the transaction's fate; the pair is paused for an operator and
    /// the owner stays non-terminal.
    Unresolved,
    /// A lower nonce of this pair vanished from the chain; the pair must run recovery, which will also
    /// carry this (persisted) attempt.
    NeedsRecovery,
    /// Shutdown or lost lease. Whatever was bound is picked up by recovery at next boot.
    Aborted,
}

#[derive(Debug)]
pub enum NotBound {
    Signer(SignerError),
    /// The owner was not in a bindable state — e.g. the job was already bound by someone else.
    OwnerGone,
    Store(StoreError),
    Rpc(RpcError),
}

/// State of the nonce being driven.
struct InFlight {
    intent: TxIntent,
    nonce: u64,
    attempts: Vec<Attempt>,
    worst_case: U256,
    /// Head when the newest attempt was (re)broadcast; "stuck" counts blocks from here.
    sent_head: u64,
    sent_at: Instant,
    ladder_pos: usize,
    bumps: u32,
    /// True when the very first broadcast of a fresh nonce has not been acknowledged yet. In that window a
    /// "nonce too low" can only mean somebody else's transaction took the nonce.
    never_accepted: bool,
}

pub struct TxDriver<'a> {
    pub engine: &'a Arc<Engine>,
    pub pair: &'a Arc<Pair>,
}

impl<'a> TxDriver<'a> {
    pub fn new(engine: &'a Arc<Engine>, pair: &'a Arc<Pair>) -> Self {
        Self { engine, pair }
    }

    fn worst_case(&self, gas_limit: u64, value: U256, data_len: usize, fees: &FeeQuote) -> U256 {
        let chain = &self.pair.chain;
        chain.adapter.worst_case_cost(&TxShape { gas_limit, value, encoded_len: estimate_encoded_len(data_len) }, fees, &chain.cost_context())
    }

    /// Worst-case cost of `intent` at `fees`, as the ledger would reserve it.
    pub fn cost_of(&self, intent: &TxIntent, fees: &FeeQuote) -> U256 {
        self.worst_case(intent.gas_limit, intent.value, intent.data.len(), fees)
    }

    /// Signs, binds, broadcasts and resolves a new transaction at the pair's next nonce.
    pub async fn execute(&self, intent: TxIntent, fees: FeeQuote) -> Outcome {
        let nonce = self.pair.next_nonce();
        let attempt = match self.sign(&intent, nonce, &fees, intent.purpose, intent.to, intent.data.clone(), intent.value, intent.gas_limit).await {
            Ok(a) => a,
            Err(e) => return Outcome::NotBound(NotBound::Signer(e)),
        };
        match self.bind(&attempt, BindKind::First).await {
            Ok(()) => {}
            Err(StoreError::Fenced { .. }) => {
                self.engine.stop_sending.cancel();
                return Outcome::Aborted;
            }
            Err(StoreError::StateConflict { .. }) => return Outcome::NotBound(NotBound::OwnerGone),
            Err(e) => return Outcome::NotBound(NotBound::Store(e)),
        }
        self.pair.advance_nonce();
        let worst_case = self.cost_of(&intent, &fees);
        self.pair.ledger.reserve(nonce, worst_case);

        let inflight = InFlight {
            intent,
            nonce,
            attempts: vec![attempt],
            worst_case,
            sent_head: self.pair.chain.head(),
            sent_at: Instant::now(),
            ladder_pos: 0,
            bumps: 0,
            never_accepted: true,
        };
        self.resolve(inflight, true).await
    }

    /// Resumes a nonce whose attempts were loaded from the store (recovery). `attempts` are all live
    /// attempts of one nonce, oldest first.
    pub async fn resume(&self, attempts: Vec<Attempt>) -> Outcome {
        let Some(first) = attempts.first() else { return Outcome::Unresolved };
        let job_attempt = attempts.iter().find(|a| a.purpose != AttemptPurpose::Cancel).unwrap_or(first);
        let intent =
            TxIntent { owner: first.owner, purpose: job_attempt.purpose, to: Address::ZERO, data: Bytes::new(), value: job_attempt.value, gas_limit: job_attempt.gas_limit };
        let worst_case = attempts
            .iter()
            .map(|a| self.worst_case(a.gas_limit, a.value, a.raw_tx.len(), &FeeQuote { max_fee_per_gas: a.max_fee_per_gas, max_priority_fee_per_gas: a.max_priority_fee_per_gas }))
            .max()
            .unwrap_or_default();
        self.pair.ledger.reserve(first.nonce, worst_case);
        let cancelled = attempts.iter().any(|a| a.purpose == AttemptPurpose::Cancel);
        let ladder_len = self.pair.chain.adapter.stuck_ladder().len();
        let inflight = InFlight {
            intent,
            nonce: first.nonce,
            attempts,
            worst_case,
            sent_head: self.pair.chain.head(),
            sent_at: Instant::now(),
            // A nonce that already carries a cancel is at the end of its ladder.
            ladder_pos: if cancelled { ladder_len } else { 0 },
            bumps: 0,
            never_accepted: false,
        };
        // The stored bytes are rebroadcast first: whatever the node says tells us where the nonce stands.
        self.resolve(inflight, true).await
    }

    #[allow(clippy::too_many_arguments)]
    async fn sign(
        &self,
        intent: &TxIntent,
        nonce: u64,
        fees: &FeeQuote,
        purpose: AttemptPurpose,
        to: Address,
        data: Bytes,
        value: U256,
        gas_limit: u64,
    ) -> Result<Attempt, SignerError> {
        let fields = TxFields {
            chain_id: self.pair.key.chain_id,
            nonce,
            to,
            data,
            value,
            gas_limit,
            max_fee_per_gas: fees.max_fee_per_gas,
            max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
        };
        let signed = self.pair.signer.sign(&fields).await?;
        Ok(Attempt {
            id: Uuid::now_v7(),
            chain_id: self.pair.key.chain_id,
            signer: self.pair.key.signer,
            nonce,
            purpose,
            owner: intent.owner,
            tx_hash: signed.hash,
            raw_tx: signed.raw,
            gas_limit,
            max_fee_per_gas: fees.max_fee_per_gas,
            max_priority_fee_per_gas: fees.max_priority_fee_per_gas,
            value,
            status: AttemptStatus::Broadcast,
            block_number: None,
            block_hash: None,
            created_at: Utc::now(),
        })
    }

    /// Commits the attempt. An ambiguous failure (connection lost around COMMIT) is resolved by asking the
    /// store whether the attempt exists: the signed bytes are fixed, so the bind can simply be retried.
    async fn bind(&self, attempt: &Attempt, kind: BindKind) -> Result<(), StoreError> {
        let mut delay = Duration::from_millis(50);
        for round in 0..6 {
            match self.engine.store.bind_attempt(self.engine.epoch(), attempt, kind).await {
                Ok(()) => return Ok(()),
                Err(e) if e.is_transient() => {
                    if let Ok(true) = self.engine.store.attempt_exists(&attempt.tx_hash).await {
                        return Ok(());
                    }
                    if let Some(suppressed) = telemetry::throttled("bind.retry", Duration::from_secs(10)) {
                        tracing::warn!(event = "bind.retry", chain = attempt.chain_id, signer = %addr_hex(&attempt.signer), nonce = attempt.nonce, round, code = e.code(), error = %e, suppressed, "bind did not commit; retrying");
                    }
                    if round == 5 {
                        return Err(e);
                    }
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(Duration::from_secs(2));
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("bind loop returns from inside")
    }

    /// Sends `attempt`'s stored bytes. `Ok(Some)` = mined (sync send), `Ok(None)` = accepted.
    async fn broadcast(&self, attempt: &Attempt, allow_sync: bool) -> Result<Option<Receipt>, RpcError> {
        let chain = &self.pair.chain;
        if allow_sync && chain.sync_send.load(std::sync::atomic::Ordering::Relaxed) {
            // The HTTP timeout sits above the node's own sync timeout so the node answers first.
            let timeout = Duration::from_millis(chain.tunables.sync_send_timeout_ms + 5_000);
            chain.rpc.send_raw_sync(&attempt.raw_tx, timeout).await.map(Some)
        } else {
            chain.rpc.send_raw(&attempt.raw_tx, 2).await.map(|_| None)
        }
    }

    async fn resolve(&self, mut f: InFlight, mut broadcast_pending: bool) -> Outcome {
        let chain = self.pair.chain.clone();
        let t = &chain.tunables;
        let base_poll = Duration::from_millis(t.receipt_poll_interval_ms.max(50));
        let mut poll = base_poll;
        let mut first_broadcast = true;
        let mut parked = false;

        loop {
            if self.engine.stop_sending.is_cancelled() {
                return Outcome::Aborted;
            }

            if broadcast_pending {
                broadcast_pending = false;
                let attempt = f.attempts.last().expect("in-flight nonce has an attempt").clone();
                // Only the first send holds the connection open for a receipt; rebroadcasts are fire-and-forget.
                let result = self.broadcast(&attempt, first_broadcast).await;
                first_broadcast = false;
                f.sent_head = chain.head().max(f.sent_head);
                f.sent_at = Instant::now();
                match result {
                    Ok(Some(receipt)) => {
                        if let Some(outcome) = self.on_receipt(&mut f, receipt).await {
                            return outcome;
                        }
                    }
                    Ok(None) => f.never_accepted = false,
                    Err(e) if e.is_definitely_not_delivered() => {
                        // Throttled or breaker open: the node never saw it. Try again shortly.
                        broadcast_pending = true;
                        first_broadcast = f.never_accepted;
                        tokio::time::sleep(Duration::from_millis(500)).await;
                        continue;
                    }
                    Err(e) => match self.on_send_error(&mut f, &attempt, e).await {
                        Next::Poll => {}
                        Next::Rebroadcast => broadcast_pending = true,
                        Next::Done(outcome) => return outcome,
                    },
                }
                continue;
            }

            tokio::select! {
                _ = tokio::time::sleep(poll) => {}
                _ = self.engine.stop_sending.cancelled() => return Outcome::Aborted,
            }
            poll = (poll * 3 / 2).min(base_poll * 8);

            // Newest first: a replacement is the most likely one to land.
            for i in (0..f.attempts.len()).rev() {
                let hash = f.attempts[i].tx_hash;
                match chain.rpc.get_receipt(hash, chain.rpc.read_opts()).await {
                    Ok(Some(receipt)) => {
                        if let Some(outcome) = self.on_receipt(&mut f, receipt).await {
                            return outcome;
                        }
                    }
                    Ok(None) => {}
                    Err(_) => break, // logged by the rpc layer; the breaker and monitor take it from here
                }
            }

            // A degraded or down chain freezes escalation: nothing can be concluded from silence.
            if chain.status() != ChainStatus::Healthy {
                f.sent_at = Instant::now();
                continue;
            }
            if parked {
                poll = Duration::from_secs(30);
                continue;
            }
            let stuck_after = Duration::from_millis(t.stuck_after_blocks.max(1) * t.block_time_ms.max(1));
            if f.sent_at.elapsed() < stuck_after {
                continue;
            }
            // Time alone proves nothing; require the head to have moved the same number of blocks.
            let head = match chain.rpc.get_block("latest", chain.rpc.read_opts()).await {
                Ok(Some(block)) => {
                    chain.observe_block(&block);
                    block.number_u64()
                }
                _ => continue,
            };
            if head < f.sent_head + t.stuck_after_blocks {
                continue;
            }

            match self.escalate(&mut f).await {
                Next::Poll => poll = base_poll,
                Next::Rebroadcast => {
                    broadcast_pending = true;
                    poll = base_poll;
                }
                Next::Done(Outcome::Unresolved) => {
                    // Keep watching slowly: if it lands after all, the pair heals without an operator.
                    parked = true;
                }
                Next::Done(outcome) => return outcome,
            }
        }
    }

    /// Turns a receipt into a settled inclusion. Returns `None` when the receipt is not usable yet.
    async fn on_receipt(&self, f: &mut InFlight, receipt: Receipt) -> Option<Outcome> {
        let attempt = f.attempts.iter().find(|a| a.tx_hash == receipt.transaction_hash)?.clone();
        let inclusion = inclusion_from(self.pair, &attempt, &receipt)?;
        self.settle(&attempt, &inclusion, f.worst_case, false).await;
        // A mined transaction is proof that whatever held this nonce back is over: it was not stuck after
        // all, and the sender evidently had the funds.
        for reason in [PauseReason::StuckUnresolved, PauseReason::InsufficientFunds] {
            self.pair.resume_if(&self.engine.store, reason);
        }
        Some(Outcome::Mined { attempt: Box::new(attempt), inclusion: Box::new(inclusion) })
    }

    /// Applies an inclusion to memory immediately and queues the durable write.
    pub async fn settle(&self, attempt: &Attempt, inclusion: &Inclusion, _worst_case: U256, reincluded: bool) {
        let chain = &self.pair.chain;
        let transferred = if inclusion.outcome == TxOutcome::Success { attempt.value } else { U256::ZERO };
        self.pair.ledger.settle(attempt.nonce, inclusion.fee_paid.saturating_add(transferred), inclusion.block_number);
        chain.observe_head(inclusion.block_number, Some(inclusion.block_hash));
        chain.observe_receipt_price(inclusion.effective_gas_price, attempt.max_priority_fee_per_gas.min(attempt.max_fee_per_gas));
        if let Some(l1) = inclusion.l1_fee {
            chain.observe_l1_fee(l1, attempt.raw_tx.len());
        }

        let confirm_now = chain.confirm_check() == ConfirmCheck::Immediate;
        if let Some(settler) = self.engine.settler(chain.chain_id) {
            let op = SettleOp::Inclusion { attempt: attempt.clone(), inclusion: inclusion.clone(), confirm_now, reincluded };
            if settler.send(op).await.is_err() {
                tracing::error!(event = "settle.queue_closed", chain = chain.chain_id, tx_hash = %hash_hex(&attempt.tx_hash), "settle queue is closed; recovery will re-derive this inclusion");
            }
        }
        if !confirm_now {
            if let Some(confirmer) = self.engine.confirmers.read().get(&chain.chain_id) {
                let due = Instant::now() + Duration::from_millis(chain.tunables.confirmation_delay_ms);
                let _ = confirmer.send(PendingConfirmation { attempt: attempt.clone(), inclusion: inclusion.clone(), worst_case: _worst_case, due, misses: 0 });
            }
        }
    }

    async fn on_send_error(&self, f: &mut InFlight, attempt: &Attempt, err: RpcError) -> Next {
        let chain = &self.pair.chain;
        let class = chain.adapter.classify_send_error(&err);
        let fresh = f.never_accepted;
        tracing::debug!(event = "tx.send_error", chain = chain.chain_id, signer = %addr_hex(&attempt.signer), nonce = attempt.nonce, tx_hash = %hash_hex(&attempt.tx_hash), class = ?class, error = %err, "send was not acknowledged cleanly");

        match class {
            SendErrorClass::AlreadyKnown | SendErrorClass::AcceptedPending => {
                f.never_accepted = false;
                Next::Poll
            }
            SendErrorClass::Indeterminate => Next::Poll,
            SendErrorClass::PoolBusy => {
                tokio::time::sleep(Duration::from_secs(1)).await;
                Next::Rebroadcast
            }
            SendErrorClass::NonceTooLow => self.on_nonce_too_low(f, attempt, fresh).await,
            SendErrorClass::NonceTooHigh => Next::Done(Outcome::NeedsRecovery),
            SendErrorClass::FeeTooLow => self.replace_with_fresh_fees(f, "node reported the fee as too low").await,
            SendErrorClass::InsufficientFunds => self.fund_and_retry(f).await,
            SendErrorClass::Deterministic => {
                if fresh && chain.adapter.capabilities().trust_stateless_rejects {
                    self.void(
                        f,
                        attempt,
                        VoidMode::NonceUnused,
                        VoidDisposition::Fail { failure: JobFailure::InvalidTx, message: &format!("rejected by the node: {err}") },
                        format!("stateless reject: {err}"),
                    )
                    .await
                } else {
                    // Not provably unexecutable here; let the chain tell us what happened.
                    Next::Poll
                }
            }
        }
    }

    async fn on_nonce_too_low(&self, f: &mut InFlight, attempt: &Attempt, fresh: bool) -> Next {
        let chain = &self.pair.chain;
        // Most likely one of our own attempts was mined. Look before concluding anything; a lagging
        // backend can answer null once, so ask a few times.
        for wait_ms in [0u64, 300, 1_000, 2_500] {
            tokio::time::sleep(Duration::from_millis(wait_ms)).await;
            for a in f.attempts.clone().iter().rev() {
                if let Ok(Some(receipt)) = chain.rpc.get_receipt(a.tx_hash, chain.rpc.read_opts()).await {
                    if let Some(outcome) = self.on_receipt(f, receipt).await {
                        return Next::Done(outcome);
                    }
                }
            }
        }
        if fresh {
            // These bytes were never accepted anywhere and the nonce is consumed: someone else's
            // transaction holds it, so ours provably never ran. Give the job a new nonce.
            telemetry::alert(
                &format!("nonce.foreign:{}", self.pair.key),
                "nonce.foreign_use",
                "a nonce was consumed by a transaction this engine did not send; is the key used elsewhere?",
                Some(chain.chain_id),
                Some(&addr_hex(&self.pair.key.signer)),
                serde_json::json!({"nonce": attempt.nonce}),
            );
            let next = self.void(f, attempt, VoidMode::NonceTakenByForeignTx, VoidDisposition::Requeue, "nonce consumed by a foreign transaction".into()).await;
            // The local nonce is behind the chain; recovery raises it to the chain's floor.
            self.pair.request_recovery();
            return next;
        }
        self.pair.pause(
            &self.engine.store,
            PauseReason::NonceDrift,
            RecoveryStep::NeedsOperator,
            format!("nonce {} is consumed on-chain but none of our transactions for it has a receipt", attempt.nonce),
        );
        telemetry::alert(
            &format!("nonce.drift:{}", self.pair.key),
            "nonce.drift",
            "nonce consumed on-chain without a receipt for any of our attempts; operator attention needed",
            Some(chain.chain_id),
            Some(&addr_hex(&self.pair.key.signer)),
            serde_json::json!({"nonce": attempt.nonce, "attempts": f.attempts.iter().map(|a| hash_hex(&a.tx_hash)).collect::<Vec<_>>()}),
        );
        Next::Done(Outcome::Unresolved)
    }

    async fn void(&self, f: &InFlight, attempt: &Attempt, mode: VoidMode, disposition: VoidDisposition<'_>, reason: String) -> Next {
        match self.engine.store.void_binding(self.engine.epoch(), attempt, mode, disposition).await {
            Ok(()) => {
                self.engine.webhook_wake.notify_one();
                self.pair.ledger.release(f.nonce);
                if mode == VoidMode::NonceUnused {
                    self.pair.set_next_nonce(f.nonce);
                }
                Next::Done(Outcome::Voided { requeued: matches!(disposition, VoidDisposition::Requeue), reason })
            }
            Err(StoreError::Fenced { .. }) => {
                self.engine.stop_sending.cancel();
                Next::Done(Outcome::Aborted)
            }
            Err(e) => {
                // The binding stands; treat the nonce as live and let polling/escalation deal with it.
                tracing::error!(event = "tx.void_failed", chain = attempt.chain_id, signer = %addr_hex(&attempt.signer), nonce = attempt.nonce, code = e.code(), error = %e, "could not undo binding; keeping the nonce in flight");
                Next::Poll
            }
        }
    }

    /// New attempt at the same nonce with fees quoted against a freshly read base fee.
    async fn replace_with_fresh_fees(&self, f: &mut InFlight, why: &str) -> Next {
        let chain = &self.pair.chain;
        let t = &chain.tunables;
        if f.bumps >= t.max_bumps.max(1) {
            return Next::Poll; // escalation continues down the ladder (cancel / operator)
        }
        let Ok(base) = chain.base_fee(true).await else { return Next::Poll };
        let last = f.attempts.last().expect("attempt present");
        let prev = FeeQuote { max_fee_per_gas: last.max_fee_per_gas, max_priority_fee_per_gas: last.max_priority_fee_per_gas };
        let quote = if chain.adapter.capabilities().replacement {
            chain.adapter.replacement(&prev, base, t.max_fee_cap_wei, t)
        } else {
            // No pool means nothing to out-bid: a plain fresh quote is a valid new attempt.
            Some(chain.adapter.fee_quote(base, t)).filter(|q| q.max_fee_per_gas > prev.max_fee_per_gas)
        };
        let Some(quote) = quote else {
            tracing::warn!(event = "tx.fee_cap_reached", chain = chain.chain_id, signer = %addr_hex(&self.pair.key.signer), nonce = f.nonce, base_fee = base, max_fee_cap = t.max_fee_cap_wei, "cannot raise fees further within the cap");
            return Next::Poll;
        };
        let original = f.attempts.iter().find(|a| a.purpose != AttemptPurpose::Cancel).cloned();
        let Some(original) = original else { return Next::Poll };
        let Some((to, data)) = decode_target(&original) else { return Next::Poll };
        let attempt = match self.sign(&f.intent, f.nonce, &quote, original.purpose, to, data, original.value, original.gas_limit).await {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(event = "tx.replace_sign_failed", chain = chain.chain_id, nonce = f.nonce, code = e.code(), error = %e, "could not sign replacement");
                return Next::Poll;
            }
        };
        match self.bind(&attempt, BindKind::Replacement).await {
            Ok(()) => {}
            Err(StoreError::Fenced { .. }) => {
                self.engine.stop_sending.cancel();
                return Next::Done(Outcome::Aborted);
            }
            Err(e) => {
                tracing::warn!(event = "tx.replace_bind_failed", chain = chain.chain_id, nonce = f.nonce, code = e.code(), error = %e, "could not persist replacement; keeping the current attempt");
                return Next::Poll;
            }
        }
        f.bumps += 1;
        f.worst_case = f.worst_case.max(self.worst_case(attempt.gas_limit, attempt.value, attempt.raw_tx.len(), &quote));
        self.pair.ledger.reserve(f.nonce, f.worst_case);
        tracing::warn!(
            event = "tx.replaced",
            chain = chain.chain_id,
            signer = %addr_hex(&self.pair.key.signer),
            nonce = f.nonce,
            owner = %f.intent.owner.id(),
            bump = f.bumps,
            max_fee_per_gas = quote.max_fee_per_gas,
            tx_hash = %hash_hex(&attempt.tx_hash),
            why,
            "replaced transaction with higher fees"
        );
        f.attempts.push(attempt);
        Next::Rebroadcast
    }

    /// The node says the sender cannot pay. Trust the chain over the ledger, top up, and resend.
    async fn fund_and_retry(&self, f: &mut InFlight) -> Next {
        let chain = &self.pair.chain;
        let ticket = self.pair.ledger.begin_reconcile();
        if let Ok(balance) = chain.rpc.get_balance(self.pair.key.signer, chain.rpc.read_opts()).await {
            self.pair.ledger.reconcile(ticket, balance, chain.head());
        }
        if self.pair.role == crate::domain::PairRole::Treasury {
            // Nobody can top up the treasury but a human.
            self.pair.pause(&self.engine.store, PauseReason::InsufficientFunds, RecoveryStep::AwaitingTreasuryRefill, "treasury cannot cover its own transaction");
            tokio::time::sleep(Duration::from_secs(15)).await;
            return Next::Rebroadcast;
        }
        match treasury::request_topup(self.engine, self.pair, f.worst_case).await {
            Ok(()) => {
                self.pair.resume_if(&self.engine.store, PauseReason::InsufficientFunds);
                Next::Rebroadcast
            }
            Err(reason) => {
                // The job is bound to this nonce for life, so the pair waits here (paused) until funded.
                self.pair.pause(&self.engine.store, PauseReason::InsufficientFunds, RecoveryStep::AwaitingTreasuryRefill, reason);
                tokio::time::sleep(Duration::from_secs(15)).await;
                Next::Rebroadcast
            }
        }
    }

    /// One step down the chain kind's stuck ladder.
    async fn escalate(&self, f: &mut InFlight) -> Next {
        let chain = &self.pair.chain;
        let ladder = chain.adapter.stuck_ladder();
        let Some(step) = ladder.get(f.ladder_pos).copied() else {
            self.pair.pause(
                &self.engine.store,
                PauseReason::StuckUnresolved,
                RecoveryStep::NeedsOperator,
                format!("nonce {} is not being mined and every automatic remedy has been tried", f.nonce),
            );
            telemetry::alert(
                &format!("tx.stuck:{}", self.pair.key),
                "tx.stuck_unresolved",
                "a transaction is stuck and automatic remedies are exhausted; operator attention needed",
                Some(chain.chain_id),
                Some(&addr_hex(&self.pair.key.signer)),
                serde_json::json!({"nonce": f.nonce, "owner": f.intent.owner.id(), "attempts": f.attempts.iter().map(|a| hash_hex(&a.tx_hash)).collect::<Vec<_>>()}),
            );
            return Next::Done(Outcome::Unresolved);
        };
        tracing::warn!(event = "tx.stuck", chain = chain.chain_id, signer = %addr_hex(&self.pair.key.signer), nonce = f.nonce, owner = %f.intent.owner.id(), step = ?step, blocks_waited = chain.head().saturating_sub(f.sent_head), "transaction not mined; escalating");
        metrics::counter!("gum_stuck_escalations_total", "chain" => chain.name.clone(), "step" => format!("{step:?}")).increment(1);

        match step {
            StuckStep::Rebroadcast => {
                f.ladder_pos += 1;
                Next::Rebroadcast
            }
            StuckStep::Diagnose => {
                f.ladder_pos += 1;
                let Ok(chain_nonce) = chain.rpc.get_transaction_count(self.pair.key.signer, chain.rpc.read_opts()).await else {
                    f.ladder_pos -= 1; // could not look; try again next round
                    return Next::Poll;
                };
                if chain_nonce < f.nonce {
                    return Next::Done(Outcome::NeedsRecovery);
                }
                if chain_nonce > f.nonce {
                    let last = f.attempts.last().expect("attempt present").clone();
                    return self.on_nonce_too_low(f, &last, false).await;
                }
                // Our turn on-chain, so it is either funds or fees.
                if let Ok(balance) = chain.rpc.get_balance(self.pair.key.signer, chain.rpc.read_opts()).await {
                    if chain.adapter.spendable(balance) < f.worst_case {
                        return self.fund_and_retry(f).await;
                    }
                }
                Next::Poll
            }
            StuckStep::Bump => {
                let last = f.attempts.last().expect("attempt present");
                let base = chain.base_fee(true).await.unwrap_or(0);
                let underpriced = base.saturating_add(last.max_priority_fee_per_gas) > last.max_fee_per_gas;
                if underpriced && f.bumps < chain.tunables.max_bumps {
                    // Stay on this step while the fee keeps being the problem.
                    return self.replace_with_fresh_fees(f, "base fee rose above max fee").await;
                }
                f.ladder_pos += 1;
                // Fees are fine: the node may simply have dropped it.
                Next::Rebroadcast
            }
            StuckStep::Noop => {
                f.ladder_pos += 1;
                self.cancel(f).await
            }
        }
    }

    /// Replaces the nonce with a zero-value self-transfer. The job fails only once that is *confirmed*;
    /// until then every attempt stays tracked, and if the original lands first the job simply succeeds.
    async fn cancel(&self, f: &mut InFlight) -> Next {
        let chain = &self.pair.chain;
        let t = &chain.tunables;
        let last = f.attempts.last().expect("attempt present");
        let prev = FeeQuote { max_fee_per_gas: last.max_fee_per_gas, max_priority_fee_per_gas: last.max_priority_fee_per_gas };
        let base = chain.base_fee(true).await.unwrap_or(0);
        let Some(quote) = chain.adapter.replacement(&prev, base, t.cancel_fee_cap_wei, t) else {
            tracing::error!(event = "tx.cancel_impossible", chain = chain.chain_id, nonce = f.nonce, "no acceptable fee quote for a cancel within cancel_fee_cap_wei");
            return Next::Poll;
        };
        let me = self.pair.key.signer;
        let gas = match chain.adapter.transfer_gas() {
            GasPlan::Fixed(g) => g,
            GasPlan::Estimate => match chain.rpc.estimate_gas(me, me, &Bytes::new(), U256::ZERO, chain.rpc.read_opts()).await {
                Ok(g) => g.saturating_mul(t.estimate_multiplier_bps as u64) / 10_000,
                Err(_) => return Next::Poll,
            },
        };
        let attempt = match self.sign(&f.intent, f.nonce, &quote, AttemptPurpose::Cancel, me, Bytes::new(), U256::ZERO, gas).await {
            Ok(a) => a,
            Err(_) => return Next::Poll,
        };
        match self.bind(&attempt, BindKind::Cancel).await {
            Ok(()) => {}
            Err(StoreError::Fenced { .. }) => {
                self.engine.stop_sending.cancel();
                return Next::Done(Outcome::Aborted);
            }
            Err(e) => {
                tracing::warn!(event = "tx.cancel_bind_failed", chain = chain.chain_id, nonce = f.nonce, code = e.code(), error = %e, "could not persist cancel");
                return Next::Poll;
            }
        }
        tracing::warn!(event = "tx.cancelling", chain = chain.chain_id, signer = %addr_hex(&me), nonce = f.nonce, owner = %f.intent.owner.id(), tx_hash = %hash_hex(&attempt.tx_hash), "sending cancel transaction for stuck nonce");
        f.worst_case = f.worst_case.max(self.worst_case(gas, U256::ZERO, 0, &quote));
        self.pair.ledger.reserve(f.nonce, f.worst_case);
        f.attempts.push(attempt);
        Next::Rebroadcast
    }
}

enum Next {
    Poll,
    Rebroadcast,
    Done(Outcome),
}

/// Reads the receipt through the chain adapter. `None` when the receipt lacks block data (not mined yet).
pub fn inclusion_from(pair: &Pair, attempt: &Attempt, receipt: &Receipt) -> Option<Inclusion> {
    let block_number = receipt.block_number?.saturating_to::<u64>();
    let block_hash = receipt.block_hash?;
    let cost = pair.chain.adapter.actual_cost(attempt.gas_limit, receipt);
    Some(Inclusion {
        tx_hash: receipt.transaction_hash,
        block_number,
        block_hash,
        outcome: TxOutcome::from_receipt_status(receipt.succeeded()),
        gas_used: receipt.gas_used_u64(),
        effective_gas_price: receipt.effective_gas_price_u128(),
        fee_paid: cost.fee_paid,
        l1_fee: cost.l1_fee,
    })
}

/// Recovers `(to, calldata)` from stored signed bytes, so a replacement never depends on anything but
/// what was persisted.
fn decode_target(attempt: &Attempt) -> Option<(Address, Bytes)> {
    use alloy::{
        consensus::{Transaction, TxEnvelope},
        eips::eip2718::Decodable2718,
    };
    let envelope = TxEnvelope::decode_2718(&mut attempt.raw_tx.as_ref()).ok()?;
    Some((envelope.to()?, envelope.input().clone()))
}
