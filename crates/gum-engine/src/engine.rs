//! Shared runtime state: the engine handle every task and API route works from, and the in-memory pair.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicBool, AtomicI64, Ordering},
    sync::Arc,
};

use alloy::primitives::Address;
use chrono::Utc;
use parking_lot::{Mutex, RwLock};
use tokio::sync::{mpsc, Notify};
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::{
    chain::ChainCtx,
    config::Config,
    domain::{addr_hex, PairKey, PairRole, Pause, PauseReason, RecoveryStep},
    funds::{ledger::Ledger, treasury::TopupRequest},
    pipeline::settle::SettleOp,
    signer::SignerHandle,
    stats::Stats,
    store::{batcher::IngestBatcher, Store},
};

pub struct Engine {
    pub cfg: Config,
    pub store: Store,
    pub ingest: IngestBatcher,
    pub stats: Stats,
    pub chains: BTreeMap<u64, Arc<ChainCtx>>,
    /// Populated once this instance becomes the leader.
    pub pairs: RwLock<BTreeMap<PairKey, Arc<Pair>>>,
    pub treasuries: RwLock<BTreeMap<u64, TreasuryHandle>>,
    /// Per-chain write-behind queues for post-inclusion writes.
    pub settlers: RwLock<BTreeMap<u64, mpsc::Sender<SettleOp>>>,
    pub confirmers: RwLock<BTreeMap<u64, mpsc::UnboundedSender<crate::chain::confirm::PendingConfirmation>>>,
    pub leader: AtomicBool,
    pub epoch: AtomicI64,
    /// Process shutdown: stop taking new work.
    pub shutdown: CancellationToken,
    /// Hard stop for anything that could broadcast (lease lost, or drain deadline reached).
    pub stop_sending: CancellationToken,
    pub prometheus: metrics_exporter_prometheus::PrometheusHandle,
    pub http: reqwest::Client,
    /// Operator kill switch across all chains.
    pub global_pause: AtomicBool,
    /// Signalled whenever a write may have produced webhook outbox rows, so delivery starts at once
    /// instead of at the dispatcher's next poll.
    pub webhook_wake: Notify,
    /// Read-held by every ingest; write-held while a new leader rebuilds its counters and queue, so a job
    /// accepted at that instant is counted and enqueued exactly once.
    pub ingest_gate: tokio::sync::RwLock<()>,
}

#[derive(Clone)]
pub struct TreasuryHandle {
    pub pair: Arc<Pair>,
    pub requests: mpsc::Sender<TopupRequest>,
}

impl Engine {
    pub fn is_leader(&self) -> bool {
        self.leader.load(Ordering::Relaxed)
    }

    pub fn epoch(&self) -> i64 {
        self.epoch.load(Ordering::Relaxed)
    }

    pub fn chain(&self, chain_id: u64) -> Option<Arc<ChainCtx>> {
        self.chains.get(&chain_id).cloned()
    }

    pub fn pair(&self, key: &PairKey) -> Option<Arc<Pair>> {
        self.pairs.read().get(key).cloned()
    }

    pub fn pairs_on(&self, chain_id: u64) -> Vec<Arc<Pair>> {
        self.pairs.read().values().filter(|p| p.key.chain_id == chain_id).cloned().collect()
    }

    pub fn treasury(&self, chain_id: u64) -> Option<TreasuryHandle> {
        self.treasuries.read().get(&chain_id).cloned()
    }

    pub fn settler(&self, chain_id: u64) -> Option<mpsc::Sender<SettleOp>> {
        self.settlers.read().get(&chain_id).cloned()
    }
}

/// One (signer, chain) pair. It owns a nonce sequence and a balance ledger, and exactly one task — its
/// worker — ever advances them, so pairs never contend with each other.
pub struct Pair {
    pub key: PairKey,
    pub role: PairRole,
    pub signer: Arc<SignerHandle>,
    pub chain: Arc<ChainCtx>,
    pub ledger: Ledger,
    state: Mutex<PairState>,
    /// Signalled on resume, recovery requests and shutdown so an idle worker re-evaluates its state.
    pub wake: Notify,
}

#[derive(Debug, Clone)]
struct PairState {
    pause: Option<Pause>,
    manual_pause: bool,
    current: Option<Uuid>,
    next_nonce: u64,
    nonce_ready: bool,
    recovery_requested: bool,
    draining: bool,
}

#[derive(Debug, Clone)]
pub struct PairView {
    pub pause: Option<Pause>,
    pub current: Option<Uuid>,
    pub next_nonce: u64,
    pub draining: bool,
    pub nonce_ready: bool,
}

impl Pair {
    pub fn new(key: PairKey, role: PairRole, signer: Arc<SignerHandle>, chain: Arc<ChainCtx>, manual_pause: bool) -> Arc<Self> {
        let pause = Some(Pause {
            reason: PauseReason::Booting,
            recovery_step: RecoveryStep::ReconcilingNonce,
            since: Utc::now(),
            detail: "loading state and reconciling with the chain".into(),
        });
        Arc::new(Self {
            key,
            role,
            signer,
            chain,
            ledger: Ledger::default(),
            state: Mutex::new(PairState { pause, manual_pause, current: None, next_nonce: 0, nonce_ready: false, recovery_requested: true, draining: false }),
            wake: Notify::new(),
        })
    }

    pub fn address(&self) -> Address {
        self.key.signer
    }

    pub fn view(&self) -> PairView {
        let s = self.state.lock();
        PairView { pause: s.pause.clone(), current: s.current, next_nonce: s.next_nonce, draining: s.draining, nonce_ready: s.nonce_ready }
    }

    pub fn is_paused(&self) -> bool {
        self.state.lock().pause.is_some()
    }

    pub fn pause_reason(&self) -> Option<PauseReason> {
        self.state.lock().pause.as_ref().map(|p| p.reason)
    }

    pub fn next_nonce(&self) -> u64 {
        self.state.lock().next_nonce
    }

    /// Sets the nonce after reconciliation: `max(chain nonce, highest nonce ever bound + 1)`.
    pub fn set_next_nonce(&self, nonce: u64) {
        let mut s = self.state.lock();
        s.next_nonce = nonce;
        s.nonce_ready = true;
    }

    pub fn advance_nonce(&self) {
        self.state.lock().next_nonce += 1;
    }

    pub fn set_current(&self, id: Option<Uuid>) {
        self.state.lock().current = id;
    }

    pub fn set_draining(&self, draining: bool) {
        self.state.lock().draining = draining;
    }

    /// Pauses the pair (or updates what is being done about an existing pause). Returns true when the
    /// pair was not already paused for this reason.
    pub fn pause(&self, store: &Store, reason: PauseReason, step: RecoveryStep, detail: impl Into<String>) -> bool {
        let detail = detail.into();
        let (changed, pause, manual) = {
            let mut s = self.state.lock();
            if reason == PauseReason::Manual {
                s.manual_pause = true;
            }
            let changed = s.pause.as_ref().map(|p| p.reason) != Some(reason);
            let since = if changed { Utc::now() } else { s.pause.as_ref().map(|p| p.since).unwrap_or_else(Utc::now) };
            let pause = Pause { reason, recovery_step: step, since, detail };
            s.pause = Some(pause.clone());
            (changed, pause, s.manual_pause)
        };
        if changed {
            tracing::warn!(
                event = "pair.paused",
                chain = self.key.chain_id,
                signer = %addr_hex(&self.key.signer),
                reason = reason.as_str(),
                recovery_step = step.as_str(),
                detail = %pause.detail,
                "signer paused on chain"
            );
            metrics::counter!("gum_pair_pauses_total", "chain" => self.chain.name.clone(), "reason" => reason.as_str()).increment(1);
        }
        self.persist_pause(store, Some(pause), manual);
        changed
    }

    /// Clears a pause, but only if it is still the given reason — a newer, different pause wins.
    pub fn resume_if(&self, store: &Store, reason: PauseReason) -> bool {
        let manual = {
            let mut s = self.state.lock();
            if s.pause.as_ref().map(|p| p.reason) != Some(reason) {
                return false;
            }
            if reason == PauseReason::Manual {
                s.manual_pause = false;
            }
            // An operator pause outlives whatever automatic pause was layered on top of it.
            if s.manual_pause {
                s.pause = Some(Pause { reason: PauseReason::Manual, recovery_step: RecoveryStep::AwaitingOperatorResume, since: Utc::now(), detail: "paused by operator".into() });
                return false;
            }
            s.pause = None;
            s.manual_pause
        };
        tracing::info!(event = "pair.resumed", chain = self.key.chain_id, signer = %addr_hex(&self.key.signer), cleared = reason.as_str(), "signer resumed on chain");
        self.persist_pause(store, None, manual);
        self.wake.notify_waiters();
        true
    }

    pub fn is_manually_paused(&self) -> bool {
        self.state.lock().manual_pause
    }

    /// Asks the pair's worker to run the recovery routine before doing anything else.
    pub fn request_recovery(&self) {
        self.state.lock().recovery_requested = true;
        self.wake.notify_waiters();
    }

    pub fn take_recovery_request(&self) -> bool {
        std::mem::take(&mut self.state.lock().recovery_requested)
    }

    pub fn recovery_requested(&self) -> bool {
        self.state.lock().recovery_requested
    }

    fn persist_pause(&self, store: &Store, pause: Option<Pause>, manual: bool) {
        let (store, chain_id, signer) = (store.clone(), self.key.chain_id, self.key.signer);
        tokio::spawn(async move {
            if let Err(e) = store.record_pause(chain_id, &signer, pause.as_ref(), manual).await {
                tracing::warn!(event = "pair.pause_persist_failed", chain = chain_id, signer = %addr_hex(&signer), code = e.code(), error = %e, "could not persist pause state (in-memory state is authoritative)");
            }
        });
    }
}
