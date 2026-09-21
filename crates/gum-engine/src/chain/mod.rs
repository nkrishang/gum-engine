//! Chain-specific behaviour (behind `ChainAdapter`) and per-chain runtime state.

pub mod adapter;
pub mod confirm;
pub mod kinds;
pub mod monitor;
pub mod registry;

use std::{
    sync::atomic::{AtomicBool, Ordering},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy::primitives::{B256, U256};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use serde::Serialize;

use self::adapter::{ChainAdapter, ConfirmCheck, CostContext, FeeQuote};
use crate::{
    config::{ChainConfig, ChainTunables},
    error::RpcError,
    queue::JobQueue,
    rpc::{types::Block, RpcClient},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ChainStatus {
    Healthy,
    /// Something looks off (head not advancing, errors) but not long enough to call it an outage.
    Degraded,
    /// Outage: signers on this chain are paused until the monitor sees it recover.
    Down,
}

#[derive(Debug, Clone)]
pub struct HealthView {
    pub status: ChainStatus,
    pub head_number: u64,
    pub head_hash: Option<B256>,
    pub last_observed_at: Option<DateTime<Utc>>,
    /// Monotonic time of the last observation of any kind.
    pub last_observed: Option<Instant>,
    /// Monotonic time the head number last increased.
    pub last_advance: Option<Instant>,
}

struct FeeState {
    base_fee: u128,
    observed: Instant,
}

/// Everything the engine knows and needs about one configured chain.
pub struct ChainCtx {
    pub name: String,
    pub chain_id: u64,
    pub cfg: ChainConfig,
    pub adapter: Arc<dyn ChainAdapter>,
    pub tunables: ChainTunables,
    pub rpc: Arc<RpcClient>,
    pub queue: JobQueue,
    /// Whether `eth_sendRawTransactionSync` is available (probed at boot).
    pub sync_send: AtomicBool,
    /// Operator kill switch for this chain.
    pub operator_paused: AtomicBool,
    /// Set once the chain's pairs and background tasks exist (leader only).
    pub started: AtomicBool,
    health: Mutex<HealthView>,
    fees: Mutex<Option<FeeState>>,
    /// Learned L1 data fee per encoded byte (EWMA), for chains that charge it separately.
    l1_fee_per_byte: Mutex<Option<U256>>,
    /// Wakes the chain monitor for an immediate probe.
    pub probe_now: tokio::sync::Notify,
}

impl ChainCtx {
    pub fn new(name: String, cfg: ChainConfig, adapter: Arc<dyn ChainAdapter>, tunables: ChainTunables, rpc: Arc<RpcClient>, queue_window: usize) -> Self {
        Self {
            name,
            chain_id: cfg.chain_id,
            cfg,
            adapter,
            tunables,
            rpc,
            queue: JobQueue::new(queue_window),
            sync_send: AtomicBool::new(false),
            operator_paused: AtomicBool::new(false),
            started: AtomicBool::new(false),
            health: Mutex::new(HealthView { status: ChainStatus::Healthy, head_number: 0, head_hash: None, last_observed_at: None, last_observed: None, last_advance: None }),
            fees: Mutex::new(None),
            l1_fee_per_byte: Mutex::new(None),
            probe_now: tokio::sync::Notify::new(),
        }
    }

    /// The confirmation strategy in force: a zero delay means the chain finalizes on inclusion, so the
    /// inclusion *is* the confirmation and no re-check is ever made.
    pub fn confirm_check(&self) -> ConfirmCheck {
        if self.tunables.confirmation_delay_ms == 0 {
            ConfirmCheck::Immediate
        } else {
            self.adapter.confirm_check()
        }
    }

    pub fn health(&self) -> HealthView {
        self.health.lock().clone()
    }

    pub fn status(&self) -> ChainStatus {
        self.health.lock().status
    }

    pub fn is_accepting_work(&self) -> bool {
        self.status() != ChainStatus::Down && !self.operator_paused.load(Ordering::Relaxed)
    }

    pub fn head(&self) -> u64 {
        self.health.lock().head_number
    }

    /// Records a sighting of block `number`. Called for every receipt and every fetched block, which is
    /// what makes liveness monitoring free while traffic flows. Returns true when the head advanced.
    pub fn observe_head(&self, number: u64, hash: Option<B256>) -> bool {
        let mut h = self.health.lock();
        let now = Instant::now();
        h.last_observed = Some(now);
        h.last_observed_at = Some(Utc::now());
        if number > h.head_number || h.last_advance.is_none() {
            h.head_number = h.head_number.max(number);
            h.head_hash = hash.or(h.head_hash);
            h.last_advance = Some(now);
            true
        } else {
            false
        }
    }

    /// Sets the status, returning the previous one so the caller can act on transitions only.
    pub fn set_status(&self, status: ChainStatus) -> ChainStatus {
        std::mem::replace(&mut self.health.lock().status, status)
    }

    pub fn observe_block(&self, block: &Block) {
        self.observe_head(block.number_u64(), Some(block.hash));
        if let Some(base) = block.base_fee_u128() {
            self.observe_base_fee(base);
        }
    }

    pub fn observe_base_fee(&self, base_fee: u128) {
        *self.fees.lock() = Some(FeeState { base_fee, observed: Instant::now() });
    }

    /// Learns the base fee from one of our own receipts: `effective = base + tip` for EIP-1559 sends.
    pub fn observe_receipt_price(&self, effective_gas_price: u128, priority_fee: u128) {
        if effective_gas_price > 0 {
            self.observe_base_fee(effective_gas_price.saturating_sub(priority_fee));
        }
    }

    pub fn observe_l1_fee(&self, l1_fee: U256, encoded_len: usize) {
        if encoded_len == 0 {
            return;
        }
        let sample = l1_fee / U256::from(encoded_len);
        let mut slot = self.l1_fee_per_byte.lock();
        *slot = Some(match *slot {
            // EWMA with alpha = 1/4; a rising fee should be picked up within a few receipts.
            Some(prev) => (prev * U256::from(3) + sample) / U256::from(4),
            None => sample,
        });
    }

    pub fn cost_context(&self) -> CostContext {
        CostContext { l1_fee_per_byte: *self.l1_fee_per_byte.lock() }
    }

    fn fresh_base_fee(&self) -> Option<u128> {
        let fees = self.fees.lock();
        fees.as_ref().filter(|f| f.observed.elapsed() < Duration::from_millis(self.tunables.fee_ttl_ms)).map(|f| f.base_fee)
    }

    /// Last known base fee regardless of age.
    pub fn last_base_fee(&self) -> Option<u128> {
        self.fees.lock().as_ref().map(|f| f.base_fee)
    }

    /// Base fee for a new transaction. Costs no RPC call while receipts keep the observation fresh;
    /// otherwise one `eth_getBlockByNumber(latest)`, which doubles as a liveness observation.
    pub async fn base_fee(&self, force_refresh: bool) -> Result<u128, RpcError> {
        if !force_refresh {
            if let Some(base) = self.fresh_base_fee() {
                return Ok(base);
            }
        }
        let block = self.rpc.get_block("latest", self.rpc.read_opts()).await?.ok_or_else(|| RpcError::Decode("latest block is null".into()))?;
        self.observe_block(&block);
        Ok(block.base_fee_u128().unwrap_or(0))
    }

    pub async fn fee_quote(&self, force_refresh: bool) -> Result<FeeQuote, RpcError> {
        let base = self.base_fee(force_refresh).await?;
        Ok(self.adapter.fee_quote(base, &self.tunables))
    }

    pub fn send_mode(&self) -> &'static str {
        if self.sync_send.load(Ordering::Relaxed) {
            "sync"
        } else {
            "async"
        }
    }
}
