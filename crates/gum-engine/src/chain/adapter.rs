//! The single seam for chain-specific behaviour.
//!
//! Everything that differs between chains is expressed through [`ChainAdapter`]. The pipeline, funds,
//! store and API layers never branch on a chain id or kind (a test enforces it). Adapters are pure: no
//! I/O, no clocks — every input is passed in — so each one is verified against recorded fixtures by the
//! shared conformance suite.
//!
//! Adding a chain of an existing kind is configuration only. Adding a new kind is one file in `kinds/`,
//! one line in `registry.rs`, fixtures under `fixtures/<kind>/`, and a green conformance run.

use alloy::primitives::U256;

use crate::{config::ChainTunables, error::RpcError, rpc::types::Receipt};

/// Fees chosen for one signed transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FeeQuote {
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

/// The cost-relevant shape of a transaction about to be signed.
#[derive(Debug, Clone, Copy)]
pub struct TxShape {
    pub gas_limit: u64,
    pub value: U256,
    /// Length of the signed, encoded transaction in bytes (estimated before signing).
    pub encoded_len: usize,
}

/// Chain-wide observations the cost model may need.
#[derive(Debug, Clone, Copy, Default)]
pub struct CostContext {
    /// Learned L1 data fee per encoded byte (rollups that charge it separately); `None` when unknown.
    pub l1_fee_per_byte: Option<U256>,
}

/// What a transaction actually cost its sender, derived from the receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActualCost {
    /// Native amount debited for fees (excluding `value`).
    pub fee_paid: U256,
    /// Portion charged separately for L1 data, when the chain reports it.
    pub l1_fee: Option<U256>,
}

/// How the node's answer to a send must be treated. The distinction that matters most:
/// only an explicit, *stateless* rejection proves the transaction can never execute.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendErrorClass {
    /// The node already has these exact bytes: treat as a successful broadcast.
    AlreadyKnown,
    /// Accepted into the pool, but no receipt arrived within the sync-send window: track it.
    AcceptedPending,
    /// The nonce is already consumed on-chain — possibly by this very transaction.
    NonceTooLow,
    /// A lower nonce is missing on-chain; an earlier transaction of ours vanished.
    NonceTooHigh,
    /// Underpriced against the current base fee or against the transaction it should replace.
    FeeTooLow,
    /// The sender cannot cover the transaction right now.
    InsufficientFunds,
    /// The pool is temporarily refusing work (full / not ready): resend the same bytes later.
    PoolBusy,
    /// Rejected by a stateless validity rule (intrinsic gas, gas cap, size, malformed).
    Deterministic,
    /// Timeout, reset, 5xx, or an unrecognised message: the transaction may or may not be in flight.
    /// The nonce must never be reused on this signal.
    Indeterminate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StuckStep {
    /// Send the stored raw bytes again.
    Rebroadcast,
    /// Read the on-chain nonce to tell a gap from a funding or fee problem before spending anything.
    Diagnose,
    /// Replace at the same nonce with higher fees (only when the base fee outgrew `max_fee`).
    Bump,
    /// Replace with a zero-value self-transfer to free the nonce; the job fails once it confirms.
    Noop,
}

/// How an inclusion is re-verified before `transaction.confirmed` fires.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfirmCheck {
    /// The chain finalizes on inclusion (local dev chains): confirmed on receipt, no extra call.
    Immediate,
    /// The block at the inclusion height must still have the recorded hash and contain the transaction.
    BlockHash,
    /// The transaction must be a member of the block at the inclusion height, give or take `window`
    /// blocks; the canonical hash is adopted (pre-confirmation receipts may carry a provisional hash).
    TxMembership { window: u64 },
    /// The chain's `finalized` tag must have reached the inclusion height, and the block hash must match.
    FinalizedTag,
}

/// A tunable rather than adapter behaviour: a local dev chain of any kind may only mine on demand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LivenessMode {
    /// A healthy chain always produces blocks: a head that stops advancing is an outage.
    HeadAdvance,
    /// Blocks are only produced when there are transactions: judge by RPC responsiveness instead.
    RpcResponsive,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GasPlan {
    Fixed(u64),
    Estimate,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// Same-nonce replacement exists on this chain.
    pub replacement: bool,
    /// A `Deterministic` rejection proves the transaction is not in any pool, so its nonce may be given
    /// to another job. False where nodes accept first and drop later.
    pub trust_stateless_rejects: bool,
    /// The priority fee influences ordering (false where it is ignored or refunded).
    pub tip_matters: bool,
}

pub trait ChainAdapter: Send + Sync + 'static {
    /// Kind identifier used in configuration (`kind = "..."`).
    fn kind(&self) -> &'static str;

    /// Kind-level defaults for every tunable; any of them can be overridden per chain in config.
    fn defaults(&self) -> ChainTunables;

    fn capabilities(&self) -> Capabilities;

    /// Upper bound of what `tx` can cost its sender, including `value`. Used to reserve balance.
    fn worst_case_cost(&self, tx: &TxShape, fees: &FeeQuote, ctx: &CostContext) -> U256;

    /// What the mined transaction cost in fees, per this chain's charging rules.
    fn actual_cost(&self, gas_limit: u64, receipt: &Receipt) -> ActualCost;

    /// The part of a confirmed balance that may actually be spent (chains may enforce a reserve).
    fn spendable(&self, confirmed: U256) -> U256 {
        confirmed
    }

    /// Blocks after which an incoming credit (a top-up) can be spent.
    fn credit_maturity_blocks(&self) -> u64 {
        0
    }

    /// Fees for a fresh transaction given the latest known base fee.
    fn fee_quote(&self, base_fee: u128, t: &ChainTunables) -> FeeQuote {
        let scaled = base_fee.saturating_mul(t.max_fee_multiplier_bps as u128) / 10_000;
        let max_fee = scaled.saturating_add(t.priority_fee_wei).min(t.max_fee_cap_wei).max(t.priority_fee_wei);
        FeeQuote { max_fee_per_gas: max_fee, max_priority_fee_per_gas: t.priority_fee_wei.min(max_fee) }
    }

    /// Fees for replacing `prev` at the same nonce, never exceeding `cap`. `None` when replacement is
    /// unsupported or the cap leaves no acceptable quote.
    fn replacement(&self, prev: &FeeQuote, base_fee: u128, cap: u128, t: &ChainTunables) -> Option<FeeQuote>;

    /// Whether `next` would be accepted by this chain's pool as a replacement for `prev`.
    fn accepts_replacement(&self, prev: &FeeQuote, next: &FeeQuote) -> bool;

    fn classify_send_error(&self, err: &RpcError) -> SendErrorClass;

    /// Escalation steps for a transaction that is not getting mined, in order.
    fn stuck_ladder(&self) -> &'static [StuckStep];

    /// How an inclusion is re-verified. Ignored when `confirmation_delay_ms` is 0 (confirmed on receipt).
    fn confirm_check(&self) -> ConfirmCheck;

    /// Gas limit for a plain native transfer (treasury top-ups, cancels).
    fn transfer_gas(&self) -> GasPlan;
}

/// Matching helpers shared by the kinds. Providers wrap node errors in varying envelopes, so
/// classification is by case-insensitive substring, never by exact equality.
pub(crate) fn message_of(err: &RpcError) -> Option<(i64, String)> {
    match err {
        RpcError::Response { code, message, data } => {
            let mut text = message.to_lowercase();
            if let Some(d) = data {
                text.push(' ');
                text.push_str(&d.to_lowercase());
            }
            Some((*code, text))
        }
        _ => None,
    }
}

pub(crate) fn contains_any(haystack: &str, needles: &[&str]) -> bool {
    needles.iter().any(|n| haystack.contains(n))
}

/// EIP-7966: the node accepted the transaction but no receipt arrived within the sync window.
pub(crate) fn is_sync_timeout(code: i64, text: &str) -> bool {
    code == 4 || contains_any(text, &["wasn't processed", "was not processed", "not confirmed within", "timed out waiting", "timeout waiting", "sync timeout"])
}

/// `prev * (100 + pct) / 100`, rounded up, and always at least one wei more.
pub(crate) fn bump_pct(prev: u128, pct: u128) -> u128 {
    let scaled = prev.saturating_mul(100 + pct).div_ceil(100);
    scaled.max(prev.saturating_add(1))
}
