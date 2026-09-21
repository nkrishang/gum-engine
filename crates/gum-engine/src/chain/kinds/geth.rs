//! Vanilla EIP-1559 chains running a geth-style transaction pool. Also the kind used for local Anvil.
//!
//! The defaults here describe a chain that finalizes on inclusion (`confirmation_delay_ms = 0`), which is
//! exactly what a local dev chain is. A real geth-family network overrides the confirmation tunables in
//! its `[chains.*]` block.

use alloy::primitives::U256;

use crate::{chain::adapter::*, config::ChainTunables, error::RpcError, rpc::types::Receipt};

pub struct Geth;

/// go-ethereum `core/txpool/errors.go` + `core/error.go`, lowercased.
pub(crate) fn classify_geth(err: &RpcError) -> SendErrorClass {
    let Some((code, text)) = message_of(err) else { return SendErrorClass::Indeterminate };
    if is_sync_timeout(code, &text) {
        return SendErrorClass::AcceptedPending;
    }
    if contains_any(&text, &["already known", "alreadyknown", "transaction already imported", "already in mempool"]) {
        return SendErrorClass::AlreadyKnown;
    }
    if contains_any(&text, &["nonce too low"]) {
        return SendErrorClass::NonceTooLow;
    }
    if contains_any(&text, &["nonce too high", "nonce gap"]) {
        return SendErrorClass::NonceTooHigh;
    }
    if contains_any(
        &text,
        &[
            "replacement transaction underpriced",
            "transaction underpriced",
            "max fee per gas less than block base fee",
            "gas price below minimum",
            "fee cap less than block base fee",
        ],
    ) {
        return SendErrorClass::FeeTooLow;
    }
    if contains_any(&text, &["insufficient funds"]) {
        return SendErrorClass::InsufficientFunds;
    }
    if contains_any(&text, &["txpool is full", "transaction pool is full", "account limit exceeded", "pool is not ready"]) {
        return SendErrorClass::PoolBusy;
    }
    if contains_any(
        &text,
        &[
            "intrinsic gas too low",
            "exceeds block gas limit",
            "exceeds maximum per-transaction gas limit",
            "oversized data",
            "max priority fee per gas higher than max fee per gas",
            "invalid chain id",
            "invalid sender",
            "transaction type not supported",
            "max initcode size exceeded",
            "gas limit too high",
        ],
    ) {
        return SendErrorClass::Deterministic;
    }
    SendErrorClass::Indeterminate
}

/// geth's `PriceBump = 10`: both fee fields must rise by at least 10%.
pub(crate) fn geth_replacement(prev: &FeeQuote, base_fee: u128, cap: u128, t: &ChainTunables) -> Option<FeeQuote> {
    let min_fee = bump_pct(prev.max_fee_per_gas, 10);
    let min_tip = bump_pct(prev.max_priority_fee_per_gas, 10);
    // Aim for the normal quote against the current base fee, but never below the pool's minimum bump.
    let target = (base_fee.saturating_mul(t.max_fee_multiplier_bps as u128) / 10_000).saturating_add(min_tip);
    let max_fee = target.max(min_fee);
    if max_fee > cap {
        return (min_fee <= cap).then_some(FeeQuote { max_fee_per_gas: cap, max_priority_fee_per_gas: min_tip.min(cap) });
    }
    Some(FeeQuote { max_fee_per_gas: max_fee, max_priority_fee_per_gas: min_tip.min(max_fee) })
}

pub(crate) fn geth_accepts_replacement(prev: &FeeQuote, next: &FeeQuote) -> bool {
    next.max_fee_per_gas >= bump_pct(prev.max_fee_per_gas, 10) && next.max_priority_fee_per_gas >= bump_pct(prev.max_priority_fee_per_gas, 10)
}

pub(crate) fn gas_times_price(receipt: &Receipt) -> U256 {
    receipt.gas_used.saturating_mul(receipt.effective_gas_price.unwrap_or(U256::ZERO))
}

impl ChainAdapter for Geth {
    fn kind(&self) -> &'static str {
        "geth"
    }

    fn defaults(&self) -> ChainTunables {
        ChainTunables {
            block_time_ms: 1_000,
            confirmation_delay_ms: 0,
            estimate_multiplier_bps: 12_000,
            max_fee_multiplier_bps: 20_000,
            priority_fee_wei: 1_000_000_000,
            max_fee_cap_wei: 500_000_000_000,
            cancel_fee_cap_wei: 650_000_000_000,
            fee_ttl_ms: 30_000,
            stuck_after_blocks: 5,
            max_bumps: 5,
            sync_send_timeout_ms: 10_000,
            receipt_poll_interval_ms: 500,
            idle_probe_interval_ms: 60_000,
            degraded_probe_interval_ms: 5_000,
            stall_warn_ms: 5_000,
            stall_outage_ms: 15_000,
            rpc_failure_threshold: 5,
            credits_per_call: 0,
            max_tx_gas: 30_000_000,
            balance_sweep_interval_ms: 300_000,
            topup_wait_timeout_ms: 60_000,
            // A local chain may mine only on demand, so an unchanged head proves nothing: judge it by
            // whether the RPC answers. Real geth-family networks set `liveness = "head_advance"`.
            liveness: LivenessMode::RpcResponsive,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { replacement: true, trust_stateless_rejects: true, tip_matters: true }
    }

    fn worst_case_cost(&self, tx: &TxShape, fees: &FeeQuote, _ctx: &CostContext) -> U256 {
        U256::from(tx.gas_limit).saturating_mul(U256::from(fees.max_fee_per_gas)).saturating_add(tx.value)
    }

    fn actual_cost(&self, _gas_limit: u64, receipt: &Receipt) -> ActualCost {
        ActualCost { fee_paid: gas_times_price(receipt), l1_fee: None }
    }

    fn replacement(&self, prev: &FeeQuote, base_fee: u128, cap: u128, t: &ChainTunables) -> Option<FeeQuote> {
        geth_replacement(prev, base_fee, cap, t)
    }

    fn accepts_replacement(&self, prev: &FeeQuote, next: &FeeQuote) -> bool {
        geth_accepts_replacement(prev, next)
    }

    fn classify_send_error(&self, err: &RpcError) -> SendErrorClass {
        classify_geth(err)
    }

    fn stuck_ladder(&self) -> &'static [StuckStep] {
        &[StuckStep::Rebroadcast, StuckStep::Diagnose, StuckStep::Bump, StuckStep::Noop]
    }

    fn confirm_check(&self) -> ConfirmCheck {
        ConfirmCheck::BlockHash
    }

    fn transfer_gas(&self) -> GasPlan {
        GasPlan::Fixed(21_000)
    }
}
