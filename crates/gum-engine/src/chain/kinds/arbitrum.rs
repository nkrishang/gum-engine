//! Arbitrum Nitro chains (Arbitrum One, Orbit chains).
//!
//! - There is no transaction pool: the sequencer orders first-come-first-served, and a send returns only
//!   once the transaction is sequenced. Same-nonce replacement therefore does not exist.
//! - The L1 data cost is folded into the gas limit, so `gas_used * price` is the whole fee. A limit
//!   estimated while the base fee was high can be too small once it falls (`intrinsic gas too low`),
//!   hence the generous estimate multiplier — unused gas is free here.
//! - Tips are ignored and refunded.
//! - Blocks are only produced when there are transactions, so liveness is judged by RPC health.

use alloy::primitives::U256;

use super::geth::gas_times_price;
use crate::{chain::adapter::*, config::ChainTunables, error::RpcError, rpc::types::Receipt};

pub struct Arbitrum;

impl ChainAdapter for Arbitrum {
    fn kind(&self) -> &'static str {
        "arbitrum"
    }

    fn defaults(&self) -> ChainTunables {
        ChainTunables {
            block_time_ms: 250,
            confirmation_delay_ms: 2_000,
            estimate_multiplier_bps: 15_000,
            max_fee_multiplier_bps: 30_000,
            priority_fee_wei: 0,
            max_fee_cap_wei: 50_000_000_000,
            cancel_fee_cap_wei: 65_000_000_000,
            fee_ttl_ms: 15_000,
            stuck_after_blocks: 60, // ~15s of blocks under load
            max_bumps: 0,
            sync_send_timeout_ms: 8_000,
            receipt_poll_interval_ms: 500,
            idle_probe_interval_ms: 60_000,
            degraded_probe_interval_ms: 5_000,
            stall_warn_ms: 5_000,
            stall_outage_ms: 90_000, // above the sequencer's ~60s failover lockout
            rpc_failure_threshold: 5,
            credits_per_call: 20,
            max_tx_gas: 32_000_000,
            balance_sweep_interval_ms: 300_000,
            topup_wait_timeout_ms: 60_000,
            liveness: LivenessMode::RpcResponsive,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { replacement: false, trust_stateless_rejects: true, tip_matters: false }
    }

    fn worst_case_cost(&self, tx: &TxShape, fees: &FeeQuote, _ctx: &CostContext) -> U256 {
        U256::from(tx.gas_limit).saturating_mul(U256::from(fees.max_fee_per_gas)).saturating_add(tx.value)
    }

    fn actual_cost(&self, _gas_limit: u64, receipt: &Receipt) -> ActualCost {
        ActualCost { fee_paid: gas_times_price(receipt), l1_fee: None }
    }

    fn replacement(&self, _prev: &FeeQuote, _base_fee: u128, _cap: u128, _t: &ChainTunables) -> Option<FeeQuote> {
        None
    }

    fn accepts_replacement(&self, _prev: &FeeQuote, _next: &FeeQuote) -> bool {
        false
    }

    fn classify_send_error(&self, err: &RpcError) -> SendErrorClass {
        let Some((code, text)) = message_of(err) else { return SendErrorClass::Indeterminate };
        if is_sync_timeout(code, &text) {
            return SendErrorClass::AcceptedPending;
        }
        if contains_any(&text, &["already known"]) {
            return SendErrorClass::AlreadyKnown;
        }
        if contains_any(&text, &["nonce too low"]) {
            return SendErrorClass::NonceTooLow;
        }
        if contains_any(&text, &["nonce too high"]) {
            return SendErrorClass::NonceTooHigh;
        }
        if contains_any(&text, &["max fee per gas less than block base fee", "fee cap less than block base fee"]) {
            return SendErrorClass::FeeTooLow;
        }
        if contains_any(&text, &["insufficient funds"]) {
            return SendErrorClass::InsufficientFunds;
        }
        if contains_any(&text, &["queue is full", "sequencer is overloaded", "too many requests"]) {
            return SendErrorClass::PoolBusy;
        }
        if contains_any(
            &text,
            &[
                "intrinsic gas too low",
                "exceeds block gas limit",
                "oversized data",
                "max priority fee per gas higher than max fee per gas",
                "invalid chain id",
                "transaction type not supported",
            ],
        ) {
            return SendErrorClass::Deterministic;
        }
        SendErrorClass::Indeterminate
    }

    /// No Bump and no Noop: neither can exist without a pool. An unresolved transaction goes to the
    /// operator rather than being "cancelled" with something that cannot cancel it.
    fn stuck_ladder(&self) -> &'static [StuckStep] {
        &[StuckStep::Rebroadcast, StuckStep::Diagnose]
    }

    fn confirm_check(&self) -> ConfirmCheck {
        ConfirmCheck::BlockHash
    }

    /// A plain transfer still pays for its L1 calldata through the gas limit, so 21000 is not enough.
    fn transfer_gas(&self) -> GasPlan {
        GasPlan::Estimate
    }
}
