//! OP Stack rollups (Base, Optimism, Unichain, …): geth-style pool, plus an L1 data fee that is charged
//! *in addition to* `gas_used * price` and reported on the receipt as `l1Fee`.

use alloy::primitives::U256;

use super::geth::{classify_geth, gas_times_price, geth_accepts_replacement, geth_replacement};
use crate::{chain::adapter::*, config::ChainTunables, error::RpcError, rpc::types::Receipt};

pub struct OpStack;

impl ChainAdapter for OpStack {
    fn kind(&self) -> &'static str {
        "opstack"
    }

    fn defaults(&self) -> ChainTunables {
        ChainTunables {
            block_time_ms: 2_000,
            // Sync-send may return a Flashblock pre-confirmation; a small share of those never land.
            confirmation_delay_ms: 4_000,
            estimate_multiplier_bps: 12_000,
            max_fee_multiplier_bps: 20_000,
            priority_fee_wei: 1_000_000,        // 0.001 gwei
            max_fee_cap_wei: 50_000_000_000,    // 50 gwei
            cancel_fee_cap_wei: 65_000_000_000, // > 1.21x the job cap, so a cancel can always out-bid
            fee_ttl_ms: 20_000,
            stuck_after_blocks: 5,
            max_bumps: 5,
            sync_send_timeout_ms: 8_000,
            receipt_poll_interval_ms: 1_000,
            idle_probe_interval_ms: 60_000,
            degraded_probe_interval_ms: 5_000,
            stall_warn_ms: 6_000,
            stall_outage_ms: 30_000,
            rpc_failure_threshold: 5,
            credits_per_call: 20,
            max_tx_gas: 16_777_216, // EIP-7825 cap (2^24)
            balance_sweep_interval_ms: 300_000,
            topup_wait_timeout_ms: 60_000,
            liveness: LivenessMode::HeadAdvance,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { replacement: true, trust_stateless_rejects: true, tip_matters: true }
    }

    fn worst_case_cost(&self, tx: &TxShape, fees: &FeeQuote, ctx: &CostContext) -> U256 {
        let l2 = U256::from(tx.gas_limit).saturating_mul(U256::from(fees.max_fee_per_gas));
        // Learned per-byte L1 fee with 2x headroom. Until the first receipt teaches us a value, reserve a
        // conservative flat amount per byte so an unfunded send cannot slip through on a cold start.
        let per_byte = ctx.l1_fee_per_byte.unwrap_or(U256::from(50_000_000_000u64));
        let l1 = per_byte.saturating_mul(U256::from(tx.encoded_len)).saturating_mul(U256::from(2));
        l2.saturating_add(l1).saturating_add(tx.value)
    }

    fn actual_cost(&self, _gas_limit: u64, receipt: &Receipt) -> ActualCost {
        let l1 = receipt.extra_u256("l1Fee");
        // Isthmus operator fee; absent or zero on most chains, so it is read defensively.
        let operator = receipt.extra_u256("operatorFee").unwrap_or(U256::ZERO);
        let fee_paid = gas_times_price(receipt).saturating_add(l1.unwrap_or(U256::ZERO)).saturating_add(operator);
        ActualCost { fee_paid, l1_fee: l1 }
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
        ConfirmCheck::TxMembership { window: 1 }
    }

    fn transfer_gas(&self) -> GasPlan {
        GasPlan::Fixed(21_000)
    }
}
