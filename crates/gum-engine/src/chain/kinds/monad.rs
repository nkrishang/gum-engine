//! Monad.
//!
//! - **Charged on the gas limit, not gas used**: `fee = gas_limit * price`. Padding a limit costs money.
//! - **Reserve balance**: each account's reserve is `min(10 MON, its balance)`. Gas is checked against
//!   it as a budget for the transactions in flight, so an account holding less than 10 MON transacts
//!   normally as long as it only pays gas. A transaction that *transfers value* reverts (and is still
//!   charged) if it would leave the balance below the reserve — so value can only come out of what an
//!   account holds above 10 MON. (The protocol exempts an account's first transaction in k blocks; a
//!   busy signer cannot rely on that.)
//! - Credits become spendable only after the delayed-execution window (k = 3 blocks).
//! - Nodes accept transactions with nonce gaps or insufficient balance and drop them later, so an
//!   accepted send proves nothing and a rejection string is never proof of non-execution.
//! - `latest` is only *Proposed*; the `finalized` tag (~2 blocks later) is the confirmation signal.
//! - Replacement needs a strictly higher `max_fee` and a tip that is not lower — no 10% rule.

use alloy::primitives::U256;

use crate::{chain::adapter::*, config::ChainTunables, error::RpcError, rpc::types::Receipt};

pub struct Monad;

/// 10 MON.
const RESERVE_WEI: u128 = 10_000_000_000_000_000_000;

impl ChainAdapter for Monad {
    fn kind(&self) -> &'static str {
        "monad"
    }

    fn defaults(&self) -> ChainTunables {
        ChainTunables {
            block_time_ms: 300,
            confirmation_delay_ms: 800,
            // Every unit of padding is paid for; keep it tight.
            estimate_multiplier_bps: 11_500,
            max_fee_multiplier_bps: 15_000,
            priority_fee_wei: 2_000_000_000,
            max_fee_cap_wei: 2_000_000_000_000, // 2000 gwei (base fee floor is 100 gwei)
            cancel_fee_cap_wei: 2_500_000_000_000,
            fee_ttl_ms: 10_000,
            stuck_after_blocks: 17, // ~5s
            max_bumps: 3,
            sync_send_timeout_ms: 5_000,
            receipt_poll_interval_ms: 400,
            idle_probe_interval_ms: 60_000,
            degraded_probe_interval_ms: 5_000,
            stall_warn_ms: 3_000,
            stall_outage_ms: 10_000,
            rpc_failure_threshold: 5,
            credits_per_call: 30,
            max_tx_gas: 30_000_000,
            balance_sweep_interval_ms: 300_000,
            topup_wait_timeout_ms: 60_000,
            liveness: LivenessMode::HeadAdvance,
        }
    }

    fn capabilities(&self) -> Capabilities {
        Capabilities { replacement: true, trust_stateless_rejects: false, tip_matters: true }
    }

    fn worst_case_cost(&self, tx: &TxShape, fees: &FeeQuote, _ctx: &CostContext) -> U256 {
        U256::from(tx.gas_limit).saturating_mul(U256::from(fees.max_fee_per_gas)).saturating_add(tx.value)
    }

    fn actual_cost(&self, gas_limit: u64, receipt: &Receipt) -> ActualCost {
        let price = receipt.effective_gas_price.unwrap_or(U256::ZERO);
        ActualCost { fee_paid: U256::from(gas_limit).saturating_mul(price), l1_fee: None }
    }

    fn spendable(&self, confirmed: U256, value: U256) -> U256 {
        if value.is_zero() {
            confirmed
        } else {
            confirmed.saturating_sub(U256::from(RESERVE_WEI))
        }
    }

    fn credit_maturity_blocks(&self) -> u64 {
        4 // k + 1
    }

    fn replacement(&self, prev: &FeeQuote, base_fee: u128, cap: u128, t: &ChainTunables) -> Option<FeeQuote> {
        let target = (base_fee.saturating_mul(t.max_fee_multiplier_bps as u128) / 10_000).saturating_add(prev.max_priority_fee_per_gas);
        let max_fee = target.max(prev.max_fee_per_gas.saturating_add(1)).min(cap);
        (max_fee > prev.max_fee_per_gas).then_some(FeeQuote { max_fee_per_gas: max_fee, max_priority_fee_per_gas: prev.max_priority_fee_per_gas.min(max_fee) })
    }

    fn accepts_replacement(&self, prev: &FeeQuote, next: &FeeQuote) -> bool {
        next.max_fee_per_gas > prev.max_fee_per_gas && next.max_priority_fee_per_gas >= prev.max_priority_fee_per_gas
    }

    /// Strings from `monad-eth-txpool-types` (`EthTxPoolDropReason::as_user_string`).
    fn classify_send_error(&self, err: &RpcError) -> SendErrorClass {
        let Some((code, text)) = message_of(err) else { return SendErrorClass::Indeterminate };
        if is_sync_timeout(code, &text) {
            return SendErrorClass::AcceptedPending;
        }
        if contains_any(&text, &["transaction nonce too low", "nonce too low"]) {
            return SendErrorClass::NonceTooLow;
        }
        if contains_any(&text, &["transaction fee too low", "fee too low"]) {
            return SendErrorClass::FeeTooLow;
        }
        if contains_any(&text, &["insufficient balance", "insufficient funds"]) {
            return SendErrorClass::InsufficientFunds;
        }
        // Our own bytes (or a better transaction of ours) already hold this nonce.
        if contains_any(&text, &["existing transaction had higher priority", "already known"]) {
            return SendErrorClass::AlreadyKnown;
        }
        if contains_any(&text, &["transaction pool is full", "transaction pool is not ready", "pool is full", "pool is not ready"]) {
            return SendErrorClass::PoolBusy;
        }
        if contains_any(
            &text,
            &["gas limit too low", "exceeds transaction gas limit", "invalid chain id", "max priority fee too high", "unsupported transaction type", "transaction decoding error"],
        ) {
            return SendErrorClass::Deterministic;
        }
        // Includes "a newer transaction had higher priority", "transaction expired",
        // "rpc no longer tracking tx": all leave the outcome open.
        SendErrorClass::Indeterminate
    }

    /// Diagnose before spending: a stuck transaction here is almost always a nonce gap or a funding
    /// problem, which no bump can fix — and every bump is paid for in full.
    fn stuck_ladder(&self) -> &'static [StuckStep] {
        &[StuckStep::Diagnose, StuckStep::Rebroadcast, StuckStep::Bump, StuckStep::Noop]
    }

    fn confirm_check(&self) -> ConfirmCheck {
        ConfirmCheck::FinalizedTag
    }

    fn transfer_gas(&self) -> GasPlan {
        GasPlan::Fixed(21_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mon(n: u64) -> U256 {
        U256::from(n) * U256::from(1_000_000_000_000_000_000u128)
    }

    #[test]
    fn small_accounts_can_pay_gas_but_cannot_move_value() {
        let m = Monad;
        // 2.5 MON signer making a contract call: the whole balance is usable for gas.
        let balance = mon(5) / U256::from(2);
        assert_eq!(m.spendable(balance, U256::ZERO), balance);
        // The same signer cannot transfer value: that would dip into its reserve and revert.
        assert_eq!(m.spendable(balance, U256::from(1)), U256::ZERO);
    }

    #[test]
    fn value_comes_only_from_what_is_held_above_the_reserve() {
        let m = Monad;
        // A 50 MON treasury can hand out 40 MON; the last 10 stay put.
        assert_eq!(m.spendable(mon(50), mon(1)), mon(40));
        assert_eq!(m.spendable(mon(10), mon(1)), U256::ZERO);
    }
}
