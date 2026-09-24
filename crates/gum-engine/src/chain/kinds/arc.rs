//! Arc (Circle's L1): Reth execution under Malachite (Tendermint-style) BFT consensus, with USDC as the
//! native gas token.
//!
//! - **Final on inclusion**: a block is committed by the validator set before anyone can read it, and
//!   `safe`/`finalized` equal `latest`. There are no re-orgs, so the receipt is the confirmation.
//! - **~500 ms blocks, produced continuously**: a head that stops moving is an outage.
//! - **Native USDC has 18 decimals** on the native side (`eth_getBalance`, `value`); the ERC-20 view at
//!   `0x3600…0000` has 6 decimals over the same balance. Everything here is in 18-decimal units.
//! - **Fees**: EIP-1559 with a smoothed base fee. The floor is 20 gwei, the base fee moves at most 2% per block,
//!   the base fee goes to the proposer instead of being burned, and ordering is by tip. The sender pays
//!   `gas_used * price`, as on any EIP-1559 chain. A transaction below the floor is *accepted* and then never
//!   mined, so quotes must track the observed base fee rather than a static price.
//! - **Per-transaction gas cap of 2^24** (EIP-7825), under a 30M block gas limit.
//! - **Reth pool**: geth-compatible error strings and the same +10% replacement rule. Arc adds compliance
//!   rejects (USDC blocklist, denylist) and an "invalid tx list" that a node fills with every pending
//!   transaction after a block-builder crash.
//! - **No `eth_sendRawTransactionSync`** on any public endpoint (the node supports it, the gateways
//!   filter it), so sends fall back to broadcast plus receipt polling.

use alloy::primitives::U256;

use super::geth::{classify_geth, gas_times_price, geth_accepts_replacement, geth_replacement};
use crate::{chain::adapter::*, config::ChainTunables, error::RpcError, rpc::types::Receipt};

/// Not `Arc`: that name belongs to `std::sync::Arc` everywhere else in this crate.
pub struct ArcNetwork;

impl ChainAdapter for ArcNetwork {
    fn kind(&self) -> &'static str {
        "arc"
    }

    fn defaults(&self) -> ChainTunables {
        ChainTunables {
            block_time_ms: 500,
            confirmation_delay_ms: 0,
            estimate_multiplier_bps: 12_000,
            max_fee_multiplier_bps: 20_000,
            // The node suggests 0.1 gwei; 1 gwei keeps us ahead in a tip-ordered pool for ~$0.00002 a transfer.
            priority_fee_wei: 1_000_000_000,
            max_fee_cap_wei: 1_000_000_000_000,    // 1000 gwei: 50x the 20 gwei floor
            cancel_fee_cap_wei: 1_300_000_000_000, // > 1.21x the job cap, so a cancel can always out-bid
            // The base fee can rise 2% per block. At that rate, 10s (~20 blocks) of staleness is still inside
            // the 2x headroom of max_fee.
            fee_ttl_ms: 10_000,
            stuck_after_blocks: 10, // ~5s
            max_bumps: 5,
            sync_send_timeout_ms: 5_000,
            receipt_poll_interval_ms: 400,
            idle_probe_interval_ms: 60_000,
            degraded_probe_interval_ms: 5_000,
            stall_warn_ms: 3_000,
            stall_outage_ms: 10_000,
            rpc_failure_threshold: 5,
            // Unverified: validate against the provider's usage report (see docs/adding-a-chain.md, step 6).
            credits_per_call: 20,
            max_tx_gas: 16_777_216, // EIP-7825 cap (2^24); `gas limit too high` above it
            balance_sweep_interval_ms: 300_000,
            topup_wait_timeout_ms: 60_000,
            liveness: LivenessMode::HeadAdvance,
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

    /// Reth's strings (geth-compatible) plus Arc's own, from `circlefin/arc-node` and live nodes.
    fn classify_send_error(&self, err: &RpcError) -> SendErrorClass {
        let Some((_, text)) = message_of(err) else { return SendErrorClass::Indeterminate };
        // After a block-builder crash, a node puts every pending transaction on an invalid list, and
        // resending those exact bytes is refused. A re-signed transaction at the same nonce is new bytes and
        // is accepted. A replacement is always nonce-safe, and FeeTooLow is the class that produces one.
        if contains_any(&text, &["transaction in invalid tx list"]) {
            return SendErrorClass::FeeTooLow;
        }
        // Compliance lists, checked at pool admission: the transaction never entered the pool. The job can
        // never run as long as its sender, or a recipient of value, stays listed.
        if contains_any(&text, &["blocked address", "is denylisted", "address is blocklisted"]) {
            return SendErrorClass::Deterministic;
        }
        // The RPC node could not forward the transaction to the network. The same bytes can go again later.
        if contains_any(&text, &["transaction relay upstreams are unreachable"]) {
            return SendErrorClass::PoolBusy;
        }
        classify_geth(err)
    }

    /// Since arc-node v0.8.0, a call whose `value` exceeds the sender's balance fails estimation with
    /// `revert: OutOfFunds` instead of geth's `insufficient funds`.
    fn estimate_lacks_funds(&self, err: &RpcError) -> bool {
        message_of(err).is_some_and(|(_, text)| contains_any(&text, &["insufficient funds", "outoffunds"]))
    }

    fn stuck_ladder(&self) -> &'static [StuckStep] {
        &[StuckStep::Rebroadcast, StuckStep::Diagnose, StuckStep::Bump, StuckStep::Noop]
    }

    /// Only used if a deployment sets a non-zero `confirmation_delay_ms`, as a belt-and-braces re-check.
    fn confirm_check(&self) -> ConfirmCheck {
        ConfirmCheck::BlockHash
    }

    /// A native transfer to a plain EOA costs exactly 21000 gas. Blocklist reads are not charged.
    fn transfer_gas(&self) -> GasPlan {
        GasPlan::Fixed(21_000)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn response(code: i64, message: &str) -> RpcError {
        RpcError::Response { code, message: message.into(), data: None }
    }

    /// Recorded from Arc mainnet: `eth_estimateGas` for a 1 USDC transfer from an account holding 0.05 USDC.
    #[test]
    fn value_beyond_balance_at_estimate_asks_for_funds() {
        assert!(ArcNetwork.estimate_lacks_funds(&response(-32003, "revert: OutOfFunds")));
        assert!(ArcNetwork.estimate_lacks_funds(&response(-32003, "insufficient funds for gas * price + value: have 0 want 1")));
    }

    /// A real revert must still fail the job before it costs anything.
    #[test]
    fn reverting_calls_are_not_mistaken_for_missing_funds() {
        assert!(!ArcNetwork.estimate_lacks_funds(&response(3, "execution reverted")));
        assert!(!ArcNetwork.estimate_lacks_funds(&response(3, "execution reverted: Zero address not allowed")));
        assert!(!ArcNetwork.estimate_lacks_funds(&response(-32603, "Blocked address")));
        assert!(!ArcNetwork.estimate_lacks_funds(&RpcError::Transport("connection reset by peer".into())));
    }
}
