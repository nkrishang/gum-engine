//! Balance monitor: reconciles ledgers with the chain, tops signers up before they run dry, watches the
//! treasury, and brings pairs back once funds return.
//!
//! One `eth_call` to Multicall3 reads every signer's balance plus the block number it was read at; where
//! Multicall3 is not deployed (local dev chains) it falls back to one call per address. Reads never
//! overwrite fresher local knowledge — see `Ledger::reconcile`.

use std::{sync::Arc, time::Duration};

use alloy::{
    primitives::{address, Address, Bytes, U256},
    sol,
    sol_types::SolCall,
};

use super::treasury;
use crate::{
    chain::{ChainCtx, ChainStatus},
    domain::{addr_hex, PairRole, PauseReason},
    engine::{Engine, Pair},
    error::RpcError,
    telemetry,
};

const MULTICALL3: Address = address!("cA11bde05977b3631167028862bE2a173976CA11");

sol! {
    struct Call3 { address target; bool allowFailure; bytes callData; }
    struct Call3Result { bool success; bytes returnData; }
    function aggregate3(Call3[] calldata calls) external payable returns (Call3Result[] memory returnData);
    function getEthBalance(address addr) external view returns (uint256 balance);
    function getBlockNumber() external view returns (uint256 blockNumber);
}

pub async fn run(engine: Arc<Engine>, chain: Arc<ChainCtx>) {
    let sweep_every = Duration::from_millis(chain.tunables.balance_sweep_interval_ms.max(5_000));
    let has_multicall = match chain.rpc.get_code(MULTICALL3, chain.rpc.background_opts()).await {
        Ok(code) => !code.is_empty(),
        Err(_) => false,
    };
    tracing::debug!(event = "balances.mode", chain = chain.chain_id, multicall3 = has_multicall, "balance sweep mode selected");

    let mut last_full_sweep = std::time::Instant::now();
    loop {
        // Short slices, so a pair that pauses for funds mid-interval is noticed within seconds rather
        // than at the next full sweep. A slice costs nothing unless someone is actually waiting.
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            _ = engine.shutdown.cancelled() => return,
        }
        let pairs = engine.pairs_on(chain.chain_id);
        let waiting_for_funds = pairs.iter().any(|p| p.pause_reason() == Some(PauseReason::InsufficientFunds));
        let full_sweep_due = last_full_sweep.elapsed() >= sweep_every;
        if !(waiting_for_funds || full_sweep_due) || chain.status() == ChainStatus::Down {
            continue;
        }
        if full_sweep_due {
            last_full_sweep = std::time::Instant::now();
        }
        let waiting_for_funds = waiting_for_funds && !full_sweep_due;

        let targets: Vec<Arc<Pair>> = if waiting_for_funds {
            // Cheap pass: the treasury (did the refill arrive?) and the pairs that are waiting on it.
            pairs.iter().filter(|p| p.role == PairRole::Treasury || p.pause_reason() == Some(PauseReason::InsufficientFunds)).cloned().collect()
        } else {
            pairs.clone()
        };
        if let Err(e) = reconcile(&chain, &targets, has_multicall).await {
            if let Some(suppressed) = telemetry::throttled(&format!("balances.sweep:{}", chain.chain_id), Duration::from_secs(60)) {
                tracing::warn!(event = "balances.sweep_failed", chain = chain.chain_id, error = %e, suppressed, "could not read balances");
            }
            continue;
        }

        for pair in &pairs {
            let available = treasury::spendable(pair);
            metrics::gauge!("gum_signer_balance_wei", "chain" => chain.name.clone(), "signer" => addr_hex(&pair.key.signer)).set(treasury::u256_to_f64(available));
            match pair.role {
                PairRole::Treasury => {
                    treasury::check_treasury_level(pair);
                    if pair.pause_reason() == Some(PauseReason::InsufficientFunds) && available >= chain.cfg.topup_amount {
                        pair.resume_if(&engine.store, PauseReason::InsufficientFunds);
                    }
                }
                PairRole::Signer => {
                    let paused_for_funds = pair.pause_reason() == Some(PauseReason::InsufficientFunds);
                    let low = pair.ledger.view().initialised && available < chain.cfg.signer_min_balance;
                    if !(low || paused_for_funds) {
                        continue;
                    }
                    // Funded again by someone else (or a late credit): nothing to wait for.
                    if paused_for_funds && !low && pair.view().current.is_none() {
                        pair.resume_if(&engine.store, PauseReason::InsufficientFunds);
                        continue;
                    }
                    let (engine, pair) = (engine.clone(), pair.clone());
                    tokio::spawn(async move {
                        if treasury::request_topup(&engine, &pair, U256::ZERO, U256::ZERO).await.is_ok() && pair.view().current.is_none() {
                            pair.resume_if(&engine.store, PauseReason::InsufficientFunds);
                        }
                    });
                }
            }
        }
    }
}

async fn reconcile(chain: &ChainCtx, pairs: &[Arc<Pair>], has_multicall: bool) -> Result<(), RpcError> {
    if pairs.is_empty() {
        return Ok(());
    }
    // Tickets are taken before the read so a settlement racing it invalidates the result.
    let tickets: Vec<_> = pairs.iter().map(|p| p.ledger.begin_reconcile()).collect();

    if has_multicall {
        let mut calls: Vec<Call3> =
            pairs.iter().map(|p| Call3 { target: MULTICALL3, allowFailure: false, callData: Bytes::from(getEthBalanceCall { addr: p.key.signer }.abi_encode()) }).collect();
        calls.push(Call3 { target: MULTICALL3, allowFailure: false, callData: Bytes::from(getBlockNumberCall {}.abi_encode()) });
        let data = Bytes::from(aggregate3Call { calls }.abi_encode());
        let raw = chain.rpc.eth_call(MULTICALL3, data, chain.rpc.background_opts()).await?;
        let results = aggregate3Call::abi_decode_returns(&raw).map_err(|e| RpcError::Decode(format!("multicall3: {e}")))?;
        if results.len() != pairs.len() + 1 {
            return Err(RpcError::Decode("multicall3 returned an unexpected number of results".into()));
        }
        let word = |b: &Bytes| U256::from_be_slice(&b[..b.len().min(32)]);
        let block = word(&results[pairs.len()].returnData).saturating_to::<u64>();
        chain.observe_head(block, None);
        for ((pair, ticket), result) in pairs.iter().zip(tickets).zip(results.iter()) {
            pair.ledger.reconcile(ticket, word(&result.returnData), block);
        }
    } else {
        for (pair, ticket) in pairs.iter().zip(tickets) {
            let balance = chain.rpc.get_balance(pair.key.signer, chain.rpc.background_opts()).await?;
            // No block number comes with a plain balance read; the last observed head is a safe lower bound.
            pair.ledger.reconcile(ticket, balance, chain.head());
        }
    }
    Ok(())
}
