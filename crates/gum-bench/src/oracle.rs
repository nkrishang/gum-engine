//! Ground truth. Talks to Anvil *directly* (never through the counting proxy) using alloy.
//!
//! Every bench job is attributable on-chain by construction:
//! * `hit(id)` / `burn(id, n)` — unique `id` in calldata and in the `Hit` log;
//! * `fail()` — the unique id is appended as trailing calldata (ignored by Solidity);
//! * value transfer — sent to a unique, never-before-used recipient address.

use std::collections::{BTreeMap, HashMap};

use alloy::consensus::{SignableTransaction, Transaction as _, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::network::{TransactionResponse as _, TxSignerSync};
use alloy::primitives::{Address, Bytes, TxKind, B256, U256};
use alloy::providers::{Provider, ProviderBuilder, RootProvider};
use alloy::rpc::types::{Filter, TransactionReceipt};
use alloy::signers::local::PrivateKeySigner;
use alloy::sol;
use alloy::sol_types::{SolCall, SolEvent};
use anyhow::{anyhow, bail, Context, Result};
use futures::stream::{self, StreamExt, TryStreamExt};
use serde::Serialize;

sol! {
    contract BenchTarget {
        event Hit(bytes32 indexed id, address indexed sender);
        function hit(bytes32 id) external;
        function burn(bytes32 id, uint256 iterations) external;
        function fail() external pure;
        function hits(bytes32 id) external view returns (uint256);
        function total() external view returns (uint256);
    }
}

/// Creation bytecode of `contracts/BenchTarget.sol` (see `scripts/regen-bytecode.sh`).
const BENCH_TARGET_HEX: &str = include_str!("../contracts/BenchTarget.hex");

pub fn creation_code() -> Result<Bytes> {
    let h = BENCH_TARGET_HEX.trim().trim_start_matches("0x");
    Ok(Bytes::from(
        hex::decode(h).context("contracts/BenchTarget.hex is not valid hex")?,
    ))
}

pub fn hit_calldata(id: B256) -> Bytes {
    BenchTarget::hitCall { id }.abi_encode().into()
}

pub fn burn_calldata(id: B256, iterations: u64) -> Bytes {
    BenchTarget::burnCall {
        id,
        iterations: U256::from(iterations),
    }
    .abi_encode()
    .into()
}

/// `fail()` selector followed by the job's id, making each reverting transaction unique on-chain.
pub fn fail_calldata(id: B256) -> Bytes {
    let mut v = BenchTarget::failCall {}.abi_encode();
    v.extend_from_slice(id.as_slice());
    v.into()
}

/// Unique recipient for a value-transfer job.
pub fn transfer_recipient(id: B256) -> Address {
    Address::from_slice(&id.as_slice()[..20])
}

pub type DirectProvider = RootProvider;

pub fn provider(url: &str) -> Result<DirectProvider> {
    Ok(ProviderBuilder::new()
        .disable_recommended_fillers()
        .connect_http(url.parse().with_context(|| format!("bad RPC url {url}"))?))
}

/// Deploy `BenchTarget` from `deployer` and return its address.
pub async fn deploy(
    p: &DirectProvider,
    chain_id: u64,
    deployer: &PrivateKeySigner,
) -> Result<Address> {
    let nonce = p
        .get_transaction_count(deployer.address())
        .await
        .context("deployer nonce")?;
    let mut tx = TxEip1559 {
        chain_id,
        nonce,
        gas_limit: 1_000_000,
        max_fee_per_gas: 20_000_000_000,
        max_priority_fee_per_gas: 1_000_000_000,
        to: TxKind::Create,
        value: U256::ZERO,
        access_list: Default::default(),
        input: creation_code()?,
    };
    let sig = deployer
        .sign_transaction_sync(&mut tx)
        .context("signing the deploy tx")?;
    let env: TxEnvelope = tx.into_signed(sig).into();
    let receipt: TransactionReceipt = p
        .raw_request(
            "eth_sendRawTransactionSync".into(),
            (Bytes::from(env.encoded_2718()),),
        )
        .await
        .context("deploying BenchTarget (needs an Anvil that supports eth_sendRawTransactionSync; verified with 1.5.1)")?;
    if !receipt.status() {
        bail!("BenchTarget deployment reverted on chain {chain_id}");
    }
    let addr = receipt
        .contract_address
        .ok_or_else(|| anyhow!("deploy receipt has no contract address"))?;
    let code = p.get_code_at(addr).await?;
    if code.is_empty() {
        bail!("BenchTarget has no code at {addr} on chain {chain_id}");
    }
    Ok(addr)
}

pub async fn set_balance(p: &DirectProvider, who: Address, wei: U256) -> Result<()> {
    let _: serde_json::Value = p
        .raw_request("anvil_setBalance".into(), (who, wei))
        .await
        .with_context(|| format!("anvil_setBalance({who})"))?;
    Ok(())
}

#[derive(Clone, Debug, Serialize)]
pub struct ChainTx {
    pub block: u64,
    pub hash: B256,
    pub from: Address,
    pub to: Option<Address>,
    pub nonce: u64,
    #[serde(skip)]
    pub input: Bytes,
    pub value: U256,
    pub success: bool,
    pub gas_used: u64,
}

/// What a transaction is, as far as the bench can tell from the chain alone.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum TxKey {
    /// hit / burn / fail carrying this id.
    Id(B256),
    /// Plain transfer to this recipient.
    Recipient(Address),
    Other,
}

impl ChainTx {
    pub fn key(&self, target: Address) -> TxKey {
        if self.to == Some(target) && self.input.len() >= 4 {
            let sel: [u8; 4] = self.input[..4].try_into().expect("checked length");
            let id_at = |off: usize| {
                (self.input.len() >= off + 32).then(|| B256::from_slice(&self.input[off..off + 32]))
            };
            if sel == BenchTarget::hitCall::SELECTOR
                || sel == BenchTarget::burnCall::SELECTOR
                || sel == BenchTarget::failCall::SELECTOR
            {
                if let Some(id) = id_at(4) {
                    return TxKey::Id(id);
                }
            }
            return TxKey::Other;
        }
        match self.to {
            Some(to) if self.input.is_empty() => TxKey::Recipient(to),
            _ => TxKey::Other,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct ChainFacts {
    pub chain_id: u64,
    pub target: Address,
    pub start_block: u64,
    pub end_block: u64,
    pub txs: Vec<ChainTx>,
    /// `Hit` log occurrences per id.
    pub hit_logs: HashMap<B256, u64>,
    pub nonce_latest: BTreeMap<Address, u64>,
    pub nonce_pending: BTreeMap<Address, u64>,
    pub balances: BTreeMap<Address, U256>,
    pub contract_total: u64,
}

pub async fn block_number(p: &DirectProvider) -> Result<u64> {
    p.get_block_number()
        .await
        .context("eth_blockNumber (direct to Anvil)")
}

/// Read everything the verdict needs from one chain. `accounts` = treasury + signers.
pub async fn collect(
    p: &DirectProvider,
    chain_id: u64,
    target: Address,
    start_block: u64,
    accounts: &[Address],
) -> Result<ChainFacts> {
    let end_block = block_number(p).await?;
    let mut facts = ChainFacts {
        chain_id,
        target,
        start_block,
        end_block,
        ..Default::default()
    };

    // Full transaction + receipt scan of the run's block range.
    let blocks: Vec<Vec<ChainTx>> = stream::iter(start_block..=end_block)
        .map(|n| async move {
            let block = p
                .get_block_by_number(n.into())
                .full()
                .await
                .with_context(|| format!("eth_getBlockByNumber({n})"))?
                .ok_or_else(|| {
                    anyhow!("chain {chain_id}: block {n} missing (did Anvil lose history?)")
                })?;
            let txs: Vec<_> = block.transactions.into_transactions().collect();
            if txs.is_empty() {
                return Ok::<_, anyhow::Error>(Vec::new());
            }
            let receipts = p
                .get_block_receipts(n.into())
                .await
                .with_context(|| format!("eth_getBlockReceipts({n})"))?
                .ok_or_else(|| anyhow!("chain {chain_id}: receipts for block {n} missing"))?;
            let by_hash: HashMap<B256, &TransactionReceipt> =
                receipts.iter().map(|r| (r.transaction_hash, r)).collect();
            txs.iter()
                .map(|tx| {
                    let hash = tx.tx_hash();
                    let r = by_hash
                        .get(&hash)
                        .ok_or_else(|| anyhow!("no receipt for tx {hash} in block {n}"))?;
                    Ok(ChainTx {
                        block: n,
                        hash,
                        from: tx.from(),
                        to: tx.to(),
                        nonce: tx.nonce(),
                        input: tx.input().clone(),
                        value: tx.value(),
                        success: r.status(),
                        gas_used: r.gas_used,
                    })
                })
                .collect()
        })
        .buffered(16)
        .try_collect()
        .await?;
    facts.txs = blocks.into_iter().flatten().collect();

    // Hit logs, chunked so a long soak does not produce one enormous response.
    let mut from = start_block;
    while from <= end_block {
        let to = (from + 1999).min(end_block);
        let filter = Filter::new()
            .address(target)
            .event_signature(BenchTarget::Hit::SIGNATURE_HASH)
            .from_block(from)
            .to_block(to);
        let logs = p
            .get_logs(&filter)
            .await
            .with_context(|| format!("eth_getLogs({from}..={to})"))?;
        for log in logs {
            let id = *log
                .topics()
                .get(1)
                .ok_or_else(|| anyhow!("Hit log without an id topic"))?;
            *facts.hit_logs.entry(id).or_insert(0) += 1;
        }
        from = to + 1;
    }

    for a in accounts {
        facts
            .nonce_latest
            .insert(*a, p.get_transaction_count(*a).await?);
        facts
            .nonce_pending
            .insert(*a, p.get_transaction_count(*a).pending().await?);
        facts.balances.insert(*a, p.get_balance(*a).await?);
    }
    let total = p
        .call(
            alloy::rpc::types::TransactionRequest::default()
                .to(target)
                .input(Bytes::from(BenchTarget::totalCall {}.abi_encode()).into()),
        )
        .await
        .context("BenchTarget.total()")?;
    facts.contract_total = U256::from_be_slice(&total).saturating_to::<u64>();
    Ok(facts)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bytecode_is_embedded_and_calldata_is_attributable() {
        assert!(creation_code().unwrap().len() > 200);
        let id = B256::repeat_byte(7);
        let target = Address::repeat_byte(1);
        let mk = |to, input: Bytes| ChainTx {
            block: 1,
            hash: B256::ZERO,
            from: Address::ZERO,
            to: Some(to),
            nonce: 0,
            input,
            value: U256::ZERO,
            success: true,
            gas_used: 0,
        };
        assert_eq!(mk(target, hit_calldata(id)).key(target), TxKey::Id(id));
        assert_eq!(mk(target, burn_calldata(id, 5)).key(target), TxKey::Id(id));
        assert_eq!(mk(target, fail_calldata(id)).key(target), TxKey::Id(id));
        let rcpt = transfer_recipient(id);
        assert_eq!(mk(rcpt, Bytes::new()).key(target), TxKey::Recipient(rcpt));
    }
}
