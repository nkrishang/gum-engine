//! Wire types for the handful of JSON-RPC results the engine reads.
//!
//! Deliberately tolerant: only the fields every EVM node returns are required, and chain-specific
//! extras (`l1Fee`, `gasUsedForL1`, …) stay available through `other` for the chain adapters.

use std::collections::BTreeMap;

use alloy::primitives::{B256, U256, U64};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Receipt {
    pub transaction_hash: B256,
    pub block_number: Option<U64>,
    pub block_hash: Option<B256>,
    /// 1 = success, 0 = reverted. Pre-Byzantium receipts have none; every supported chain does.
    pub status: Option<U64>,
    pub gas_used: U256,
    pub effective_gas_price: Option<U256>,
    #[serde(flatten)]
    pub other: BTreeMap<String, serde_json::Value>,
}

impl Receipt {
    pub fn succeeded(&self) -> bool {
        self.status.is_some_and(|s| s == U64::from(1))
    }

    /// A hex-quantity extra field (e.g. `l1Fee`), if present and well-formed.
    pub fn extra_u256(&self, name: &str) -> Option<U256> {
        let raw = self.other.get(name)?.as_str()?;
        let hex = raw.strip_prefix("0x")?;
        U256::from_str_radix(hex, 16).ok()
    }

    pub fn gas_used_u64(&self) -> u64 {
        self.gas_used.saturating_to::<u64>()
    }

    pub fn effective_gas_price_u128(&self) -> u128 {
        self.effective_gas_price.map(|p| p.saturating_to::<u128>()).unwrap_or(0)
    }
}

/// A block fetched with `full = false`: header fields plus transaction hashes.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Block {
    pub number: U64,
    pub hash: B256,
    pub parent_hash: B256,
    pub timestamp: U64,
    pub base_fee_per_gas: Option<U256>,
    #[serde(default)]
    pub transactions: Vec<B256>,
}

impl Block {
    pub fn number_u64(&self) -> u64 {
        self.number.saturating_to::<u64>()
    }

    pub fn base_fee_u128(&self) -> Option<u128> {
        self.base_fee_per_gas.map(|f| f.saturating_to::<u128>())
    }
}
