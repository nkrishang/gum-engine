//! Wire types for the handful of JSON-RPC results the engine reads.
//!
//! Deliberately tolerant: only the fields every EVM node returns are required, and chain-specific
//! extras (`l1Fee`, `gasUsedForL1`, …) stay available through `other` for the chain adapters.

use std::collections::BTreeMap;

use alloy::primitives::{B256, U256, U64};
use serde::{Deserialize, Serialize};

/// Serialises back to the node's own shape: typed fields are re-encoded as hex quantities and every
/// other field (`logs`, `contractAddress`, `l1Fee`, …) is carried through untouched in `other`.
#[derive(Debug, Clone, Deserialize, Serialize)]
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Callers get the receipt we store, so nothing the node sent may be lost on the way through:
    /// not the logs, not fields this engine has never heard of.
    #[test]
    fn receipt_round_trips_with_every_field() {
        let node = serde_json::json!({
            "transactionHash": "0x3300000000000000000000000000000000000000000000000000000000000000",
            "transactionIndex": "0x2",
            "blockNumber": "0x2d97a0e",
            "blockHash": "0x4400000000000000000000000000000000000000000000000000000000000000",
            "from": "0x6aeacf052d05b11a6c96cb3b39d43a982bca36c1",
            "to": "0x000000000000000000000000000000000000dead",
            "contractAddress": null,
            "status": "0x1",
            "type": "0x2",
            "gasUsed": "0xb0c4",
            "cumulativeGasUsed": "0x1b0c4",
            "effectiveGasPrice": "0x4c4b40",
            "logsBloom": "0x00",
            "logs": [{
                "address": "0x000000000000000000000000000000000000dead",
                "topics": ["0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"],
                "data": "0x01",
                "logIndex": "0x0",
                "removed": false
            }],
            "l1Fee": "0x2540be400",
            "l1GasUsed": "0x640",
            "someFutureField": {"nested": [1, 2, 3]}
        });
        let receipt: Receipt = serde_json::from_value(node.clone()).expect("receipt parses");
        assert!(receipt.succeeded());
        assert_eq!(receipt.extra_u256("l1Fee"), Some(U256::from(0x2540be400u64)));
        let back = serde_json::to_value(&receipt).expect("receipt serialises");
        assert_eq!(back, node, "the receipt handed to callers must equal the receipt the node returned");
    }
}
