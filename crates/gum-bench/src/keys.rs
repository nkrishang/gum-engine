//! Anvil dev accounts, derived from the standard test mnemonic.
//! Account 0 = treasury, 1..=N = engine signers, N+1 = harness deployer (never given to the engine).

use alloy::primitives::Address;
use alloy::signers::local::{coins_bip39::English, MnemonicBuilder, PrivateKeySigner};
use anyhow::{Context, Result};

pub const TEST_MNEMONIC: &str = "test test test test test test test test test test test junk";

pub fn dev_signer(index: u32) -> Result<PrivateKeySigner> {
    MnemonicBuilder::<English>::default()
        .phrase(TEST_MNEMONIC)
        .index(index)
        .with_context(|| format!("derivation index {index}"))?
        .build()
        .with_context(|| format!("deriving dev account {index}"))
}

pub fn private_key_hex(s: &PrivateKeySigner) -> String {
    format!("0x{}", hex::encode(s.to_bytes()))
}

#[derive(Clone)]
pub struct DevAccounts {
    pub treasury: PrivateKeySigner,
    pub signers: Vec<PrivateKeySigner>,
    pub deployer: PrivateKeySigner,
}

impl DevAccounts {
    pub fn derive(n_signers: usize) -> Result<Self> {
        let treasury = dev_signer(0)?;
        let signers = (1..=n_signers as u32)
            .map(dev_signer)
            .collect::<Result<Vec<_>>>()?;
        let deployer = dev_signer(n_signers as u32 + 1)?;
        Ok(Self {
            treasury,
            signers,
            deployer,
        })
    }
    pub fn signer_addresses(&self) -> Vec<Address> {
        self.signers.iter().map(|s| s.address()).collect()
    }
    /// Number of accounts Anvil must be started with.
    pub fn anvil_accounts(&self) -> usize {
        self.signers.len() + 2
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_anvil_well_known_accounts() {
        let a0 = dev_signer(0).unwrap();
        assert_eq!(
            format!("{:?}", a0.address()),
            "0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
        );
        assert_eq!(
            private_key_hex(&a0),
            "0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80"
        );
        let a1 = dev_signer(1).unwrap();
        assert_eq!(
            format!("{:?}", a1.address()),
            "0x70997970c51812dc3a010c7d01b50e0d17dc79c8"
        );
    }
}
