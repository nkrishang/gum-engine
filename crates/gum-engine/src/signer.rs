//! The signer pool. AWS KMS in production, local private keys for development and tests — both behind
//! alloy's `TxSigner`, so nothing downstream knows which one it is talking to.
//!
//! Signing is the only thing a signer does here. Nonces, fees and gas are decided by the pipeline, and a
//! signature is requested exactly once per persisted attempt: KMS ECDSA signatures are non-deterministic,
//! so signing the same transaction twice yields two different transaction hashes.

use std::{collections::HashMap, str::FromStr, sync::Arc, time::Duration};

use alloy::{
    consensus::{SignableTransaction, TxEip1559, TxEnvelope},
    eips::eip2718::Encodable2718,
    network::TxSigner,
    primitives::{Address, Bytes, Signature, TxKind, B256, U256},
    signers::{aws::AwsSigner, local::PrivateKeySigner},
};

use crate::{
    config::{SignerMode, SignersConfig},
    domain::addr_hex,
    error::SignerError,
};

type DynSigner = Arc<dyn TxSigner<Signature> + Send + Sync>;

#[derive(Debug, Clone)]
pub struct SignedTx {
    pub hash: B256,
    pub raw: Bytes,
}

/// Everything needed to build an EIP-1559 transaction; assembled by the pipeline, never by the signer.
#[derive(Debug, Clone)]
pub struct TxFields {
    pub chain_id: u64,
    pub nonce: u64,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
}

pub struct SignerHandle {
    pub address: Address,
    /// KMS key id, or `local`.
    pub key_ref: String,
    inner: DynSigner,
    timeout: Duration,
}

impl SignerHandle {
    pub async fn sign(&self, f: &TxFields) -> Result<SignedTx, SignerError> {
        let mut tx = TxEip1559 {
            chain_id: f.chain_id,
            nonce: f.nonce,
            gas_limit: f.gas_limit,
            max_fee_per_gas: f.max_fee_per_gas,
            max_priority_fee_per_gas: f.max_priority_fee_per_gas,
            to: TxKind::Call(f.to),
            value: f.value,
            access_list: Default::default(),
            input: f.data.clone(),
        };
        let started = std::time::Instant::now();
        let signature =
            tokio::time::timeout(self.timeout, self.inner.sign_transaction(&mut tx)).await.map_err(|_| SignerError::Timeout(self.timeout.as_millis() as u64))?.map_err(|e| {
                let text = e.to_string();
                if text.contains("Throttling") {
                    metrics::counter!("gum_kms_throttled_total").increment(1);
                }
                SignerError::Sign(text)
            })?;
        metrics::histogram!("gum_sign_latency_seconds").record(started.elapsed().as_secs_f64());
        let envelope = TxEnvelope::Eip1559(tx.into_signed(signature));
        Ok(SignedTx { hash: *envelope.tx_hash(), raw: envelope.encoded_2718().into() })
    }
}

/// Encoded size of a signed EIP-1559 transaction with the given calldata, for cost estimation before
/// signing: type byte + RLP list of ~9 short fields + 65-byte signature, plus the calldata.
pub fn estimate_encoded_len(data_len: usize) -> usize {
    120 + data_len
}

pub struct SignerPool {
    signers: Vec<Arc<SignerHandle>>,
    by_address: HashMap<Address, Arc<SignerHandle>>,
}

impl SignerPool {
    /// Loads the job signers. For KMS this performs one `GetPublicKey` per key (concurrently) and nothing
    /// else — steady state is exactly one `Sign` call per transaction.
    pub async fn load(cfg: &SignersConfig) -> Result<(Self, SignerFactory), SignerError> {
        let factory = SignerFactory::new(cfg).await?;
        let refs: Vec<String> = match cfg.mode {
            SignerMode::Local => cfg.local_private_keys.clone(),
            SignerMode::Kms => cfg.kms_key_ids.clone(),
        };
        let handles = futures::future::try_join_all(refs.iter().map(|r| factory.build(r))).await?;
        let mut pool = Self { signers: Vec::new(), by_address: HashMap::new() };
        for handle in handles {
            let handle = Arc::new(handle);
            if pool.by_address.insert(handle.address, handle.clone()).is_some() {
                return Err(SignerError::Init(format!("signer {} is configured more than once", addr_hex(&handle.address))));
            }
            pool.signers.push(handle);
        }
        Ok((pool, factory))
    }

    pub fn all(&self) -> &[Arc<SignerHandle>] {
        &self.signers
    }

    pub fn get(&self, address: &Address) -> Option<Arc<SignerHandle>> {
        self.by_address.get(address).cloned()
    }

    pub fn len(&self) -> usize {
        self.signers.len()
    }

    pub fn is_empty(&self) -> bool {
        self.signers.is_empty()
    }
}

/// Builds signer handles from key references; shared by the pool and the per-chain treasuries.
pub struct SignerFactory {
    mode: SignerMode,
    kms: Option<aws_sdk_kms::Client>,
    timeout: Duration,
}

impl SignerFactory {
    async fn new(cfg: &SignersConfig) -> Result<Self, SignerError> {
        let kms = match cfg.mode {
            SignerMode::Local => None,
            SignerMode::Kms => {
                let aws = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
                Some(aws_sdk_kms::Client::new(&aws))
            }
        };
        Ok(Self { mode: cfg.mode, kms, timeout: Duration::from_millis(cfg.kms_timeout_ms) })
    }

    /// `key_ref` is a private key (local mode) or a KMS key id / ARN (kms mode). Aliases are refused:
    /// retargeting an alias would silently change the address while the cached public key stays stale.
    pub async fn build(&self, key_ref: &str) -> Result<SignerHandle, SignerError> {
        match self.mode {
            SignerMode::Local => {
                let signer = PrivateKeySigner::from_str(key_ref.trim()).map_err(|e| SignerError::Init(format!("invalid local private key: {e}")))?;
                let address = signer.address();
                Ok(SignerHandle { address, key_ref: "local".to_string(), inner: Arc::new(signer), timeout: self.timeout })
            }
            SignerMode::Kms => {
                if key_ref.starts_with("alias/") || key_ref.contains(":alias/") {
                    return Err(SignerError::Init(format!("KMS key `{key_ref}` is an alias; use the key id or key ARN")));
                }
                let client = self.kms.clone().ok_or_else(|| SignerError::Init("KMS client is not initialised".into()))?;
                let signer = AwsSigner::new(client, key_ref.to_string(), None).await.map_err(|e| SignerError::Init(format!("KMS key `{key_ref}`: {e}")))?;
                let address = TxSigner::address(&signer);
                Ok(SignerHandle { address, key_ref: key_ref.to_string(), inner: Arc::new(signer), timeout: self.timeout })
            }
        }
    }
}
