//! Core domain types shared by every layer. Nothing in here knows about a specific chain.

use std::{fmt, str::FromStr};

use alloy::primitives::{Address, Bytes, B256, U256};
use chrono::{DateTime, Utc};
use serde::Serialize;
use uuid::Uuid;

pub type JobId = Uuid;
pub type ChainId = u64;

/// Lowercase 0x-hex, the canonical textual form used in the database, logs and API.
pub fn addr_hex(a: &Address) -> String {
    format!("{a:#x}")
}

pub fn hash_hex(h: &B256) -> String {
    format!("{h:#x}")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PairKey {
    pub chain_id: ChainId,
    pub signer: Address,
}

impl fmt::Display for PairKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}@{}", addr_hex(&self.signer), self.chain_id)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Sent,
    Included,
    Confirmed,
    Cancelling,
    Failed,
}

impl JobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Sent => "sent",
            Self::Included => "included",
            Self::Confirmed => "confirmed",
            Self::Cancelling => "cancelling",
            Self::Failed => "failed",
        }
    }

    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Confirmed | Self::Failed)
    }
}

impl FromStr for JobStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "queued" => Self::Queued,
            "sent" => Self::Sent,
            "included" => Self::Included,
            "confirmed" => Self::Confirmed,
            "cancelling" => Self::Cancelling,
            "failed" => Self::Failed,
            other => return Err(format!("unknown job status `{other}`")),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Success,
    Reverted,
}

impl Outcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Success => "success",
            Self::Reverted => "reverted",
        }
    }

    pub fn from_receipt_status(ok: bool) -> Self {
        if ok {
            Self::Success
        } else {
            Self::Reverted
        }
    }
}

impl FromStr for Outcome {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "success" => Ok(Self::Success),
            "reverted" => Ok(Self::Reverted),
            other => Err(format!("unknown outcome `{other}`")),
        }
    }
}

/// The caller's request, immutable for the life of the job.
#[derive(Debug, Clone)]
pub struct JobRequest {
    pub chain_id: ChainId,
    pub to: Address,
    pub data: Bytes,
    pub value: U256,
    /// Present => used verbatim, never simulated. Absent => estimated by the engine.
    pub gas_limit: Option<u64>,
    pub deadline: Option<DateTime<Utc>>,
    pub webhook_url: String,
}

/// A job as held in the in-memory queue: the request plus the few mutable fields the pipeline needs.
#[derive(Debug, Clone)]
pub struct QueuedJob {
    pub id: JobId,
    pub request: JobRequest,
    pub requeue_count: u32,
    pub created_at: DateTime<Utc>,
    /// Monotonic instant of acceptance (or of load at boot), for queue-latency measurement.
    pub enqueued_at: std::time::Instant,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptPurpose {
    Job,
    Topup,
    Cancel,
}

impl AttemptPurpose {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Job => "job",
            Self::Topup => "topup",
            Self::Cancel => "cancel",
        }
    }
}

impl FromStr for AttemptPurpose {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "job" => Ok(Self::Job),
            "topup" => Ok(Self::Topup),
            "cancel" => Ok(Self::Cancel),
            other => Err(format!("unknown attempt purpose `{other}`")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptStatus {
    /// Persisted and (possibly) broadcast; no receipt yet.
    Broadcast,
    Included,
    Confirmed,
    /// Another attempt at the same nonce was mined instead.
    Replaced,
    /// Vanished from the chain after having been included (re-org) and superseded.
    Dropped,
}

impl AttemptStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Broadcast => "broadcast",
            Self::Included => "included",
            Self::Confirmed => "confirmed",
            Self::Replaced => "replaced",
            Self::Dropped => "dropped",
        }
    }
}

impl FromStr for AttemptStatus {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Ok(match s {
            "broadcast" => Self::Broadcast,
            "included" => Self::Included,
            "confirmed" => Self::Confirmed,
            "replaced" => Self::Replaced,
            "dropped" => Self::Dropped,
            other => return Err(format!("unknown attempt status `{other}`")),
        })
    }
}

/// What a nonce was bound to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotOwner {
    Job(JobId),
    Topup(Uuid),
}

impl SlotOwner {
    pub fn owner_kind(&self) -> &'static str {
        match self {
            Self::Job(_) => "job",
            Self::Topup(_) => "topup",
        }
    }

    pub fn id(&self) -> Uuid {
        match self {
            Self::Job(id) | Self::Topup(id) => *id,
        }
    }
}

/// A signed transaction. Persisted before its first broadcast; the raw bytes are the only thing ever
/// rebroadcast (KMS signatures are non-deterministic, so re-signing would create a different transaction).
#[derive(Debug, Clone)]
pub struct Attempt {
    pub id: Uuid,
    pub chain_id: ChainId,
    pub signer: Address,
    pub nonce: u64,
    pub purpose: AttemptPurpose,
    pub owner: SlotOwner,
    pub tx_hash: B256,
    pub raw_tx: Bytes,
    pub gas_limit: u64,
    pub max_fee_per_gas: u128,
    pub max_priority_fee_per_gas: u128,
    pub value: U256,
    pub status: AttemptStatus,
    pub block_number: Option<u64>,
    pub block_hash: Option<B256>,
    pub created_at: DateTime<Utc>,
}

/// The facts of an inclusion, extracted from a receipt by the chain adapter.
#[derive(Debug, Clone)]
pub struct Inclusion {
    pub tx_hash: B256,
    pub block_number: u64,
    pub block_hash: B256,
    pub outcome: Outcome,
    pub gas_used: u64,
    pub effective_gas_price: u128,
    /// Total native cost to the sender as defined by the chain (may exceed gas_used * price).
    pub fee_paid: U256,
    pub l1_fee: Option<U256>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PairRole {
    Signer,
    Treasury,
}

impl PairRole {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Signer => "signer",
            Self::Treasury => "treasury",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PauseReason {
    Booting,
    ChainOutage,
    Reorg,
    NonceDrift,
    InsufficientFunds,
    StuckUnresolved,
    SignerUnavailable,
    Manual,
}

impl PauseReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Booting => "booting",
            Self::ChainOutage => "chain_outage",
            Self::Reorg => "reorg",
            Self::NonceDrift => "nonce_drift",
            Self::InsufficientFunds => "insufficient_funds",
            Self::StuckUnresolved => "stuck_unresolved",
            Self::SignerUnavailable => "signer_unavailable",
            Self::Manual => "manual",
        }
    }
}

/// What the engine is doing to get a paused pair back, shown verbatim on `/v1/signers`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryStep {
    AwaitingChainHead,
    ReconcilingNonce,
    Rebroadcasting,
    AwaitingTreasuryTopup,
    AwaitingTreasuryRefill,
    AwaitingOperatorResume,
    NeedsOperator,
}

impl RecoveryStep {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AwaitingChainHead => "awaiting_chain_head",
            Self::ReconcilingNonce => "reconciling_nonce",
            Self::Rebroadcasting => "rebroadcasting",
            Self::AwaitingTreasuryTopup => "awaiting_treasury_topup",
            Self::AwaitingTreasuryRefill => "awaiting_treasury_refill",
            Self::AwaitingOperatorResume => "awaiting_operator_resume",
            Self::NeedsOperator => "needs_operator",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Pause {
    pub reason: PauseReason,
    pub recovery_step: RecoveryStep,
    pub since: DateTime<Utc>,
    pub detail: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WebhookEvent {
    #[serde(rename = "transaction.included")]
    Included,
    #[serde(rename = "transaction.confirmed")]
    Confirmed,
    #[serde(rename = "transaction.failed")]
    Failed,
}

impl WebhookEvent {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Included => "transaction.included",
            Self::Confirmed => "transaction.confirmed",
            Self::Failed => "transaction.failed",
        }
    }
}
