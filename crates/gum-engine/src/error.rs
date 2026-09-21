//! Error taxonomy. Every error that can reach a log line or an API response has a stable `code()`, so
//! logs can be filtered on `@code:` and dashboards never depend on message text.

use thiserror::Error;

/// Errors from the persistence layer.
#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// A lease-gated write was refused: this process is no longer (or not yet) the leader.
    #[error("lease epoch {mine} is stale; this instance is not the leader")]
    Fenced { mine: i64 },
    /// The compare-and-set on a nonce slot lost: the slot already belongs to another owner.
    #[error("nonce {nonce} of {signer} on chain {chain_id} is already bound to another owner")]
    NonceSlotTaken { chain_id: u64, signer: String, nonce: u64 },
    /// A conditional status transition matched no row: the row was not in the expected state.
    #[error("{entity} {id} was not in the expected state `{expected}`")]
    StateConflict { entity: &'static str, id: String, expected: &'static str },
    #[error("stored value is corrupt: {0}")]
    Corrupt(String),
}

impl StoreError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Db(_) => "store.db",
            Self::Fenced { .. } => "store.fenced",
            Self::NonceSlotTaken { .. } => "store.nonce_slot_taken",
            Self::StateConflict { .. } => "store.state_conflict",
            Self::Corrupt(_) => "store.corrupt",
        }
    }

    /// Transient failures are retried; everything else indicates a logic or data problem.
    pub fn is_transient(&self) -> bool {
        match self {
            Self::Db(e) => {
                matches!(e, sqlx::Error::Io(_) | sqlx::Error::PoolTimedOut | sqlx::Error::PoolClosed | sqlx::Error::WorkerCrashed | sqlx::Error::Tls(_))
                    || e.as_database_error().is_some_and(|d| {
                        // serialization_failure, deadlock_detected, admin shutdown / crash / cannot_connect_now
                        matches!(d.code().as_deref(), Some("40001" | "40P01" | "57P01" | "57P02" | "57P03"))
                    })
            }
            _ => false,
        }
    }
}

/// Errors from a JSON-RPC call, after transport-level retries have been exhausted.
#[derive(Debug, Clone, Error)]
pub enum RpcError {
    /// The node answered with a JSON-RPC error object.
    #[error("rpc error {code}: {message}")]
    Response { code: i64, message: String, data: Option<String> },
    /// The provider throttled us (HTTP 429 / -32007 / -32008 / -32011).
    #[error("rpc rate limited: {0}")]
    RateLimited(String),
    /// The request may or may not have reached the node: timeout, reset, 5xx, undecodable body.
    #[error("rpc transport failure: {0}")]
    Transport(String),
    /// The chain's circuit breaker is open; the call was not attempted.
    #[error("rpc circuit open for chain {0}")]
    CircuitOpen(u64),
    #[error("rpc response could not be decoded: {0}")]
    Decode(String),
}

impl RpcError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Response { .. } => "rpc.response",
            Self::RateLimited(_) => "rpc.rate_limited",
            Self::Transport(_) => "rpc.transport",
            Self::CircuitOpen(_) => "rpc.circuit_open",
            Self::Decode(_) => "rpc.decode",
        }
    }

    /// True when the node definitely did not act on the request.
    pub fn is_definitely_not_delivered(&self) -> bool {
        matches!(self, Self::CircuitOpen(_) | Self::RateLimited(_))
    }

    pub fn is_method_not_found(&self) -> bool {
        matches!(self, Self::Response { code: -32601, .. })
    }
}

#[derive(Debug, Error)]
pub enum SignerError {
    #[error("signer {0} is not loaded")]
    Unknown(String),
    #[error("signing failed: {0}")]
    Sign(String),
    #[error("signing timed out after {0} ms")]
    Timeout(u64),
    #[error("signer initialisation failed: {0}")]
    Init(String),
}

impl SignerError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::Unknown(_) => "signer.unknown",
            Self::Sign(_) => "signer.sign",
            Self::Timeout(_) => "signer.timeout",
            Self::Init(_) => "signer.init",
        }
    }
}

/// Terminal failure reasons for a job; exposed verbatim in the API and in `transaction.failed` webhooks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JobFailure {
    SimulationReverted,
    InvalidTx,
    Expired,
    StuckCancelled,
    /// Cancelled by an operator while still queued.
    Cancelled,
    Internal,
}

impl JobFailure {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SimulationReverted => "simulation_reverted",
            Self::InvalidTx => "invalid_tx",
            Self::Expired => "expired",
            Self::StuckCancelled => "stuck_cancelled",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        }
    }
}
