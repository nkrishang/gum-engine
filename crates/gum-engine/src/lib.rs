//! gum-engine: a durable, concurrent EVM transaction processing service.
//!
//! Layout: `api` (HTTP) → `store` (Postgres, durable truth) + `queue` (in-memory window) → `pipeline`
//! (pair workers driving nonces) → `rpc` (rate-limited JSON-RPC) and `signer` (KMS / local keys).
//! Everything chain-specific sits behind `chain::adapter::ChainAdapter`.

pub mod api;
pub mod chain;
pub mod config;
pub mod domain;
pub mod engine;
pub mod error;
pub mod funds;
pub mod leader;
pub mod pipeline;
pub mod queue;
pub mod rpc;
pub mod signer;
pub mod stats;
pub mod store;
pub mod telemetry;
pub mod webhook;
