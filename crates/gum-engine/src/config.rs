//! Configuration: a TOML file (`$GUM_CONFIG`) overlaid with `GUM_*` environment variables.
//!
//! Chain-specific *behaviour* never lives here — only values. Each chain kind supplies defaults for
//! every tunable (`ChainAdapter::defaults`), and a `[chains.<name>]` block may override any of them.

use std::collections::BTreeMap;

use alloy::primitives::U256;
use figment::{
    providers::{Env, Format, Toml},
    Figment,
};
use serde::{Deserialize, Deserializer};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub server: ServerConfig,
    pub database: DatabaseConfig,
    pub webhook: WebhookConfig,
    #[serde(default)]
    pub rpc: RpcConfig,
    pub signers: SignersConfig,
    #[serde(default)]
    pub queue: QueueConfig,
    #[serde(default)]
    pub alerts: AlertsConfig,
    pub chains: BTreeMap<String, ChainConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    /// Largest accepted request body, in bytes.
    #[serde(default = "default_body_limit")]
    pub body_limit_bytes: usize,
    /// How long SIGTERM waits for in-flight sends to settle before exiting anyway.
    #[serde(default = "default_drain_ms")]
    pub drain_timeout_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self { port: default_port(), body_limit_bytes: default_body_limit(), drain_timeout_ms: default_drain_ms() }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DatabaseConfig {
    pub url: String,
    #[serde(default = "default_true")]
    pub auto_migrate: bool,
    /// Connections for the transaction pipeline (binds, settlements).
    #[serde(default = "default_pipeline_pool")]
    pub pipeline_pool_size: u32,
    /// Connections for API reads; kept separate so analytics can never block a bind.
    #[serde(default = "default_api_pool")]
    pub api_pool_size: u32,
    #[serde(default = "default_api_statement_timeout_ms")]
    pub api_statement_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WebhookConfig {
    pub signing_secret: String,
    /// Callers live on the private network, so private hosts are allowed by default. The engine's own
    /// address is always refused and redirects are never followed.
    #[serde(default = "default_true")]
    pub allow_private_hosts: bool,
    /// When non-empty, only these hosts may receive webhooks.
    #[serde(default)]
    pub host_allowlist: Vec<String>,
    #[serde(default = "default_webhook_concurrency")]
    pub max_concurrency: usize,
    #[serde(default = "default_webhook_per_host")]
    pub max_per_host: usize,
    #[serde(default = "default_webhook_timeout_ms")]
    pub timeout_ms: u64,
    #[serde(default = "default_webhook_attempts")]
    pub max_attempts: u32,
    #[serde(default = "default_webhook_backoff_base_ms")]
    pub backoff_base_ms: u64,
    #[serde(default = "default_webhook_backoff_max_ms")]
    pub backoff_max_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RpcConfig {
    /// Global request budget shared by every chain (QuickNode limits are per account).
    #[serde(default = "default_account_rps")]
    pub account_rps: u32,
    /// Guaranteed share for receipts / estimates / confirmations under sustained send load.
    #[serde(default = "default_p1_rps")]
    pub reserved_p1_rps: u32,
    /// Guaranteed share for health probes and balance sweeps.
    #[serde(default = "default_p2_rps")]
    pub reserved_p2_rps: u32,
    #[serde(default = "default_rpc_timeout_ms")]
    pub request_timeout_ms: u64,
    #[serde(default = "default_rpc_connect_timeout_ms")]
    pub connect_timeout_ms: u64,
    /// 0 disables the budget alert.
    #[serde(default)]
    pub monthly_credit_budget: u64,
    /// 0 disables shedding. When exceeded, new jobs get 503 `shedding`; in-flight work always finishes.
    #[serde(default)]
    pub daily_credit_cap: u64,
}

impl Default for RpcConfig {
    fn default() -> Self {
        Self {
            account_rps: default_account_rps(),
            reserved_p1_rps: default_p1_rps(),
            reserved_p2_rps: default_p2_rps(),
            request_timeout_ms: default_rpc_timeout_ms(),
            connect_timeout_ms: default_rpc_connect_timeout_ms(),
            monthly_credit_budget: 0,
            daily_credit_cap: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SignerMode {
    Local,
    Kms,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SignersConfig {
    pub mode: SignerMode,
    #[serde(default)]
    pub local_private_keys: Vec<String>,
    #[serde(default)]
    pub kms_key_ids: Vec<String>,
    #[serde(default = "default_kms_timeout_ms")]
    pub kms_timeout_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
    /// Per-chain cap on queued jobs; beyond it `POST /v1/transactions` answers 503 `queue_full`.
    #[serde(default = "default_max_depth")]
    pub max_depth: usize,
    /// Jobs held in memory per chain; the rest wait in Postgres and are paged in by the sweep.
    #[serde(default = "default_window")]
    pub memory_window: usize,
    #[serde(default = "default_sweep_ms")]
    pub sweep_interval_ms: u64,
    /// A job that was requeued this many times fails instead of pausing yet another signer.
    #[serde(default = "default_max_requeues")]
    pub max_requeues: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self { max_depth: default_max_depth(), memory_window: default_window(), sweep_interval_ms: default_sweep_ms(), max_requeues: default_max_requeues() }
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AlertsConfig {
    /// Optional endpoint that receives every `alert=true` event as JSON.
    #[serde(default)]
    pub webhook_url: Option<String>,
}

/// `deny_unknown_fields` cannot be combined with `flatten`, so unknown keys are collected into
/// `tunables_raw` and parsed strictly by [`ChainConfig::overrides`] — a typo in a tunable is a boot error.
#[derive(Debug, Clone, Deserialize)]
pub struct ChainConfig {
    pub chain_id: u64,
    pub kind: String,
    #[serde(default)]
    pub rpc_url: Option<String>,
    /// Name of an environment variable holding the RPC URL (keeps secrets out of files).
    #[serde(default)]
    pub rpc_url_env: Option<String>,
    /// Sent as the `x-token` header, so the token never appears in a URL.
    #[serde(default)]
    pub rpc_token_env: Option<String>,
    #[serde(default)]
    pub fallback_rpc_urls: Vec<String>,
    #[serde(default)]
    pub treasury_private_key: Option<String>,
    #[serde(default)]
    pub treasury_key_id: Option<String>,
    #[serde(deserialize_with = "de_wei")]
    pub signer_min_balance: U256,
    #[serde(deserialize_with = "de_wei")]
    pub topup_amount: U256,
    /// Amount of a signer's *first* top-up on this chain (its initial funding). Defaults to `topup_amount`.
    #[serde(default, deserialize_with = "de_wei_opt")]
    pub initial_topup_amount: Option<U256>,
    #[serde(deserialize_with = "de_wei")]
    pub treasury_min_balance: U256,
    /// Largest worst-case cost a single job may have. Defaults to `topup_amount`.
    #[serde(default, deserialize_with = "de_wei_opt")]
    pub max_job_cost: Option<U256>,
    #[serde(default = "default_topups_per_hour")]
    pub max_topups_per_signer_per_hour: u32,
    #[serde(flatten)]
    tunables_raw: serde_json::Map<String, serde_json::Value>,
}

impl ChainConfig {
    pub fn resolve_rpc_url(&self, name: &str) -> anyhow::Result<String> {
        if let Some(var) = &self.rpc_url_env {
            return std::env::var(var).map_err(|_| anyhow::anyhow!("chain `{name}`: environment variable `{var}` (rpc_url_env) is not set"));
        }
        self.rpc_url.clone().ok_or_else(|| anyhow::anyhow!("chain `{name}`: set `rpc_url` or `rpc_url_env`"))
    }

    pub fn resolve_rpc_token(&self) -> Option<String> {
        self.rpc_token_env.as_ref().and_then(|var| std::env::var(var).ok())
    }

    /// Tunable overrides from this chain's block. Unknown keys are rejected.
    pub fn overrides(&self, name: &str) -> anyhow::Result<TunableOverrides> {
        TunableOverrides::deserialize(serde_json::Value::Object(self.tunables_raw.clone())).map_err(|e| anyhow::anyhow!("chain `{name}`: {e}"))
    }

    pub fn initial_topup_amount(&self) -> U256 {
        self.initial_topup_amount.unwrap_or(self.topup_amount)
    }

    pub fn max_job_cost(&self) -> U256 {
        self.max_job_cost.unwrap_or(self.topup_amount)
    }
}

/// Declares `ChainTunables` (every field required; produced by a chain kind's `defaults()`) together with
/// `TunableOverrides` (every field optional; parsed from a `[chains.*]` block) and the merge between them.
macro_rules! tunables {
    ($( $(#[$doc:meta])* $name:ident : $ty:ty ),+ $(,)?) => {
        #[derive(Debug, Clone, PartialEq)]
        pub struct ChainTunables { $( $(#[$doc])* pub $name: $ty, )+ }

        #[derive(Debug, Clone, Default, Deserialize)]
        #[serde(deny_unknown_fields)]
        pub struct TunableOverrides { $( #[serde(default)] pub $name: Option<$ty>, )+ }

        impl ChainTunables {
            pub fn with_overrides(mut self, o: &TunableOverrides) -> Self {
                $( if let Some(v) = &o.$name { self.$name = v.clone(); } )+
                self
            }
        }
    };
}

tunables! {
    /// Nominal block time; used only to size timers, never as a finality assumption.
    block_time_ms: u64,
    /// Delay before the inclusion is re-checked and `transaction.confirmed` fires. 0 = confirmed on receipt.
    confirmation_delay_ms: u64,
    /// Multiplier applied to `eth_estimateGas` results, in basis points (12000 = 1.2x).
    estimate_multiplier_bps: u32,
    /// `max_fee = base_fee * this + tip`, in basis points. Overpaying `max_fee` costs nothing under EIP-1559.
    max_fee_multiplier_bps: u32,
    priority_fee_wei: u128,
    /// Hard ceiling for `max_fee_per_gas` on job transactions, in wei.
    max_fee_cap_wei: u128,
    /// Ceiling for cancel (NOOP) transactions; must out-bid a job already sitting at `max_fee_cap_wei`.
    cancel_fee_cap_wei: u128,
    /// Base-fee observations older than this are refreshed before the next send.
    fee_ttl_ms: u64,
    /// A sent transaction becomes "stuck" once the head is this many blocks past its send block.
    stuck_after_blocks: u64,
    max_bumps: u32,
    /// Server-side timeout passed to `eth_sendRawTransactionSync`.
    sync_send_timeout_ms: u64,
    /// Poll interval of the fallback receipt tracker (async sends and sync timeouts only).
    receipt_poll_interval_ms: u64,
    /// With no passive head observation for this long, probe the chain once.
    idle_probe_interval_ms: u64,
    degraded_probe_interval_ms: u64,
    stall_warn_ms: u64,
    stall_outage_ms: u64,
    /// Consecutive RPC transport failures that open the chain's circuit breaker.
    rpc_failure_threshold: u32,
    /// QuickNode credits charged per successful call on this chain.
    credits_per_call: u32,
    /// Per-transaction gas ceiling enforced at the API.
    max_tx_gas: u64,
    balance_sweep_interval_ms: u64,
    topup_wait_timeout_ms: u64,
    /// `head_advance`: a head that stops moving is an outage. `rpc_responsive`: judge by RPC health only.
    liveness: crate::chain::adapter::LivenessMode,
}

impl Config {
    pub fn load() -> anyhow::Result<Self> {
        let path = std::env::var("GUM_CONFIG").unwrap_or_else(|_| "config/default.toml".to_string());
        Self::load_from(&path)
    }

    pub fn load_from(path: &str) -> anyhow::Result<Self> {
        let mut figment = Figment::new().merge(Toml::file(path)).merge(Env::prefixed("GUM_").ignore(&["CONFIG"]).split("__"));
        if let Ok(url) = std::env::var("DATABASE_URL") {
            figment = figment.merge(("database.url", url));
        }
        if let Ok(port) = std::env::var("PORT") {
            let port: u16 = port.parse().map_err(|_| anyhow::anyhow!("PORT is not a valid port: `{port}`"))?;
            figment = figment.merge(("server.port", port));
        }
        let config: Config = figment.extract().map_err(|e| anyhow::anyhow!("invalid configuration ({path}): {e}"))?;
        config.validate()?;
        Ok(config)
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(!self.chains.is_empty(), "at least one [chains.*] block is required");
        anyhow::ensure!(!self.webhook.signing_secret.is_empty(), "webhook.signing_secret must not be empty");
        match self.signers.mode {
            SignerMode::Local => {
                anyhow::ensure!(!self.signers.local_private_keys.is_empty(), "signers.local_private_keys is empty")
            }
            SignerMode::Kms => anyhow::ensure!(!self.signers.kms_key_ids.is_empty(), "signers.kms_key_ids is empty"),
        }
        let mut seen = std::collections::BTreeSet::new();
        for (name, chain) in &self.chains {
            anyhow::ensure!(seen.insert(chain.chain_id), "chain id {} is configured twice", chain.chain_id);
            match self.signers.mode {
                SignerMode::Local => anyhow::ensure!(chain.treasury_private_key.is_some(), "chain `{name}`: treasury_private_key is required when signers.mode = \"local\""),
                SignerMode::Kms => anyhow::ensure!(chain.treasury_key_id.is_some(), "chain `{name}`: treasury_key_id is required when signers.mode = \"kms\""),
            }
            chain.overrides(name)?;
            anyhow::ensure!(chain.topup_amount > U256::ZERO, "chain `{name}`: topup_amount must be greater than zero");
        }
        anyhow::ensure!(self.rpc.reserved_p1_rps + self.rpc.reserved_p2_rps < self.rpc.account_rps, "rpc.reserved_p1_rps + rpc.reserved_p2_rps must be below rpc.account_rps");
        Ok(())
    }
}

fn parse_wei(s: &str) -> Result<U256, String> {
    let s = s.trim();
    let parsed = if let Some(hex) = s.strip_prefix("0x") { U256::from_str_radix(hex, 16) } else { U256::from_str_radix(s, 10) };
    parsed.map_err(|e| format!("invalid wei amount `{s}`: {e}"))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum WeiRepr {
    Str(String),
    Int(u64),
}

fn de_wei<'de, D: Deserializer<'de>>(d: D) -> Result<U256, D::Error> {
    match WeiRepr::deserialize(d)? {
        WeiRepr::Str(s) => parse_wei(&s).map_err(serde::de::Error::custom),
        WeiRepr::Int(n) => Ok(U256::from(n)),
    }
}

fn de_wei_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<U256>, D::Error> {
    match Option::<WeiRepr>::deserialize(d)? {
        None => Ok(None),
        Some(WeiRepr::Str(s)) => parse_wei(&s).map(Some).map_err(serde::de::Error::custom),
        Some(WeiRepr::Int(n)) => Ok(Some(U256::from(n))),
    }
}

fn default_true() -> bool {
    true
}
fn default_port() -> u16 {
    8080
}
fn default_body_limit() -> usize {
    256 * 1024
}
fn default_drain_ms() -> u64 {
    25_000
}
fn default_pipeline_pool() -> u32 {
    12
}
fn default_api_pool() -> u32 {
    6
}
fn default_api_statement_timeout_ms() -> u64 {
    5_000
}
fn default_webhook_concurrency() -> usize {
    64
}
fn default_webhook_per_host() -> usize {
    16
}
fn default_webhook_timeout_ms() -> u64 {
    10_000
}
fn default_webhook_attempts() -> u32 {
    12
}
fn default_webhook_backoff_base_ms() -> u64 {
    1_000
}
fn default_webhook_backoff_max_ms() -> u64 {
    3_600_000
}
fn default_account_rps() -> u32 {
    45
}
fn default_p1_rps() -> u32 {
    8
}
fn default_p2_rps() -> u32 {
    2
}
fn default_rpc_timeout_ms() -> u64 {
    10_000
}
fn default_rpc_connect_timeout_ms() -> u64 {
    3_000
}
fn default_kms_timeout_ms() -> u64 {
    5_000
}
fn default_max_depth() -> usize {
    100_000
}
fn default_window() -> usize {
    5_000
}
fn default_sweep_ms() -> u64 {
    5_000
}
fn default_max_requeues() -> u32 {
    5
}
fn default_topups_per_hour() -> u32 {
    12
}
