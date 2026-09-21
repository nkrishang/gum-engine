//! Open-loop load generator.
//!
//! Arrivals are scheduled at fixed instants (`t0 + i / rate`) and fired whether or not earlier requests
//! have completed. Latency is measured from the *intended* start instant, so a stalled engine (or a
//! stalled harness) cannot hide behind coordinated omission. How late the harness actually fired each
//! request is recorded separately (`schedule lag`) so a harness bottleneck is visible in the report.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use alloy::primitives::{Address, Bytes, B256, U256};
use hdrhistogram::Histogram;
use parking_lot::Mutex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::api::{EngineClient, SubmitResult, TxStatus};
use crate::oracle;
use crate::sink::{Sink, SinkMode};
use crate::util::{new_hist, record, Clock};

pub const GAS_HIT: u64 = 120_000;
pub const GAS_FAIL: u64 = 60_000;
pub const GAS_TRANSFER: u64 = 21_000;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    /// `hit(id)` with an explicit gas_limit → confirmed/success, no simulation.
    HitGas,
    /// `hit(id)` without gas_limit → engine estimates → confirmed/success.
    HitNoGas,
    /// `burn(id, n)` with an explicit gas_limit → confirmed/success.
    BurnGas,
    /// `fail()` with a gas_limit → included on-chain and reverted.
    FailGas,
    /// `fail()` without gas_limit → failed(simulation_reverted), never on-chain.
    FailNoGas,
    /// Plain value transfer to a unique recipient (gas_limit 21000).
    Transfer,
}

impl JobKind {
    pub fn label(&self) -> &'static str {
        match self {
            JobKind::HitGas => "hit_gas",
            JobKind::HitNoGas => "hit_nogas",
            JobKind::BurnGas => "burn_gas",
            JobKind::FailGas => "fail_gas",
            JobKind::FailNoGas => "fail_nogas",
            JobKind::Transfer => "transfer",
        }
    }
    pub fn has_gas_limit(&self) -> bool {
        !matches!(self, JobKind::HitNoGas | JobKind::FailNoGas)
    }
}

/// Relative weights of each job kind.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct JobMix {
    pub hit_gas: u32,
    pub hit_nogas: u32,
    pub burn_gas: u32,
    pub fail_gas: u32,
    pub fail_nogas: u32,
    pub transfer: u32,
}

impl JobMix {
    pub const MIXED: JobMix = JobMix {
        hit_gas: 40,
        hit_nogas: 30,
        burn_gas: 5,
        fail_gas: 8,
        fail_nogas: 7,
        transfer: 10,
    };
    pub const HIT_GAS_ONLY: JobMix = JobMix {
        hit_gas: 1,
        hit_nogas: 0,
        burn_gas: 0,
        fail_gas: 0,
        fail_nogas: 0,
        transfer: 0,
    };
    pub const HIT_NOGAS_ONLY: JobMix = JobMix {
        hit_gas: 0,
        hit_nogas: 1,
        burn_gas: 0,
        fail_gas: 0,
        fail_nogas: 0,
        transfer: 0,
    };

    pub fn parse(s: &str) -> anyhow::Result<JobMix> {
        match s {
            "mixed" => return Ok(Self::MIXED),
            "gas" | "with-gas" => return Ok(Self::HIT_GAS_ONLY),
            "nogas" | "without-gas" => return Ok(Self::HIT_NOGAS_ONLY),
            _ => {}
        }
        let mut m = JobMix {
            hit_gas: 0,
            hit_nogas: 0,
            burn_gas: 0,
            fail_gas: 0,
            fail_nogas: 0,
            transfer: 0,
        };
        for part in s.split(',') {
            let (k, v) = part.split_once('=').ok_or_else(|| {
                anyhow::anyhow!(
                    "bad --mix entry {part:?}; expected kind=weight (e.g. hit_gas=3,transfer=1)"
                )
            })?;
            let w: u32 = v
                .parse()
                .map_err(|_| anyhow::anyhow!("bad weight in --mix entry {part:?}"))?;
            match k {
                "hit_gas" => m.hit_gas = w,
                "hit_nogas" => m.hit_nogas = w,
                "burn_gas" => m.burn_gas = w,
                "fail_gas" => m.fail_gas = w,
                "fail_nogas" => m.fail_nogas = w,
                "transfer" => m.transfer = w,
                _ => anyhow::bail!("unknown job kind {k:?} in --mix"),
            }
        }
        if m.weights().iter().map(|(_, w)| *w).sum::<u32>() == 0 {
            anyhow::bail!("--mix has no positive weight");
        }
        Ok(m)
    }

    fn weights(&self) -> [(JobKind, u32); 6] {
        [
            (JobKind::HitGas, self.hit_gas),
            (JobKind::HitNoGas, self.hit_nogas),
            (JobKind::BurnGas, self.burn_gas),
            (JobKind::FailGas, self.fail_gas),
            (JobKind::FailNoGas, self.fail_nogas),
            (JobKind::Transfer, self.transfer),
        ]
    }

    fn pick(&self, rng: &mut StdRng) -> JobKind {
        pick_weighted(&self.weights(), rng)
    }
}

fn pick_weighted<T: Copy>(items: &[(T, u32)], rng: &mut StdRng) -> T {
    let total: u32 = items.iter().map(|(_, w)| *w).sum();
    let mut x = rng.random_range(0..total.max(1));
    for (item, w) in items {
        if x < *w {
            return *item;
        }
        x -= *w;
    }
    items[0].0
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplayPlan {
    None,
    /// Re-POST the identical body + key → expect 200 `replayed:true` with the same job id.
    Same,
    /// Re-POST a different body under the same key → expect 409 `idempotency_conflict`.
    Conflict,
}

#[derive(Clone, Debug, Serialize)]
pub struct JobSpec {
    pub idx: usize,
    pub kind: JobKind,
    pub chain_id: u64,
    /// The unique id that makes this job attributable on-chain.
    pub bench_id: B256,
    pub to: Address,
    #[serde(skip)]
    pub data: Bytes,
    pub value: U256,
    pub gas_limit: Option<u64>,
    pub webhook_mode: SinkMode,
    #[serde(skip)]
    pub webhook_url: String,
    pub idem_key: Option<String>,
    pub replay: ReplayPlan,
}

impl JobSpec {
    pub fn body(&self) -> Value {
        let mut b = json!({
            "chain_id": self.chain_id,
            "to": format!("{:#x}", self.to),
            "data": format!("0x{}", hex::encode(&self.data)),
            "value": self.value.to_string(),
            "webhook": { "url": self.webhook_url },
        });
        if let Some(g) = self.gas_limit {
            b["gas_limit"] = json!(g);
        }
        b
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ReplayResult {
    pub plan: ReplayPlan,
    pub result: SubmitResult,
}

/// Everything the harness knows about one job. Times are offsets on the run clock.
#[derive(Clone, Debug, Serialize)]
pub struct JobRecord {
    pub spec: JobSpec,
    pub intended: Duration,
    pub sent: Duration,
    pub responded: Duration,
    pub attempts: u32,
    pub submit: SubmitResult,
    /// True if any POST attempt for this job ended without an HTTP response (the engine may have it).
    pub indeterminate: bool,
    pub replay: Option<ReplayResult>,
    pub final_status: Option<TxStatus>,
    pub final_poll_error: Option<String>,
}

impl JobRecord {
    /// The engine acknowledged this job (202, or 200 replay of an earlier indeterminate attempt).
    pub fn accepted(&self) -> bool {
        self.submit.job_id.is_some() && matches!(self.submit.status, Some(202) | Some(200))
    }
    pub fn job_id(&self) -> Option<&str> {
        if self.accepted() {
            self.submit.job_id.as_deref()
        } else {
            None
        }
    }
}

#[derive(Clone, Debug)]
pub struct ChainTarget {
    pub chain_id: u64,
    pub target: Address,
}

#[derive(Clone, Debug, Serialize)]
pub struct LoadConfig {
    pub mix: JobMix,
    #[serde(skip)]
    pub chains: Vec<ChainTarget>,
    pub webhook_modes: Vec<(SinkMode, u32)>,
    pub slow_ms: u64,
    pub flaky_p: f64,
    pub burn_iterations: u64,
    /// Fraction of jobs sent with an `Idempotency-Key`.
    pub idem_fraction: f64,
    /// Fraction of keyed jobs that are deliberately replayed (alternating same-body / conflicting-body).
    pub replay_fraction: f64,
    /// Keyed jobs retry (same key) on transport errors and 503s, like a well-behaved client would.
    pub retry_with_idem: bool,
    pub seed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Segment {
    pub start_s: f64,
    pub rate: f64,
    pub duration_s: f64,
    pub scheduled: usize,
}

pub struct Loadgen {
    client: EngineClient,
    sink: Sink,
    clock: Clock,
    pub cfg: LoadConfig,
    rng: Mutex<StdRng>,
    next_idx: AtomicUsize,
    chain_rr: AtomicUsize,
    replay_rr: AtomicUsize,
    inflight: AtomicUsize,
    records: Mutex<Vec<JobRecord>>,
    lag: Mutex<Histogram<u64>>,
    segments: Mutex<Vec<Segment>>,
}

impl Loadgen {
    pub fn new(client: EngineClient, sink: Sink, clock: Clock, cfg: LoadConfig) -> Arc<Self> {
        Arc::new(Self {
            client,
            sink,
            clock,
            rng: Mutex::new(StdRng::seed_from_u64(cfg.seed)),
            cfg,
            next_idx: AtomicUsize::new(0),
            chain_rr: AtomicUsize::new(0),
            replay_rr: AtomicUsize::new(0),
            inflight: AtomicUsize::new(0),
            records: Mutex::new(Vec::new()),
            lag: Mutex::new(new_hist()),
            segments: Mutex::new(Vec::new()),
        })
    }

    fn make_job(&self, mix: &JobMix) -> JobSpec {
        let idx = self.next_idx.fetch_add(1, Ordering::Relaxed);
        let chain =
            &self.cfg.chains[self.chain_rr.fetch_add(1, Ordering::Relaxed) % self.cfg.chains.len()];
        let mut rng = self.rng.lock();
        let kind = mix.pick(&mut rng);
        let bench_id = B256::from(rng.random::<[u8; 32]>());
        let mode = pick_weighted(&self.cfg.webhook_modes, &mut rng);
        let keyed = rng.random::<f64>() < self.cfg.idem_fraction;
        let replayed = keyed && rng.random::<f64>() < self.cfg.replay_fraction;
        drop(rng);
        let (to, data, value, gas_limit) = match kind {
            JobKind::HitGas => (
                chain.target,
                oracle::hit_calldata(bench_id),
                U256::ZERO,
                Some(GAS_HIT),
            ),
            JobKind::HitNoGas => (
                chain.target,
                oracle::hit_calldata(bench_id),
                U256::ZERO,
                None,
            ),
            JobKind::BurnGas => (
                chain.target,
                oracle::burn_calldata(bench_id, self.cfg.burn_iterations),
                U256::ZERO,
                Some(GAS_HIT + self.cfg.burn_iterations * 400),
            ),
            JobKind::FailGas => (
                chain.target,
                oracle::fail_calldata(bench_id),
                U256::ZERO,
                Some(GAS_FAIL),
            ),
            JobKind::FailNoGas => (
                chain.target,
                oracle::fail_calldata(bench_id),
                U256::ZERO,
                None,
            ),
            JobKind::Transfer => (
                oracle::transfer_recipient(bench_id),
                Bytes::new(),
                U256::from(1_000u64),
                Some(GAS_TRANSFER),
            ),
        };
        let replay = if !replayed {
            ReplayPlan::None
        } else if self
            .replay_rr
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(2)
        {
            ReplayPlan::Same
        } else {
            ReplayPlan::Conflict
        };
        JobSpec {
            idx,
            kind,
            chain_id: chain.chain_id,
            bench_id,
            to,
            data,
            value,
            gas_limit,
            webhook_mode: mode,
            webhook_url: self.sink.url(mode, self.cfg.slow_ms, self.cfg.flaky_p),
            idem_key: keyed.then(|| format!("bench-{}", hex::encode(&bench_id[..16]))),
            replay,
        }
    }

    /// Fire `rate` jobs/s for `duration` on a fixed schedule. Returns the number of arrivals scheduled.
    /// `stop` aborts the segment early (remaining arrivals are not scheduled).
    pub async fn run_segment(
        self: &Arc<Self>,
        rate: f64,
        duration: Duration,
        mix: Option<JobMix>,
        stop: &AtomicBool,
    ) -> usize {
        let mix = mix.unwrap_or(self.cfg.mix);
        let n = (rate * duration.as_secs_f64()).floor() as usize;
        let t0 = Instant::now();
        let seg_start = self.clock.at(t0);
        let mut scheduled = 0;
        for i in 0..n {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            let intended = t0 + Duration::from_secs_f64(i as f64 / rate);
            tokio::time::sleep_until(intended.into()).await;
            let spec = self.make_job(&mix);
            scheduled += 1;
            self.inflight.fetch_add(1, Ordering::Relaxed);
            let me = self.clone();
            tokio::spawn(async move {
                me.fire(spec, intended).await;
                me.inflight.fetch_sub(1, Ordering::Relaxed);
            });
        }
        // Hold the segment for its full length so consecutive segments keep their shape.
        if !stop.load(Ordering::Relaxed) {
            tokio::time::sleep_until((t0 + duration).into()).await;
        }
        self.segments.lock().push(Segment {
            start_s: seg_start.as_secs_f64(),
            rate,
            duration_s: t0.elapsed().as_secs_f64(),
            scheduled,
        });
        scheduled
    }

    async fn fire(&self, spec: JobSpec, intended: Instant) {
        let body = spec.body();
        let sent = Instant::now();
        record(
            &mut self.lag.lock(),
            sent.saturating_duration_since(intended),
        );
        let mut attempts = 1;
        let mut indeterminate = false;
        let mut submit = self.client.submit(&body, spec.idem_key.as_deref()).await;
        if self.cfg.retry_with_idem && spec.idem_key.is_some() {
            while attempts < 20 && (submit.transport_error.is_some() || submit.status == Some(503))
            {
                indeterminate |= submit.transport_error.is_some();
                tokio::time::sleep(Duration::from_millis(500)).await;
                submit = self.client.submit(&body, spec.idem_key.as_deref()).await;
                attempts += 1;
            }
        }
        indeterminate |= submit.transport_error.is_some();
        let responded = Instant::now();

        let mut replay = None;
        if spec.replay != ReplayPlan::None && submit.status == Some(202) && submit.job_id.is_some()
        {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let replay_body = match spec.replay {
                ReplayPlan::Conflict => {
                    let mut b = body.clone();
                    // Same key, different body: bump the value by one wei.
                    b["value"] = json!((spec.value + U256::from(1u64)).to_string());
                    b
                }
                _ => body.clone(),
            };
            let result = self
                .client
                .submit(&replay_body, spec.idem_key.as_deref())
                .await;
            replay = Some(ReplayResult {
                plan: spec.replay,
                result,
            });
        }

        self.records.lock().push(JobRecord {
            spec,
            intended: self.clock.at(intended),
            sent: self.clock.at(sent),
            responded: self.clock.at(responded),
            attempts,
            submit,
            indeterminate,
            replay,
            final_status: None,
            final_poll_error: None,
        });
    }

    /// Wait until every fired request has its HTTP outcome recorded.
    pub async fn wait_idle(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while self.inflight.load(Ordering::Relaxed) > 0 {
            if Instant::now() > deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        true
    }

    pub fn inflight(&self) -> usize {
        self.inflight.load(Ordering::Relaxed)
    }

    pub fn accepted_so_far(&self) -> usize {
        self.records.lock().iter().filter(|r| r.accepted()).count()
    }

    pub fn take_records(&self) -> Vec<JobRecord> {
        let mut v = std::mem::take(&mut *self.records.lock());
        v.sort_by_key(|r| r.spec.idx);
        v
    }

    pub fn segments(&self) -> Vec<Segment> {
        self.segments.lock().clone()
    }

    pub fn lag_hist(&self) -> Histogram<u64> {
        self.lag.lock().clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mix_parsing() {
        assert!(JobMix::parse("mixed").is_ok());
        let m = JobMix::parse("hit_gas=3,transfer=1").unwrap();
        assert_eq!((m.hit_gas, m.transfer, m.hit_nogas), (3, 1, 0));
        assert!(JobMix::parse("hit_gas=0").is_err());
        assert!(JobMix::parse("nope=1").is_err());
    }

    #[test]
    fn weighted_pick_respects_zero_weights() {
        let mut rng = StdRng::seed_from_u64(1);
        for _ in 0..200 {
            assert_eq!(JobMix::HIT_GAS_ONLY.pick(&mut rng), JobKind::HitGas);
        }
    }
}
