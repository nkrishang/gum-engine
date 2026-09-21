//! Observes the engine through its public routes only: a 1 Hz time series during the run, and the final
//! status of every accepted job at the end.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::stream::{self, StreamExt};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::api::{AnalyticsResp, ChainsResp, Counts, EngineClient, SignersResp};
use crate::loadgen::JobRecord;
use crate::util::Clock;

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Sample {
    pub t_s: f64,
    /// All three routes answered and parsed.
    pub ok: bool,
    pub counts: Counts,
    pub by_chain: BTreeMap<u64, Counts>,
    /// Sum of `queue_depth` over `/v1/chains`.
    pub queue_depth: u64,
    pub busy_pairs: u64,
    pub paused_pairs: u64,
    pub total_pairs: u64,
    /// busy signer pairs / total signer pairs (treasury pairs excluded).
    pub utilisation: f64,
}

pub struct Sampler {
    samples: Arc<Mutex<Vec<Sample>>>,
    stop: Arc<AtomicBool>,
}

impl Sampler {
    pub fn start(client: EngineClient, clock: Clock) -> Sampler {
        let samples = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let (s2, stop2) = (samples.clone(), stop.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            while !stop2.load(Ordering::Relaxed) {
                tick.tick().await;
                let client = client.clone();
                let s3 = s2.clone();
                // Each sample runs detached so a hung engine cannot stall the sampling clock.
                tokio::spawn(async move {
                    let sample = take_sample(&client, clock).await;
                    s3.lock().push(sample);
                });
            }
        });
        Sampler { samples, stop }
    }

    pub fn stop(&self) {
        self.stop.store(true, Ordering::Relaxed);
    }

    pub fn samples(&self) -> Vec<Sample> {
        let mut v = self.samples.lock().clone();
        v.sort_by(|a, b| a.t_s.total_cmp(&b.t_s));
        v
    }
}

pub async fn take_sample(client: &EngineClient, clock: Clock) -> Sample {
    let t_s = clock.now().as_secs_f64();
    let to = Duration::from_millis(900);
    let (a, s, c) = tokio::join!(
        client.get_json::<AnalyticsResp>("/v1/analytics/transactions", to),
        client.get_json::<SignersResp>("/v1/signers", to),
        client.get_json::<ChainsResp>("/v1/chains", to),
    );
    let mut sample = Sample {
        t_s,
        ok: a.is_ok() && s.is_ok() && c.is_ok(),
        ..Default::default()
    };
    if let Ok(a) = a {
        sample.counts = a.totals;
        sample.by_chain = a
            .by_chain
            .into_iter()
            .map(|c| (c.chain_id, c.counts))
            .collect();
    }
    if let Ok(s) = s {
        let signer_pairs: Vec<_> = s.pairs.iter().filter(|p| p.role != "treasury").collect();
        sample.total_pairs = signer_pairs.len() as u64;
        sample.busy_pairs = signer_pairs.iter().filter(|p| p.state == "busy").count() as u64;
        sample.paused_pairs = signer_pairs.iter().filter(|p| p.state == "paused").count() as u64;
        if sample.total_pairs > 0 {
            sample.utilisation = sample.busy_pairs as f64 / sample.total_pairs as f64;
        }
    }
    if let Ok(c) = c {
        sample.queue_depth = c.chains.iter().map(|c| c.queue_depth).sum();
    }
    sample
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DrainStats {
    pub waited_s: f64,
    pub poll_rounds: u32,
    pub timed_out: bool,
    pub reason: Option<String>,
}

/// Poll `GET /v1/transactions/{id}` for every accepted job until all are terminal.
/// Gives up when nothing became terminal for `stall_timeout`, or after `hard_timeout` overall.
pub async fn drain(
    client: &EngineClient,
    jobs: &mut [JobRecord],
    stall_timeout: Duration,
    hard_timeout: Duration,
) -> DrainStats {
    let started = Instant::now();
    let mut last_progress = Instant::now();
    let mut stats = DrainStats::default();
    loop {
        let pending: Vec<(usize, String)> = jobs
            .iter()
            .enumerate()
            .filter(|(_, j)| {
                !j.final_status
                    .as_ref()
                    .map(|s| s.is_terminal())
                    .unwrap_or(false)
            })
            .filter_map(|(i, j)| j.job_id().map(|id| (i, id.to_string())))
            .collect();
        if pending.is_empty() {
            break;
        }
        stats.poll_rounds += 1;
        let results: Vec<_> = stream::iter(pending)
            .map(|(i, id)| async move { (i, client.tx_status(&id).await) })
            .buffer_unordered(32)
            .collect()
            .await;
        let mut remaining = 0;
        for (i, res) in results {
            match res {
                Ok(Some(s)) => {
                    let terminal = s.is_terminal();
                    let changed = jobs[i]
                        .final_status
                        .as_ref()
                        .map(|p| p.status != s.status)
                        .unwrap_or(true);
                    jobs[i].final_status = Some(s);
                    jobs[i].final_poll_error = None;
                    if terminal || changed {
                        last_progress = Instant::now();
                    }
                    if !terminal {
                        remaining += 1;
                    }
                }
                Ok(None) => {
                    jobs[i].final_poll_error = Some("404 not_found".into());
                    remaining += 1;
                }
                Err(e) => {
                    jobs[i].final_poll_error = Some(format!("{e:#}"));
                    remaining += 1;
                }
            }
        }
        if remaining == 0 {
            break;
        }
        if last_progress.elapsed() > stall_timeout {
            stats.timed_out = true;
            stats.reason = Some(format!(
                "{remaining} job(s) made no progress for {stall_timeout:?}"
            ));
            break;
        }
        if started.elapsed() > hard_timeout {
            stats.timed_out = true;
            stats.reason = Some(format!(
                "{remaining} job(s) still not terminal after the hard limit of {hard_timeout:?}"
            ));
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    stats.waited_s = started.elapsed().as_secs_f64();
    stats
}

/// Length of the run of consecutive ok samples at the tail in which queued work never decreased —
/// counted only if the queue actually grew over that run. (Non-strict on purpose: with 1 s sampling and
/// 1 s blocks, two neighbouring samples of a growing queue are occasionally equal.)
pub fn queue_growth_streak(samples: &[Sample]) -> usize {
    let ok: Vec<&Sample> = samples.iter().filter(|s| s.ok).collect();
    let Some(last) = ok.last() else { return 0 };
    let mut streak = 0;
    let mut run_start = last.counts.queued;
    for w in ok.windows(2).rev() {
        if w[1].counts.queued >= w[0].counts.queued {
            streak += 1;
            run_start = w[0].counts.queued;
        } else {
            break;
        }
    }
    if last.counts.queued > run_start {
        streak
    } else {
        0
    }
}

/// Finished-jobs-per-second between two offsets on the run clock, from the analytics time series.
/// Returns (overall, per chain). `None` when the window has fewer than two usable samples.
pub fn finished_rate(
    samples: &[Sample],
    from_s: f64,
    to_s: f64,
    on_chain_only: bool,
) -> Option<(f64, BTreeMap<u64, f64>)> {
    let ok: Vec<&Sample> = samples
        .iter()
        .filter(|s| s.ok && s.t_s >= from_s && s.t_s <= to_s)
        .collect();
    let (first, last) = (ok.first()?, ok.last()?);
    let dt = last.t_s - first.t_s;
    if dt < 1.0 {
        return None;
    }
    let done = |c: &Counts| {
        if on_chain_only {
            c.succeeded + c.reverted
        } else {
            c.finished()
        }
    };
    let overall = (done(&last.counts).saturating_sub(done(&first.counts))) as f64 / dt;
    let mut per_chain = BTreeMap::new();
    for (chain, c) in &last.by_chain {
        let base = first.by_chain.get(chain).copied().unwrap_or_default();
        per_chain.insert(*chain, done(c).saturating_sub(done(&base)) as f64 / dt);
    }
    Some((overall, per_chain))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn s(t: f64, queued: u64, succeeded: u64) -> Sample {
        Sample {
            t_s: t,
            ok: true,
            counts: Counts {
                queued,
                succeeded,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn growth_streak_counts_strict_increases_at_tail() {
        let v = vec![
            s(0., 5, 0),
            s(1., 3, 0),
            s(2., 4, 0),
            s(3., 6, 0),
            s(4., 9, 0),
        ];
        assert_eq!(queue_growth_streak(&v), 3);
        let plateau = vec![s(0., 1, 0), s(1., 2, 0), s(2., 2, 0)];
        assert_eq!(queue_growth_streak(&plateau), 2);
        let flat = vec![s(0., 2, 0), s(1., 2, 0), s(2., 2, 0)];
        assert_eq!(queue_growth_streak(&flat), 0);
    }

    #[test]
    fn finished_rate_uses_window() {
        let v = vec![
            s(0., 0, 0),
            s(1., 0, 5),
            s(2., 0, 10),
            s(3., 0, 15),
            s(10., 0, 15),
        ];
        let (r, _) = finished_rate(&v, 0.0, 3.0, true).unwrap();
        assert!((r - 5.0).abs() < 1e-9);
    }
}
