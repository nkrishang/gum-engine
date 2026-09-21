//! Ramp the offered rate until the queue grows monotonically, then report the max sustainable rate.
//! extra: step_secs (10), step (capacity/8), growth_samples (5). `--rate` = starting rate
//! (default: half the theoretical capacity), `--duration` = upper bound for the whole ramp.

use std::time::Duration;

use anyhow::Result;
use futures::future::BoxFuture;
use serde_json::json;

use super::{Ctx, Params, Scenario};
use crate::loadgen::JobMix;
use crate::report::Report;
use crate::tracker::{finished_rate, queue_growth_streak};
use crate::verdict::Violations;

pub struct Saturation;

impl Scenario for Saturation {
    fn name(&self) -> &'static str {
        "saturation"
    }
    fn about(&self) -> &'static str {
        "ramp offered rate until queue depth grows monotonically; reports max sustainable jobs/s"
    }
    fn defaults(&self) -> Params {
        // rate 0 is replaced by capacity/2 in drive().
        Params {
            chains: 1,
            signers: 10,
            rate: 5.0,
            duration_s: 240.0,
            mix: JobMix::HIT_GAS_ONLY,
            idem_fraction: 0.0,
            replay_fraction: 0.0,
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let cap = ctx.params.capacity_per_chain() * ctx.params.chains as f64;
            let step_secs = ctx.params.extra_f64("step_secs", 10.0);
            let step = ctx.params.extra_f64("step", (cap / 8.0).max(0.5));
            let need = ctx.params.extra_f64("growth_samples", 5.0) as usize;
            let mut rate = ctx.params.rate;
            let mut sustainable: Option<f64> = None;
            let mut saturated_at: Option<f64> = None;
            let mut steps = Vec::new();
            let mut elapsed = 0.0;
            while elapsed < ctx.params.duration_s {
                let from = ctx.clock.now().as_secs_f64();
                ctx.constant(rate, step_secs).await;
                elapsed += step_secs;
                // let the last sample of this step land
                tokio::time::sleep(Duration::from_millis(300)).await;
                let samples = ctx.sampler.samples();
                let to = ctx.clock.now().as_secs_f64();
                let in_step: Vec<_> = samples
                    .iter()
                    .filter(|s| s.t_s >= from && s.t_s <= to)
                    .cloned()
                    .collect();
                let streak = queue_growth_streak(&in_step);
                let queued = in_step
                    .iter()
                    .rev()
                    .find(|s| s.ok)
                    .map(|s| s.counts.queued)
                    .unwrap_or(0);
                let done = finished_rate(&samples, from, to, true)
                    .map(|(r, _)| r)
                    .unwrap_or(0.0);
                steps.push(json!({"rate": rate, "on_chain_per_s": (done * 100.0).round() / 100.0, "queued_at_end": queued, "growth_streak": streak}));
                ctx.note(format!("saturation: offered {rate:.1}/s → on-chain {done:.2}/s, queued {queued}, growth streak {streak}"));
                if streak >= need && queued as f64 > rate {
                    saturated_at = Some(rate);
                    break;
                }
                // Sustained = the step ended with less than one second's worth of backlog.
                if (queued as f64) < rate {
                    sustainable = Some(rate);
                }
                rate += step;
            }
            ctx.metric("theoretical_capacity_per_s", cap);
            ctx.metric("max_sustainable_rate", json!(sustainable));
            ctx.metric("saturated_at_rate", json!(saturated_at));
            // The headline number: the highest on-chain rate the engine actually delivered in any step.
            let peak = steps
                .iter()
                .filter_map(|s| s["on_chain_per_s"].as_f64())
                .fold(0.0, f64::max);
            ctx.metric("max_sustainable_throughput_per_s", peak);
            ctx.metric("steps", json!(steps));
            Ok(())
        })
    }
    fn assess(&self, ctx: &Ctx, report: &Report, v: &mut Violations) {
        let cap = ctx.params.capacity_per_chain() * ctx.params.chains as f64;
        let m = &report.scenario_metrics;
        if m.get("saturated_at_rate")
            .map(|x| x.is_null())
            .unwrap_or(true)
        {
            v.add(
                "s",
                "never_saturated",
                "the ramp ended before the queue started growing; raise --duration or --rate",
                "",
                "",
            );
        }
        let peak = m
            .get("steps")
            .and_then(|s| s.as_array())
            .map(|a| {
                a.iter()
                    .filter_map(|s| s["on_chain_per_s"].as_f64())
                    .fold(0.0, f64::max)
            })
            .unwrap_or(0.0);
        if peak < 0.6 * cap {
            v.add(
                "s",
                "capacity_not_reached",
                "peak on-chain throughput is far below signers/block_time",
                "",
                format!("peak {peak:.2}/s vs theoretical {cap:.2}/s"),
            );
        }
    }
}
