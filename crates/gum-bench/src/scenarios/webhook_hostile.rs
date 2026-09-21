//! Slow, failing, flaky and dead webhook receivers must not dent transaction throughput.

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};
use crate::report::Report;
use crate::sink::SinkMode;
use crate::verdict::Violations;

pub struct WebhookHostile;

impl Scenario for WebhookHostile {
    fn name(&self) -> &'static str {
        "webhook-hostile"
    }
    fn about(&self) -> &'static str {
        "40% ok / 20% slow(3s) / 15% always-500 / 15% flaky / 10% dead receivers at a sub-capacity rate"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 5,
            rate: 3.0,
            duration_s: 40.0,
            slow_ms: 3000,
            flaky_p: 0.5,
            webhook_modes: vec![
                (SinkMode::Ok, 40),
                (SinkMode::Slow, 20),
                (SinkMode::Fail, 15),
                (SinkMode::Flaky, 15),
                (SinkMode::Dead, 10),
            ],
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ctx.constant(ctx.params.rate, ctx.params.duration_s).await;
            Ok(())
        })
    }
    fn assess(&self, ctx: &Ctx, report: &Report, v: &mut Violations) {
        let min_fraction = ctx.params.extra_f64("min_throughput_fraction", 0.8);
        match report.throughput.finished_per_s_in_load_window {
            Some(done) if done >= min_fraction * report.load.achieved_accept_rate => {}
            Some(done) => v.add("s", "throughput_dented_by_webhooks", "hostile webhook receivers slowed transaction processing", "",
                                format!("finished {done}/s inside the load window vs accepted {}/s (need ≥ {min_fraction})", report.load.achieved_accept_rate)),
            None => v.add("s", "throughput_unmeasurable", "no usable analytics samples inside the load window", "", ""),
        }
    }
}
