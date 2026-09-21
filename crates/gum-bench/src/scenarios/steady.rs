//! Fixed rate below capacity: the latency baseline.

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};
use crate::report::Report;
use crate::verdict::Violations;

pub struct Steady;

impl Scenario for Steady {
    fn name(&self) -> &'static str {
        "steady"
    }
    fn about(&self) -> &'static str {
        "constant rate at ~80% of capacity (10 signers, 8 jobs/s, 60 s)"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 10,
            rate: 8.0,
            duration_s: 60.0,
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
        // Below capacity the engine must keep up: finished/s inside the window ≈ accepted/s.
        let cap = ctx.params.capacity_per_chain() * ctx.params.chains as f64;
        if ctx.params.rate <= 0.9 * cap {
            if let Some(done) = report.throughput.finished_per_s_in_load_window {
                if done < 0.85 * report.load.achieved_accept_rate {
                    v.add(
                        "s",
                        "fell_behind_below_capacity",
                        "engine did not keep up with a sub-capacity offered rate",
                        "",
                        format!(
                            "finished {done}/s vs accepted {}/s (capacity {cap}/s)",
                            report.load.achieved_accept_rate
                        ),
                    );
                }
            }
        }
    }
}
