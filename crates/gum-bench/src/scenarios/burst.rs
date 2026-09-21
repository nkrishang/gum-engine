//! Quiet base rate with periodic bursts far above capacity.
//! extra: burst_rate (default 40), burst_secs (3), period_secs (15).

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};

pub struct Burst;

impl Scenario for Burst {
    fn name(&self) -> &'static str {
        "burst"
    }
    fn about(&self) -> &'static str {
        "base 2 jobs/s with 3 s bursts of 40 jobs/s every 15 s (10 signers, 60 s)"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 10,
            rate: 2.0,
            duration_s: 60.0,
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let burst_rate = ctx.params.extra_f64("burst_rate", 40.0);
            let burst_secs = ctx.params.extra_f64("burst_secs", 3.0);
            let period = ctx
                .params
                .extra_f64("period_secs", 15.0)
                .max(burst_secs + 1.0);
            let mut left = ctx.params.duration_s;
            while left > 0.0 {
                let quiet = (period - burst_secs).min(left);
                ctx.constant(ctx.params.rate, quiet).await;
                left -= quiet;
                if left <= 0.0 {
                    break;
                }
                let b = burst_secs.min(left);
                ctx.constant(burst_rate, b).await;
                left -= b;
            }
            ctx.metric("burst_rate", burst_rate);
            Ok(())
        })
    }
}
