//! Long steady run. `--duration` sets the length (default 10 minutes).

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};
use crate::report::Report;
use crate::verdict::Violations;

pub struct Soak;

impl Scenario for Soak {
    fn name(&self) -> &'static str {
        "soak"
    }
    fn about(&self) -> &'static str {
        "long steady run (default 600 s at 8 jobs/s, 10 signers); watches RSS growth"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 10,
            rate: 8.0,
            duration_s: 600.0,
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // One-minute segments so progress is visible and ctrl-c style stops take effect quickly.
            let mut left = ctx.params.duration_s;
            while left > 0.0 {
                let seg = left.min(60.0);
                ctx.constant(ctx.params.rate, seg).await;
                left -= seg;
                ctx.note(format!(
                    "soak: {:.0} s left, {} accepted so far",
                    left,
                    ctx.load.accepted_so_far()
                ));
            }
            Ok(())
        })
    }
    fn assess(&self, _ctx: &Ctx, report: &Report, _v: &mut Violations) {
        let _ = report; // RSS growth is reported via engine_process; no hard gate (allocator behaviour varies).
    }
}
