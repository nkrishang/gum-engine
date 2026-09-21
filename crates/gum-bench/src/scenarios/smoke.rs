//! CI gate: small, mixed, deterministic. ~10 jobs/s offered for ~20 s against 1 chain / 5 signers.
//! Offered load is above the chain's capacity (5 tx/s at 1 s blocks) on purpose: the backlog makes the
//! throughput number equal to capacity, which is stable across machines.

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};

pub struct Smoke;

impl Scenario for Smoke {
    fn name(&self) -> &'static str {
        "smoke"
    }
    fn about(&self) -> &'static str {
        "1 chain, 5 signers, ~10 jobs/s for ~20 s, every job kind, idempotency replays"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 5,
            rate: 10.0,
            duration_s: 20.0,
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            ctx.constant(ctx.params.rate, ctx.params.duration_s).await;
            Ok(())
        })
    }
}
