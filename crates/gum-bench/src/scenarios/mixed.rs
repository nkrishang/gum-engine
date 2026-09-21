//! 2–3 chains at once, every job kind, offered slightly above capacity on every chain.
//! Asserts that a signer busy on chain A still serves chain B: each chain must individually reach
//! (most of) its own capacity. If pairs blocked each other, per-chain throughput would be capacity/chains.

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};
use crate::report::Report;
use crate::verdict::Violations;

pub struct Mixed;

impl Scenario for Mixed {
    fn name(&self) -> &'static str {
        "mixed"
    }
    fn about(&self) -> &'static str {
        "3 chains, all job kinds, offered ≈1.3× capacity per chain; asserts per-chain throughput ≈ per-chain capacity"
    }
    fn defaults(&self) -> Params {
        // 5 signers × 3 chains = 15 tx/s capacity; rate is total across chains (round-robin).
        Params {
            chains: 3,
            signers: 5,
            rate: 19.5,
            duration_s: 40.0,
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
        let cap = ctx.params.capacity_per_chain();
        let min_fraction = ctx.params.extra_f64("min_capacity_fraction", 0.75);
        let offered_per_chain = ctx.params.rate / ctx.params.chains as f64;
        let want = min_fraction * cap.min(offered_per_chain * 0.85); // ~7% of the mix never goes on-chain
        let by_chain = &report.throughput.on_chain_per_s_in_load_window_by_chain;
        if by_chain.len() != ctx.params.chains {
            v.add(
                "s",
                "per_chain_throughput_unavailable",
                "analytics by_chain did not cover every chain during the load window",
                "",
                format!(
                    "got chains {:?}, expected {}",
                    by_chain.keys().collect::<Vec<_>>(),
                    ctx.params.chains
                ),
            );
        }
        for (chain, rate) in by_chain {
            if *rate < want {
                v.add("s", "chain_starved", "a chain ran well below its own capacity while other chains were busy (cross-chain blocking?)", "",
                      format!("chain {chain}: {rate:.2} on-chain jobs/s, expected ≥ {want:.2} (capacity {cap:.2})"));
            }
        }
    }
}
