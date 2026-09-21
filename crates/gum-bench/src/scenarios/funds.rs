//! Balance management.
//!  phase 1  signers start below `signer_min_balance` → the engine must top them up from the treasury;
//!  phase 2  treasury and signers are drained with `anvil_setBalance` → pairs must pause with reason
//!           InsufficientFunds and jobs must stay queued → treasury refilled → everything completes.

use std::time::{Duration, Instant};

use alloy::primitives::U256;
use alloy::providers::Provider;
use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};
use crate::api::{AnalyticsResp, SignersResp};
use crate::loadgen::JobMix;
use crate::oracle::set_balance;
use crate::rig::eth;

pub struct Funds;

impl Scenario for Funds {
    fn name(&self) -> &'static str {
        "funds"
    }
    fn about(&self) -> &'static str {
        "low signers → top-ups; drained treasury → InsufficientFunds pause + queued jobs; refill → resume"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 3,
            rate: 2.0,
            duration_s: 12.0,
            mix: JobMix::HIT_GAS_ONLY,
            initial_signer_balance_eth: Some(0.5),
            signer_min_balance_eth: 1.0,
            topup_amount_eth: 5.0,
            treasury_min_balance_eth: 100.0,
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let rig = &ctx.rig;
            let signers = rig.accounts.signer_addresses();
            let treasury = rig.accounts.treasury.address();
            let initial = eth(ctx.params.initial_signer_balance_eth.unwrap_or(0.5));

            // ---- phase 1: low signers get topped up
            ctx.note("funds: phase 1 — signers start below signer_min_balance");
            ctx.constant(ctx.params.rate, ctx.params.duration_s).await;
            if !ctx.wait_engine_idle(Duration::from_secs(90)).await {
                ctx.violation(
                    "phase1_not_drained",
                    "phase-1 jobs did not finish within 90 s",
                    "",
                );
            }
            for c in &rig.chains {
                for s in &signers {
                    let bal = c.direct.get_balance(*s).await?;
                    if bal <= initial {
                        ctx.violation(
                            "no_topup",
                            "a signer that started below signer_min_balance was never topped up",
                            format!(
                                "chain {} {s:#x}: balance {bal} (started at {initial})",
                                c.chain_id
                            ),
                        );
                    }
                }
            }

            // ---- phase 2: nobody has money
            ctx.note("funds: phase 2 — draining treasury and signers via anvil_setBalance");
            for c in &rig.chains {
                set_balance(&c.direct, treasury, U256::ZERO).await?;
                for s in &signers {
                    set_balance(&c.direct, *s, U256::ZERO).await?;
                }
            }
            ctx.constant(ctx.params.rate, ctx.params.duration_s.min(8.0))
                .await;

            let deadline = Instant::now() + Duration::from_secs(45);
            let mut paused_reason = None;
            while Instant::now() < deadline && paused_reason.is_none() {
                if let Ok(s) = rig
                    .client
                    .get_json::<SignersResp>("/v1/signers", Duration::from_secs(2))
                    .await
                {
                    paused_reason = s
                        .pairs
                        .iter()
                        .filter(|p| p.state == "paused")
                        .filter_map(|p| p.pause.as_ref())
                        .map(|p| p.reason.clone())
                        .find(|r| {
                            r.to_lowercase()
                                .replace('_', "")
                                .contains("insufficientfunds")
                        });
                }
                tokio::time::sleep(Duration::from_millis(500)).await;
            }
            match &paused_reason {
                Some(r) => ctx.note(format!("funds: pair paused with reason {r:?}")),
                None => ctx.violation("no_insufficient_funds_pause", "with an empty treasury no pair reported pause.reason InsufficientFunds on /v1/signers within 45 s", ""),
            }
            match rig
                .client
                .get_json::<AnalyticsResp>("/v1/analytics/transactions", Duration::from_secs(2))
                .await
            {
                Ok(a) => {
                    ctx.metric("queued_while_unfunded", a.totals.queued);
                    if a.totals.queued == 0 {
                        ctx.violation(
                            "jobs_not_queued_while_unfunded",
                            "jobs should stay queued while no signer can pay for gas",
                            format!("totals: {:?}", a.totals),
                        );
                    }
                }
                Err(e) => ctx.violation(
                    "analytics_unavailable",
                    "could not read analytics while unfunded",
                    format!("{e:#}"),
                ),
            }

            ctx.note("funds: refilling the treasury");
            for c in &rig.chains {
                set_balance(&c.direct, treasury, eth(10_000.0)).await?;
            }
            // The framework's drain now requires every queued job to complete.
            Ok(())
        })
    }
}
