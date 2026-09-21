//! kill -9 the engine mid-load (2–3 times), stop Anvil for ~10 s, optionally restart Postgres.
//! Every job carries an Idempotency-Key and the client retries on transport errors, exactly like a
//! production consumer would — so nothing the engine acknowledged (or half-acknowledged) may be lost
//! or executed twice. extra: kills (3), anvil_outage_secs (10).

use std::time::Duration;

use anyhow::{Context, Result};
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};

pub struct Chaos;

impl Scenario for Chaos {
    fn name(&self) -> &'static str {
        "chaos"
    }
    fn about(&self) -> &'static str {
        "kill -9 + restart the engine 3× mid-load, stop Anvil ~10 s and restart it; verdict must stay clean"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 5,
            rate: 3.0,
            duration_s: 75.0,
            idem_fraction: 1.0,
            replay_fraction: 0.1,
            retry_with_idem: true,
            ..Default::default()
        }
    }
    fn needs_process_control(&self) -> bool {
        true
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            let d = ctx.params.duration_s;
            let kills = ctx.params.extra_f64("kills", 3.0) as usize;
            let outage = ctx.params.extra_f64("anvil_outage_secs", 10.0);
            let t0 = ctx.clock.now().as_secs_f64();
            let script = async {
                // kills in the first 60% of the run, the Anvil outage at 70%.
                for i in 0..kills {
                    let at = d * 0.6 * (i + 1) as f64 / (kills + 1) as f64;
                    let now = ctx.clock.now().as_secs_f64();
                    tokio::time::sleep(Duration::from_secs_f64((at - (now - t0)).max(0.0))).await;
                    ctx.note(format!("chaos: kill -9 engine (#{})", i + 1));
                    ctx.rig.kill_engine().await.context("kill -9 engine")?;
                    tokio::time::sleep(Duration::from_millis(1000)).await;
                    ctx.rig
                        .restart_engine()
                        .await
                        .context("engine did not come back after kill -9")?;
                    ctx.note("chaos: engine is ready again");
                }
                let now = ctx.clock.now().as_secs_f64() - t0;
                tokio::time::sleep(Duration::from_secs_f64((d * 0.7 - now).max(0.0))).await;
                ctx.note(format!(
                    "chaos: stopping anvil (chain {}) for {outage:.0} s",
                    ctx.rig.chains[0].chain_id
                ));
                ctx.rig.stop_anvil(0).await?;
                tokio::time::sleep(Duration::from_secs_f64(outage)).await;
                ctx.rig.start_anvil(0).await?;
                ctx.note("chaos: anvil is back");
                if ctx.rig.restart_postgres().await? {
                    ctx.note("chaos: postgres container restarted");
                } else {
                    ctx.note("chaos: postgres restart skipped (no --postgres-container)");
                }
                anyhow::Ok(())
            };
            let load = async {
                ctx.constant(ctx.params.rate, d).await;
                anyhow::Ok(())
            };
            let (a, b) = tokio::join!(script, load);
            a?;
            b?;
            ctx.metric("engine_kills", kills as u64);
            Ok(())
        })
    }
}
