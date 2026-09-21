//! RPC provider misbehaviour, injected by the counting proxy. The correctness verdict must stay clean —
//! in particular across "drop response after forward" on the send method (indeterminate sends).

use std::time::{Duration, Instant};

use anyhow::Result;
use futures::future::BoxFuture;

use super::{Ctx, Params, Scenario};
use crate::proxy::{FaultKind, FaultRule};

pub struct RpcFaults;

const SEND_METHODS: [&str; 2] = ["eth_sendRawTransactionSync", "eth_sendRawTransaction"];

impl Scenario for RpcFaults {
    fn name(&self) -> &'static str {
        "rpc-faults"
    }
    fn about(&self) -> &'static str {
        "429 / -32007 bursts, 5xx bursts, dropped responses on send, slow-then-closed, full blackhole window"
    }
    fn defaults(&self) -> Params {
        Params {
            chains: 1,
            signers: 5,
            rate: 3.0,
            duration_s: 60.0,
            ..Default::default()
        }
    }
    fn drive<'a>(&'a self, ctx: &'a Ctx) -> BoxFuture<'a, Result<()>> {
        Box::pin(async move {
            // Windows are fractions of the load duration so --duration scales the whole script.
            let d = ctx.params.duration_s;
            let t0 = Instant::now();
            let at = |f: f64| t0 + Duration::from_secs_f64(d * f);
            let all = |kind: FaultKind, p: f64, from: f64, until: f64| FaultRule {
                methods: None,
                kind,
                probability: p,
                from: at(from),
                until: at(until),
            };
            let send = |kind: FaultKind, p: f64, from: f64, until: f64| FaultRule {
                methods: Some(SEND_METHODS.iter().map(|s| s.to_string()).collect()),
                kind,
                probability: p,
                from: at(from),
                until: at(until),
            };
            let script = vec![
                all(FaultKind::Http429, 0.5, 0.08, 0.17),
                all(FaultKind::RpcError32007, 0.5, 0.17, 0.22),
                all(FaultKind::Http500, 0.5, 0.27, 0.33),
                all(FaultKind::Http503, 0.5, 0.33, 0.38),
                send(FaultKind::DropAfterForward, 0.35, 0.43, 0.60),
                all(FaultKind::DelayThenClose { secs: 3.0 }, 0.3, 0.65, 0.72),
                all(FaultKind::Blackhole, 1.0, 0.78, 0.90),
            ];
            for c in &ctx.rig.chains {
                if let Some(p) = &c.proxy {
                    for r in &script {
                        p.add_fault(r.clone());
                    }
                }
            }
            ctx.note("rpc-faults: fault script armed (429, -32007, 500, 503, drop-after-forward on send, delay-then-close, blackhole)");
            ctx.constant(ctx.params.rate, d).await;
            Ok(())
        })
    }
}
