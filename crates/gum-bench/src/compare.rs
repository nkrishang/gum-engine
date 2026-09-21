//! Regression gate: `gum-bench compare <baseline.json> <new.json>`.

use crate::report::Report;

#[derive(Clone, Debug)]
pub struct CompareOpts {
    pub throughput_tolerance: f64,
    pub p99_tolerance: f64,
    /// Allowed increase of RPC calls/tx. 0 = strict (the default).
    pub rpc_tolerance: f64,
    pub ignore_latency: bool,
    pub force_latency: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    Pass,
    Fail,
    Skipped,
    Info,
}

pub struct Row {
    pub metric: String,
    pub baseline: String,
    pub new: String,
    pub change: String,
    pub limit: String,
    pub outcome: Outcome,
}

pub struct Comparison {
    pub rows: Vec<Row>,
    pub warnings: Vec<String>,
}

impl Comparison {
    pub fn failed(&self) -> bool {
        self.rows.iter().any(|r| r.outcome == Outcome::Fail)
    }

    pub fn render(&self) -> String {
        let mut table: Vec<[String; 6]> = vec![[
            "metric".into(),
            "baseline".into(),
            "new".into(),
            "change".into(),
            "limit".into(),
            "result".into(),
        ]];
        for r in &self.rows {
            let res = match r.outcome {
                Outcome::Pass => "ok",
                Outcome::Fail => "FAIL",
                Outcome::Skipped => "skipped",
                Outcome::Info => "info",
            };
            table.push([
                r.metric.clone(),
                r.baseline.clone(),
                r.new.clone(),
                r.change.clone(),
                r.limit.clone(),
                res.into(),
            ]);
        }
        let widths: Vec<usize> = (0..6)
            .map(|c| {
                table
                    .iter()
                    .map(|row| row[c].chars().count())
                    .max()
                    .unwrap_or(0)
            })
            .collect();
        let mut out = String::new();
        for (i, row) in table.iter().enumerate() {
            let cells: Vec<String> = row
                .iter()
                .enumerate()
                .map(|(c, v)| format!("{v:<w$}", w = widths[c]))
                .collect();
            out += &format!("| {} |\n", cells.join(" | "));
            if i == 0 {
                out += &format!(
                    "|{}|\n",
                    widths
                        .iter()
                        .map(|w| "-".repeat(w + 2))
                        .collect::<Vec<_>>()
                        .join("|")
                );
            }
        }
        for w in &self.warnings {
            out += &format!("warning: {w}\n");
        }
        out += if self.failed() {
            "\nRESULT: FAIL\n"
        } else {
            "\nRESULT: PASS\n"
        };
        out
    }
}

fn pct(base: f64, new: f64) -> String {
    if base == 0.0 {
        "n/a".into()
    } else {
        format!("{:+.1}%", (new - base) / base * 100.0)
    }
}

fn milli(v: f64) -> i64 {
    (v * 1000.0).round() as i64
}

pub fn compare(base: &Report, new: &Report, o: &CompareOpts) -> Comparison {
    let mut rows = Vec::new();
    let mut warnings = Vec::new();

    if base.scenario != new.scenario {
        warnings.push(format!(
            "comparing different scenarios: baseline `{}` vs new `{}`",
            base.scenario, new.scenario
        ));
    }
    if base.params != new.params {
        warnings.push(
            "scenario parameters differ between the two runs; numbers may not be comparable".into(),
        );
    }

    // 1. correctness — always gates.
    rows.push(Row {
        metric: "correctness violations (new run)".into(),
        baseline: base.verdict.violations.len().to_string(),
        new: new.verdict.violations.len().to_string(),
        change: String::new(),
        limit: "0".into(),
        outcome: if new.verdict.pass && new.verdict.violations.is_empty() {
            Outcome::Pass
        } else {
            Outcome::Fail
        },
    });

    // 2. RPC calls/tx — machine independent, compared to 3 decimals.
    let (b, n) = (base.rpc.calls_per_tx, new.rpc.calls_per_tx);
    let comparable = base.rpc.source == new.rpc.source && base.rpc.source != "unavailable";
    let limit_milli = milli(b * (1.0 + o.rpc_tolerance));
    rows.push(Row {
        metric: "rpc calls/tx".into(),
        baseline: format!("{b:.3}"),
        new: format!("{n:.3}"),
        change: pct(b, n),
        limit: if o.rpc_tolerance == 0.0 {
            "no increase".into()
        } else {
            format!("+{:.1}%", o.rpc_tolerance * 100.0)
        },
        outcome: if !comparable {
            Outcome::Skipped
        } else if milli(n) > limit_milli {
            Outcome::Fail
        } else {
            Outcome::Pass
        },
    });
    if !comparable {
        warnings.push(format!(
            "rpc calls/tx not comparable (sources: `{}` vs `{}`)",
            base.rpc.source, new.rpc.source
        ));
    }
    let mut methods: Vec<&String> = base
        .rpc
        .by_method
        .keys()
        .chain(new.rpc.by_method.keys())
        .collect();
    methods.sort();
    methods.dedup();
    for m in methods {
        let b = base
            .rpc
            .by_method
            .get(m)
            .map(|x| x.calls_per_tx)
            .unwrap_or(0.0);
        let n = new
            .rpc
            .by_method
            .get(m)
            .map(|x| x.calls_per_tx)
            .unwrap_or(0.0);
        if milli(b) != milli(n) {
            rows.push(Row {
                metric: format!("  {m} /tx"),
                baseline: format!("{b:.3}"),
                new: format!("{n:.3}"),
                change: pct(b, n),
                limit: String::new(),
                outcome: Outcome::Info,
            });
        }
    }

    // 3/4. throughput + p99 — only between equal machine fingerprints.
    let same_machine = match (&base.machine, &new.machine) {
        (Some(a), Some(b)) => a.fingerprint == b.fingerprint,
        _ => false,
    };
    let gate_perf = (same_machine || o.force_latency) && !o.ignore_latency;
    if o.ignore_latency {
        warnings.push("throughput/latency gates skipped (--ignore-latency)".into());
    } else if !same_machine && !o.force_latency {
        warnings.push(format!(
            "machine fingerprints differ (`{}` vs `{}`): throughput/latency gates skipped (use --force-latency to apply them anyway)",
            base.machine.as_ref().map(|m| m.fingerprint.as_str()).unwrap_or("?"),
            new.machine.as_ref().map(|m| m.fingerprint.as_str()).unwrap_or("?"),
        ));
    }

    let (b, n) = (
        base.throughput.confirmed_per_s,
        new.throughput.confirmed_per_s,
    );
    rows.push(Row {
        metric: "throughput (confirmed jobs/s)".into(),
        baseline: format!("{b:.3}"),
        new: format!("{n:.3}"),
        change: pct(b, n),
        limit: format!("-{:.1}%", o.throughput_tolerance * 100.0),
        outcome: if !gate_perf {
            Outcome::Skipped
        } else if n < b * (1.0 - o.throughput_tolerance) {
            Outcome::Fail
        } else {
            Outcome::Pass
        },
    });

    // A percentile is only a stable statistic when enough samples sit above it. With fewer than
    // MIN_SAMPLES_FOR_P99 samples the p99 is decided by one or two requests (at n = 200 it is the
    // second-worst), so gating on it just measures machine noise. Small runs gate on p90 instead and
    // report p99 for information.
    const MIN_SAMPLES_FOR_P99: u64 = 1_000;
    for (label, base_l, new_l) in [
        (
            "accept latency",
            &base.latency_ms.accept,
            &new.latency_ms.accept,
        ),
        (
            "accept→included",
            &base.latency_ms.accept_to_included,
            &new.latency_ms.accept_to_included,
        ),
    ] {
        let enough = base_l.count >= MIN_SAMPLES_FOR_P99 && new_l.count >= MIN_SAMPLES_FOR_P99;
        let (gated, b, n) = if enough {
            ("p99", base_l.p99, new_l.p99)
        } else {
            ("p90", base_l.p90, new_l.p90)
        };
        rows.push(Row {
            metric: format!("{gated} {label} (ms)"),
            baseline: format!("{b:.3}"),
            new: format!("{n:.3}"),
            change: pct(b, n),
            limit: format!("+{:.1}%", o.p99_tolerance * 100.0),
            outcome: if !gate_perf {
                Outcome::Skipped
            } else if n > b * (1.0 + o.p99_tolerance) {
                Outcome::Fail
            } else {
                Outcome::Pass
            },
        });
        if !enough {
            rows.push(Row {
                metric: format!("  p99 {label} (ms) — n={} is too few to gate", new_l.count),
                baseline: format!("{:.3}", base_l.p99),
                new: format!("{:.3}", new_l.p99),
                change: pct(base_l.p99, new_l.p99),
                limit: String::new(),
                outcome: Outcome::Info,
            });
        }
    }
    for (name, b, n) in [
        (
            "p50 accept latency (ms)",
            base.latency_ms.accept.p50,
            new.latency_ms.accept.p50,
        ),
        (
            "p99 accept→confirmed webhook (ms)",
            base.latency_ms.accept_to_confirmed_webhook.p99,
            new.latency_ms.accept_to_confirmed_webhook.p99,
        ),
        (
            "engine CPU avg (%)",
            base.engine_process.cpu_pct_avg,
            new.engine_process.cpu_pct_avg,
        ),
        (
            "engine RSS max (MB)",
            base.engine_process.rss_mb_max,
            new.engine_process.rss_mb_max,
        ),
    ] {
        rows.push(Row {
            metric: name.into(),
            baseline: format!("{b:.3}"),
            new: format!("{n:.3}"),
            change: pct(b, n),
            limit: String::new(),
            outcome: Outcome::Info,
        });
    }

    Comparison { rows, warnings }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::Machine;
    use crate::verdict::Violation;

    fn report(calls_per_tx: f64, tput: f64, p99: f64) -> Report {
        let mut r = Report {
            scenario: "smoke".into(),
            ..Default::default()
        };
        r.verdict.pass = true;
        r.rpc.source = "proxy".into();
        r.rpc.calls_per_tx = calls_per_tx;
        r.throughput.confirmed_per_s = tput;
        // Enough samples for the p99 gate to apply (see MIN_SAMPLES_FOR_P99).
        r.latency_ms.accept.count = 5_000;
        r.latency_ms.accept.p99 = p99;
        r.latency_ms.accept.p90 = p99 / 2.0;
        r.latency_ms.accept_to_included.count = 5_000;
        r.latency_ms.accept_to_included.p99 = 1000.0;
        r.latency_ms.accept_to_included.p90 = 500.0;
        r.machine = Some(Machine {
            os: "x".into(),
            os_version: "1".into(),
            arch: "a".into(),
            cpu_model: "c".into(),
            cores: 8,
            ram_bytes: 1,
            fingerprint: "fp".into(),
        });
        r
    }

    fn opts() -> CompareOpts {
        CompareOpts {
            throughput_tolerance: 0.05,
            p99_tolerance: 0.10,
            rpc_tolerance: 0.0,
            ignore_latency: false,
            force_latency: false,
        }
    }

    #[test]
    fn identical_reports_pass() {
        let a = report(1.5, 5.0, 3.0);
        assert!(!compare(&a, &a.clone(), &opts()).failed());
    }

    #[test]
    fn rpc_increase_is_strict_to_three_decimals() {
        let a = report(1.500, 5.0, 3.0);
        assert!(compare(&a, &report(1.501, 5.0, 3.0), &opts()).failed());
        assert!(!compare(&a, &report(1.5004, 5.0, 3.0), &opts()).failed());
        assert!(!compare(&a, &report(1.2, 5.0, 3.0), &opts()).failed());
    }

    #[test]
    fn throughput_and_p99_tolerances() {
        let a = report(1.5, 100.0, 10.0);
        assert!(!compare(&a, &report(1.5, 95.5, 10.9), &opts()).failed());
        assert!(compare(&a, &report(1.5, 94.0, 10.0), &opts()).failed());
        assert!(compare(&a, &report(1.5, 100.0, 11.5), &opts()).failed());
    }

    #[test]
    fn small_samples_gate_on_p90_not_p99() {
        let small = |p90: f64, p99: f64| {
            let mut r = report(1.5, 100.0, p99);
            r.latency_ms.accept.count = 200;
            r.latency_ms.accept.p90 = p90;
            r
        };
        let base = small(8.0, 12.0);
        // At n = 200 the p99 is one or two requests: a swing there is noise and must not fail the run…
        assert!(!compare(&base, &small(8.2, 20.0), &opts()).failed());
        // …but a real shift of the distribution shows up in p90 and does.
        assert!(compare(&base, &small(9.5, 12.0), &opts()).failed());
    }

    #[test]
    fn violations_always_fail_and_machines_gate_latency() {
        let a = report(1.5, 100.0, 10.0);
        let mut bad = a.clone();
        bad.verdict.pass = false;
        bad.verdict.violations.push(Violation {
            check: "b".into(),
            code: "x".into(),
            message: String::new(),
            count: 1,
            job_ids: vec![],
            examples: vec![],
        });
        assert!(compare(&a, &bad, &opts()).failed());

        let mut other = report(1.5, 50.0, 50.0);
        other.machine.as_mut().unwrap().fingerprint = "different".into();
        let c = compare(&a, &other, &opts());
        assert!(!c.failed(), "perf gates must be skipped across machines");
        assert!(c.warnings.iter().any(|w| w.contains("fingerprints differ")));
        let forced = compare(
            &a,
            &other,
            &CompareOpts {
                force_latency: true,
                ..opts()
            },
        );
        assert!(forced.failed());
        let ignored = compare(
            &a,
            &report(1.5, 50.0, 50.0),
            &CompareOpts {
                ignore_latency: true,
                ..opts()
            },
        );
        assert!(!ignored.failed());
    }
}
