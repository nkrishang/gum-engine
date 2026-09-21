//! Harness self-checks: the verdict must be clean against the clean fake engine and must FAIL, with the
//! right violation, against each deliberately broken fake.
//!
//! These tests need `anvil` on PATH and a reachable Postgres (`GUM_BENCH_POSTGRES_URL`, default
//! `postgres://localhost:5432/postgres`). Without them they skip with a message instead of failing.

use std::path::{Path, PathBuf};
use std::process::Command;

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_gum-bench");

fn anvil_on_path() -> bool {
    std::env::var_os("PATH")
        .map(|p| std::env::split_paths(&p).any(|d| d.join("anvil").is_file()))
        .unwrap_or(false)
}

struct Run {
    code: i32,
    report: Option<Value>,
    report_path: Option<PathBuf>,
    stderr: String,
}

/// Returns `None` when the environment cannot run the rig (test should be skipped).
fn run_smoke(name: &str, extra: &[&str]) -> Option<Run> {
    if !anvil_on_path() {
        eprintln!("SKIP {name}: `anvil` is not on PATH (install foundry to run the gum-bench self-checks)");
        return None;
    }
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("selfcheck-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let out = Command::new(BIN)
        .args([
            "run",
            "smoke",
            "--duration",
            "8",
            "--quiet",
            "--drain-stall-timeout",
            "12",
            "--webhook-wait",
            "5",
            "--settle-timeout",
            "5",
        ])
        .arg("--results-dir")
        .arg(&dir)
        .args(extra)
        .output()
        .expect("spawning gum-bench");
    let stderr = String::from_utf8_lossy(&out.stderr).to_string();
    let code = out.status.code().unwrap_or(-1);
    if code == 2 && stderr.contains("connecting to Postgres") {
        eprintln!("SKIP {name}: Postgres is not reachable (set GUM_BENCH_POSTGRES_URL)\n{stderr}");
        return None;
    }
    let report_path = std::fs::read_dir(&dir).ok().and_then(|rd| {
        rd.filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| p.extension().map(|x| x == "json").unwrap_or(false))
    });
    let report = report_path.as_ref().map(|p| {
        serde_json::from_slice(&std::fs::read(p).expect("reading report")).expect("report is JSON")
    });
    Some(Run {
        code,
        report,
        report_path,
        stderr,
    })
}

fn violation_codes(report: &Value) -> Vec<String> {
    report["verdict"]["violations"]
        .as_array()
        .map(|a| {
            a.iter()
                .map(|v| {
                    format!(
                        "{}:{}",
                        v["check"].as_str().unwrap_or("?"),
                        v["code"].as_str().unwrap_or("?")
                    )
                })
                .collect()
        })
        .unwrap_or_default()
}

fn expect_bug_caught(bug: &str, expected: &str) {
    let Some(run) = run_smoke(bug, &["--fake-bug", bug]) else {
        return;
    };
    let report = run
        .report
        .unwrap_or_else(|| panic!("no report written for bug {bug}; stderr:\n{}", run.stderr));
    let codes = violation_codes(&report);
    assert_eq!(
        run.code, 1,
        "a run with violations must exit 1 (bug {bug}); stderr:\n{}",
        run.stderr
    );
    assert_eq!(report["verdict"]["pass"], false);
    assert!(
        codes.iter().any(|c| c == expected),
        "bug {bug}: expected violation {expected}, got {codes:?}"
    );
    let v = report["verdict"]["violations"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| {
            format!(
                "{}:{}",
                v["check"].as_str().unwrap(),
                v["code"].as_str().unwrap()
            ) == expected
        })
        .unwrap();
    assert!(v["count"].as_u64().unwrap() >= 1);
    assert!(
        !v["job_ids"].as_array().unwrap().is_empty(),
        "violation {expected} must name the offending jobs"
    );
}

#[test]
fn clean_fake_engine_passes_smoke_and_compare_gates_work() {
    let Some(run) = run_smoke("clean", &["--fake-engine"]) else {
        return;
    };
    let report = run
        .report
        .clone()
        .unwrap_or_else(|| panic!("no report written; stderr:\n{}", run.stderr));
    assert_eq!(
        run.code,
        0,
        "clean fake must pass; violations: {:?}\nstderr:\n{}",
        violation_codes(&report),
        run.stderr
    );
    assert_eq!(report["verdict"]["pass"], true);
    assert_eq!(report["schema_version"], 1);
    assert_eq!(report["load"]["offered_jobs"], 80);
    assert_eq!(report["load"]["accepted_jobs"], 80);
    assert_eq!(report["jobs"]["non_terminal"], 0);
    assert_eq!(report["rpc"]["source"], "proxy");
    let sends = report["rpc"]["by_method"]["eth_sendRawTransactionSync"]["calls"]
        .as_u64()
        .unwrap();
    let on_chain = report["jobs"]["confirmed_success"].as_u64().unwrap()
        + report["jobs"]["confirmed_reverted"].as_u64().unwrap();
    assert_eq!(
        sends, on_chain,
        "proxy must count exactly one send per on-chain job for the fake engine"
    );
    assert!(report["latency_ms"]["accept"]["count"].as_u64().unwrap() == 80);
    assert!(
        report["latency_ms"]["accept_to_confirmed_webhook"]["count"]
            .as_u64()
            .unwrap()
            == on_chain
    );
    assert!(report["throughput"]["confirmed_per_s"].as_f64().unwrap() > 2.0);

    // compare: a report always passes against itself …
    let path = run.report_path.unwrap();
    let ok = Command::new(BIN)
        .arg("compare")
        .arg(&path)
        .arg(&path)
        .output()
        .unwrap();
    assert_eq!(
        ok.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&ok.stdout)
    );

    // … fails when RPC calls/tx went up …
    let mut worse = report.clone();
    worse["rpc"]["calls_per_tx"] =
        Value::from(report["rpc"]["calls_per_tx"].as_f64().unwrap() + 0.001);
    let worse_path = path.with_file_name("worse-rpc.json");
    std::fs::write(&worse_path, serde_json::to_vec(&worse).unwrap()).unwrap();
    let bad = Command::new(BIN)
        .arg("compare")
        .arg(&path)
        .arg(&worse_path)
        .output()
        .unwrap();
    assert_eq!(
        bad.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&bad.stdout)
    );

    // … fails when throughput dropped by more than the tolerance, unless latency gates are ignored.
    let mut slower = report.clone();
    slower["throughput"]["confirmed_per_s"] =
        Value::from(report["throughput"]["confirmed_per_s"].as_f64().unwrap() * 0.9);
    let slower_path = path.with_file_name("slower.json");
    std::fs::write(&slower_path, serde_json::to_vec(&slower).unwrap()).unwrap();
    let bad = Command::new(BIN)
        .arg("compare")
        .arg(&path)
        .arg(&slower_path)
        .output()
        .unwrap();
    assert_eq!(
        bad.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&bad.stdout)
    );
    let ignored = Command::new(BIN)
        .arg("compare")
        .arg(&path)
        .arg(&slower_path)
        .arg("--ignore-latency")
        .output()
        .unwrap();
    assert_eq!(
        ignored.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&ignored.stdout)
    );
}

#[test]
fn double_send_is_caught_by_the_chain_oracle() {
    expect_bug_caught("double-send", "b:executed_more_than_once");
}

#[test]
fn skipped_webhook_is_caught() {
    expect_bug_caught("skip-webhook", "d:missing_confirmed_webhook");
}

#[test]
fn lost_job_is_caught() {
    expect_bug_caught("lose-job", "a:not_terminal");
}

#[test]
fn bad_signature_is_caught() {
    expect_bug_caught("bad-signature", "d:invalid_signature");
}

#[test]
fn wrong_analytics_is_caught() {
    let Some(run) = run_smoke("wrong-analytics", &["--fake-bug", "wrong-analytics"]) else {
        return;
    };
    let report = run.report.expect("report");
    assert_eq!(run.code, 1);
    assert!(
        violation_codes(&report)
            .iter()
            .any(|c| c == "e:analytics_mismatch"),
        "{:?}",
        violation_codes(&report)
    );
}
