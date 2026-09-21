//! Small shared helpers: clocks, ports, fd limits, git + machine fingerprint, histogram summaries.

use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hdrhistogram::Histogram;
use serde::{Deserialize, Serialize};

/// Run clock: every harness-side measurement is a monotonic offset from `start`.
/// `start_wall` is only used to label the report and to map offsets to wall time for humans.
#[derive(Clone, Copy, Debug)]
pub struct Clock {
    pub start: Instant,
    pub start_wall: DateTime<Utc>,
}

impl Clock {
    pub fn new() -> Self {
        Self {
            start: Instant::now(),
            start_wall: Utc::now(),
        }
    }
    pub fn now(&self) -> Duration {
        self.start.elapsed()
    }
    pub fn at(&self, i: Instant) -> Duration {
        i.saturating_duration_since(self.start)
    }
}

/// Ask the OS for a currently-free TCP port on localhost.
pub fn free_port() -> Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0").context("binding an ephemeral port")?;
    Ok(l.local_addr()?.port())
}

/// macOS defaults to 256 fds, far too low for an open-loop generator. Children inherit the limit.
pub fn raise_nofile_limit() {
    unsafe {
        let mut lim = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) == 0 {
            let want: libc::rlim_t = 10_240;
            let target = if lim.rlim_max == libc::RLIM_INFINITY {
                want
            } else {
                want.min(lim.rlim_max)
            };
            if lim.rlim_cur < target {
                lim.rlim_cur = target;
                let _ = libc::setrlimit(libc::RLIMIT_NOFILE, &lim);
            }
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GitInfo {
    pub sha: String,
    pub dirty: bool,
}

pub fn git_info() -> GitInfo {
    let run = |args: &[&str]| -> Option<String> {
        let out = std::process::Command::new("git").args(args).output().ok()?;
        out.status
            .success()
            .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
    };
    match run(&["rev-parse", "--short", "HEAD"]) {
        Some(sha) if !sha.is_empty() => {
            let dirty = run(&["status", "--porcelain"])
                .map(|s| !s.is_empty())
                .unwrap_or(true);
            GitInfo { sha, dirty }
        }
        _ => GitInfo {
            sha: "nogit".into(),
            dirty: true,
        },
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Machine {
    pub os: String,
    pub os_version: String,
    pub arch: String,
    pub cpu_model: String,
    pub cores: usize,
    pub ram_bytes: u64,
    /// `<os>/<arch>/<cpu model>/<cores>/<ram GiB>` — latency gates only apply between equal fingerprints.
    pub fingerprint: String,
}

pub fn machine() -> Machine {
    use sysinfo::{CpuRefreshKind, MemoryRefreshKind, RefreshKind, System};
    let sys = System::new_with_specifics(
        RefreshKind::nothing()
            .with_cpu(CpuRefreshKind::everything())
            .with_memory(MemoryRefreshKind::everything()),
    );
    let cpu_model = sys
        .cpus()
        .first()
        .map(|c| c.brand().trim().to_string())
        .unwrap_or_default();
    let cores = sys.cpus().len();
    let ram_bytes = sys.total_memory();
    let os = System::name().unwrap_or_else(|| std::env::consts::OS.to_string());
    let os_version = System::os_version().unwrap_or_default();
    let arch = std::env::consts::ARCH.to_string();
    let fingerprint = format!(
        "{os}/{arch}/{cpu_model}/{cores}c/{}GiB",
        (ram_bytes as f64 / (1u64 << 30) as f64).round()
    );
    Machine {
        os,
        os_version,
        arch,
        cpu_model,
        cores,
        ram_bytes,
        fingerprint,
    }
}

/// Latency histogram in microseconds, 3 significant digits, up to one hour.
pub fn new_hist() -> Histogram<u64> {
    Histogram::<u64>::new_with_bounds(1, 3_600_000_000, 3).expect("static histogram bounds")
}

pub fn record(h: &mut Histogram<u64>, d: Duration) {
    h.saturating_record((d.as_micros() as u64).max(1));
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Percentiles {
    pub count: u64,
    pub min: f64,
    pub mean: f64,
    pub p50: f64,
    pub p90: f64,
    pub p95: f64,
    pub p99: f64,
    pub p999: f64,
    pub max: f64,
}

/// Summarise a µs histogram as milliseconds.
pub fn percentiles_ms(h: &Histogram<u64>) -> Percentiles {
    if h.is_empty() {
        return Percentiles::default();
    }
    let ms = |v: u64| (v as f64 / 1000.0 * 1000.0).round() / 1000.0;
    Percentiles {
        count: h.len(),
        min: ms(h.min()),
        mean: (h.mean() / 1000.0 * 1000.0).round() / 1000.0,
        p50: ms(h.value_at_quantile(0.50)),
        p90: ms(h.value_at_quantile(0.90)),
        p95: ms(h.value_at_quantile(0.95)),
        p99: ms(h.value_at_quantile(0.99)),
        p999: ms(h.value_at_quantile(0.999)),
        max: ms(h.max()),
    }
}

pub fn round3(v: f64) -> f64 {
    (v * 1000.0).round() / 1000.0
}

/// Last `n` lines of a (log) file, for error context.
pub fn tail_file(path: &std::path::Path, n: usize) -> String {
    match std::fs::read_to_string(path) {
        Ok(s) => {
            let lines: Vec<&str> = s.lines().collect();
            lines[lines.len().saturating_sub(n)..].join("\n")
        }
        Err(e) => format!("<could not read {}: {e}>", path.display()),
    }
}

/// Walk up from the cwd to the workspace root (the directory holding `bench/` and `Cargo.toml`).
pub fn workspace_root() -> std::path::PathBuf {
    let cwd = std::env::current_dir().unwrap_or_else(|_| ".".into());
    let mut dir = cwd.as_path();
    loop {
        if dir.join("bench").is_dir() && dir.join("Cargo.toml").is_file() {
            return dir.to_path_buf();
        }
        match dir.parent() {
            Some(p) => dir = p,
            None => return cwd,
        }
    }
}
