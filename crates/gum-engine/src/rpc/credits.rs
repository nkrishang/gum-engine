//! Per-chain RPC accounting: calls, errors and provider credits, by method.
//!
//! Providers bill per successful response, so credits are counted on success only. The meter feeds
//! `/v1/chains`, the budget alert, and the optional daily shedding cap.

use std::{
    collections::BTreeMap,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

use parking_lot::Mutex;

const WINDOW_MINUTES: usize = 60;

pub struct CreditMeter {
    credits_per_call: u32,
    calls: AtomicU64,
    errors: AtomicU64,
    credits: AtomicU64,
    inner: Mutex<Inner>,
}

struct Inner {
    by_method: BTreeMap<&'static str, u64>,
    /// Credits per minute over the last hour, for burn-rate projection.
    minutes: [u64; WINDOW_MINUTES],
    current_minute: u64,
    started: Instant,
    day: chrono::NaiveDate,
    credits_today: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CreditSnapshot {
    pub calls: u64,
    pub errors: u64,
    pub credits_used: u64,
    pub credits_today: u64,
    pub credits_projected_month: u64,
    pub by_method: BTreeMap<&'static str, u64>,
}

impl CreditMeter {
    pub fn new(credits_per_call: u32) -> Self {
        Self {
            credits_per_call,
            calls: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            credits: AtomicU64::new(0),
            inner: Mutex::new(Inner {
                by_method: BTreeMap::new(),
                minutes: [0; WINDOW_MINUTES],
                current_minute: 0,
                started: Instant::now(),
                day: chrono::Utc::now().date_naive(),
                credits_today: 0,
            }),
        }
    }

    pub fn record(&self, method: &'static str, success: bool) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        let mut inner = self.inner.lock();
        *inner.by_method.entry(method).or_insert(0) += 1;
        if !success {
            self.errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let credits = self.credits_per_call as u64;
        self.credits.fetch_add(credits, Ordering::Relaxed);

        let minute = inner.started.elapsed().as_secs() / 60;
        if minute != inner.current_minute {
            let gap = (minute - inner.current_minute).min(WINDOW_MINUTES as u64);
            for i in 1..=gap {
                let idx = ((inner.current_minute + i) % WINDOW_MINUTES as u64) as usize;
                inner.minutes[idx] = 0;
            }
            inner.current_minute = minute;
        }
        let idx = (minute % WINDOW_MINUTES as u64) as usize;
        inner.minutes[idx] += credits;

        let today = chrono::Utc::now().date_naive();
        if today != inner.day {
            inner.day = today;
            inner.credits_today = 0;
        }
        inner.credits_today += credits;
    }

    pub fn credits_today(&self) -> u64 {
        let inner = self.inner.lock();
        if chrono::Utc::now().date_naive() == inner.day {
            inner.credits_today
        } else {
            0
        }
    }

    pub fn snapshot(&self) -> CreditSnapshot {
        let inner = self.inner.lock();
        let observed = inner.started.elapsed().min(Duration::from_secs(60 * WINDOW_MINUTES as u64));
        let window_credits: u64 = inner.minutes.iter().sum();
        let per_sec = if observed.as_secs() >= 60 { window_credits as f64 / observed.as_secs_f64() } else { 0.0 };
        CreditSnapshot {
            calls: self.calls.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            credits_used: self.credits.load(Ordering::Relaxed),
            credits_today: inner.credits_today,
            credits_projected_month: (per_sec * 86_400.0 * 30.0) as u64,
            by_method: inner.by_method.clone(),
        }
    }
}
