//! Global, priority-aware RPC rate limiter.
//!
//! One token bucket covers every chain (provider limits are per account). Three lanes share it:
//! sends first, then reads the pipeline is waiting on, then background work. Strict priority alone would
//! let a saturated send lane starve confirmations and balance sweeps forever, so the two lower lanes each
//! accrue a small guaranteed share; whatever they do not use stays available to everyone
//! (work-conserving). Providers publish no `Retry-After`, so throttling responses shrink the rate
//! multiplicatively and it recovers additively (AIMD).

use std::{
    collections::VecDeque,
    sync::atomic::{AtomicU64, Ordering},
    sync::Arc,
    time::Duration,
};

use tokio::{
    sync::{mpsc, oneshot},
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lane {
    /// Broadcasts and rebroadcasts.
    Send = 0,
    /// Estimates, receipts, confirmation checks, recovery reads.
    Read = 1,
    /// Health probes and balance sweeps.
    Background = 2,
}

struct Waiter {
    grant: oneshot::Sender<()>,
}

pub struct Limiter {
    lanes: [mpsc::UnboundedSender<Waiter>; 3],
    penalties: Arc<AtomicU64>,
    granted: [AtomicU64; 3],
}

impl Limiter {
    pub fn spawn(rate_per_sec: u32, reserved_read: u32, reserved_background: u32) -> Arc<Self> {
        let (tx0, rx0) = mpsc::unbounded_channel();
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();
        let penalties = Arc::new(AtomicU64::new(0));
        let limiter = Arc::new(Self { lanes: [tx0, tx1, tx2], penalties: penalties.clone(), granted: [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)] });
        let dispatcher = Dispatcher {
            rate: rate_per_sec.max(1) as f64,
            reserved: [0.0, reserved_read as f64, reserved_background as f64],
            rx: [rx0, rx1, rx2],
            queues: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
            penalties,
            seen_penalties: 0,
            factor: 1.0,
            owner: Arc::downgrade(&limiter),
        };
        tokio::spawn(dispatcher.run());
        limiter
    }

    /// Waits for permission to issue one request on `lane`.
    pub async fn acquire(&self, lane: Lane) {
        let (grant, granted) = oneshot::channel();
        if self.lanes[lane as usize].send(Waiter { grant }).is_err() {
            return; // dispatcher gone (shutdown): do not block callers
        }
        let _ = granted.await;
    }

    /// The provider throttled us: halve the rate (floor 25%); it recovers on its own.
    pub fn penalize(&self) {
        self.penalties.fetch_add(1, Ordering::Relaxed);
    }

    pub fn granted(&self, lane: Lane) -> u64 {
        self.granted[lane as usize].load(Ordering::Relaxed)
    }
}

struct Dispatcher {
    rate: f64,
    reserved: [f64; 3],
    rx: [mpsc::UnboundedReceiver<Waiter>; 3],
    queues: [VecDeque<Waiter>; 3],
    penalties: Arc<AtomicU64>,
    seen_penalties: u64,
    factor: f64,
    owner: std::sync::Weak<Limiter>,
}

impl Dispatcher {
    async fn run(mut self) {
        // Allow ~200ms of burst so an idle system adds no latency to the first requests.
        let burst = (self.rate / 5.0).max(1.0);
        let mut tokens = burst;
        let mut credit = [0.0f64; 3];
        let mut last = Instant::now();

        loop {
            // Pull everything that is waiting into the local queues.
            let mut closed = 0;
            for lane in 0..3 {
                loop {
                    match self.rx[lane].try_recv() {
                        Ok(w) => self.queues[lane].push_back(w),
                        Err(mpsc::error::TryRecvError::Empty) => break,
                        Err(mpsc::error::TryRecvError::Disconnected) => {
                            closed += 1;
                            break;
                        }
                    }
                }
            }
            if closed == 3 && self.queues.iter().all(VecDeque::is_empty) {
                return;
            }

            let now = Instant::now();
            let elapsed = now.duration_since(last).as_secs_f64();
            last = now;

            let penalties = self.penalties.load(Ordering::Relaxed);
            if penalties != self.seen_penalties {
                self.seen_penalties = penalties;
                self.factor = (self.factor * 0.5).max(0.25);
                tokens = tokens.min(1.0);
            } else {
                self.factor = (self.factor + 0.05 * elapsed).min(1.0);
            }
            let rate = self.rate * self.factor;
            tokens = (tokens + elapsed * rate).min(burst);
            for (slot, reserved) in credit.iter_mut().zip(self.reserved).skip(1) {
                *slot = (*slot + elapsed * reserved * self.factor).min(reserved.max(1.0));
            }

            // Grant as many as the bucket allows.
            while tokens >= 1.0 {
                let lane = if !self.queues[1].is_empty() && credit[1] >= 1.0 {
                    1
                } else if !self.queues[2].is_empty() && credit[2] >= 1.0 {
                    2
                } else if let Some(l) = (0..3).find(|l| !self.queues[*l].is_empty()) {
                    l
                } else {
                    break;
                };
                let Some(waiter) = self.queues[lane].pop_front() else { break };
                // A dropped receiver means the caller gave up; its token is not spent.
                if waiter.grant.send(()).is_ok() {
                    tokens -= 1.0;
                    if lane > 0 {
                        credit[lane] = (credit[lane] - 1.0).max(0.0);
                    }
                    if let Some(owner) = self.owner.upgrade() {
                        owner.granted[lane].fetch_add(1, Ordering::Relaxed);
                    }
                }
            }

            let waiting = self.queues.iter().any(|q| !q.is_empty());
            if waiting {
                let wait = ((1.0 - tokens).max(0.0) / rate).clamp(0.0005, 0.25);
                tokio::time::sleep(Duration::from_secs_f64(wait)).await;
            } else {
                // Idle: park until any lane receives a request.
                let [rx0, rx1, rx2] = &mut self.rx;
                let received = tokio::select! {
                    w = rx0.recv() => w.map(|w| (0, w)),
                    w = rx1.recv() => w.map(|w| (1, w)),
                    w = rx2.recv() => w.map(|w| (2, w)),
                };
                match received {
                    Some((lane, w)) => self.queues[lane].push_back(w),
                    None => {
                        // One lane closed; if all are closed the top of the loop returns.
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn background_lane_is_not_starved_by_sends() {
        let limiter = Limiter::spawn(20, 4, 2);
        let mut handles = Vec::new();
        // Saturate the send lane.
        for _ in 0..200 {
            let l = limiter.clone();
            handles.push(tokio::spawn(async move { l.acquire(Lane::Send).await }));
        }
        for _ in 0..10 {
            let l = limiter.clone();
            handles.push(tokio::spawn(async move { l.acquire(Lane::Background).await }));
        }
        tokio::time::sleep(Duration::from_secs(4)).await;
        let background = limiter.granted(Lane::Background);
        let sends = limiter.granted(Lane::Send);
        assert!(background >= 6, "background lane starved: {background} grants");
        assert!(sends >= 40, "send lane should still dominate: {sends} grants");
        for h in handles {
            h.abort();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_limiter_grants_immediately() {
        let limiter = Limiter::spawn(50, 8, 2);
        let started = tokio::time::Instant::now();
        limiter.acquire(Lane::Send).await;
        assert!(started.elapsed() < Duration::from_millis(5));
    }
}
