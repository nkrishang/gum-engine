//! Per-chain in-memory job queue: a window over the durable queue in Postgres.
//!
//! Every job enters through [`JobQueue::admit`], which consults a single `known` set covering a job from
//! the moment it is loaded until it is terminal or handed back to the database. That makes double
//! enqueueing impossible regardless of who offers the job — the API handler, the boot loader or the
//! periodic sweep that picks up rows written by another instance. (The database has its own backstop:
//! binding a job compare-and-sets its status, so even a duplicate could never be sent twice.)

use std::collections::{HashSet, VecDeque};

use parking_lot::Mutex;
use tokio::sync::Notify;

use crate::domain::{JobId, QueuedJob};

pub struct JobQueue {
    inner: Mutex<Inner>,
    notify: Notify,
    /// Jobs kept in memory; anything beyond stays in Postgres until the sweep pages it in.
    window: usize,
}

struct Inner {
    jobs: VecDeque<QueuedJob>,
    known: HashSet<JobId>,
    /// Queued jobs that exist only in Postgres because the window was full when they arrived.
    overflow: u64,
}

#[derive(Debug, PartialEq, Eq)]
pub enum Admit {
    Queued,
    /// Already tracked by this process.
    Duplicate,
    /// The window is full; the job stays durable in Postgres and will be paged in later.
    Deferred,
}

impl JobQueue {
    pub fn new(window: usize) -> Self {
        Self { inner: Mutex::new(Inner { jobs: VecDeque::new(), known: HashSet::new(), overflow: 0 }), notify: Notify::new(), window: window.max(1) }
    }

    /// Offers a job to the back of the queue.
    pub fn admit(&self, job: QueuedJob) -> Admit {
        let mut inner = self.inner.lock();
        if inner.known.contains(&job.id) {
            return Admit::Duplicate;
        }
        if inner.jobs.len() >= self.window {
            inner.overflow += 1;
            return Admit::Deferred;
        }
        inner.known.insert(job.id);
        inner.jobs.push_back(job);
        drop(inner);
        self.notify.notify_one();
        Admit::Queued
    }

    /// Returns a job that was popped but not bound to the very front (it keeps its place in `known`).
    pub fn push_front(&self, job: QueuedJob) {
        let mut inner = self.inner.lock();
        inner.known.insert(job.id);
        inner.jobs.push_front(job);
        drop(inner);
        self.notify.notify_one();
    }

    /// Waits for the next job.
    pub async fn pop(&self) -> QueuedJob {
        loop {
            // Register interest before checking, so a push between the check and the await is not lost.
            let notified = self.notify.notified();
            if let Some(job) = self.try_pop() {
                return job;
            }
            notified.await;
        }
    }

    pub fn try_pop(&self) -> Option<QueuedJob> {
        let mut inner = self.inner.lock();
        let job = inner.jobs.pop_front();
        if job.is_some() && !inner.jobs.is_empty() {
            drop(inner);
            // Chain the wake-up: `notify_one` stores a single permit, and several workers may be idle.
            self.notify.notify_one();
        }
        job
    }

    /// The job reached a terminal state (or was handed back to the database): stop tracking it.
    pub fn forget(&self, id: &JobId) {
        self.inner.lock().known.remove(id);
    }

    /// Jobs waiting in memory.
    pub fn len(&self) -> usize {
        self.inner.lock().jobs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Whether the sweep should look for more rows in Postgres.
    pub fn has_room(&self) -> bool {
        self.inner.lock().jobs.len() < self.window
    }

    pub fn window(&self) -> usize {
        self.window
    }

    /// Called by the sweep after it re-read the durable queue.
    pub fn reset_overflow(&self) {
        self.inner.lock().overflow = 0;
    }

    pub fn overflow(&self) -> u64 {
        self.inner.lock().overflow
    }

    /// Removes a specific queued job (operator cancel). Returns true if it was waiting here.
    pub fn remove(&self, id: &JobId) -> bool {
        let mut inner = self.inner.lock();
        let before = inner.jobs.len();
        inner.jobs.retain(|j| &j.id != id);
        let removed = inner.jobs.len() != before;
        if removed {
            inner.known.remove(id);
        }
        removed
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::JobRequest;
    use alloy::primitives::{Address, Bytes, U256};

    fn job() -> QueuedJob {
        QueuedJob {
            id: uuid::Uuid::now_v7(),
            request: JobRequest { chain_id: 1, to: Address::ZERO, data: Bytes::new(), value: U256::ZERO, gas_limit: None, deadline: None, webhook_url: "http://x".into() },
            requeue_count: 0,
            created_at: chrono::Utc::now(),
            enqueued_at: std::time::Instant::now(),
        }
    }

    #[test]
    fn a_job_can_only_be_enqueued_once() {
        let q = JobQueue::new(10);
        let j = job();
        assert_eq!(q.admit(j.clone()), Admit::Queued);
        assert_eq!(q.admit(j.clone()), Admit::Duplicate);
        // Still known while a worker holds it.
        let popped = q.try_pop().unwrap();
        assert_eq!(q.admit(j.clone()), Admit::Duplicate);
        q.forget(&popped.id);
        assert_eq!(q.admit(j), Admit::Queued);
    }

    #[test]
    fn push_front_jumps_the_queue() {
        let q = JobQueue::new(10);
        let (a, b, c) = (job(), job(), job());
        q.admit(a.clone());
        q.admit(b.clone());
        let first = q.try_pop().unwrap();
        assert_eq!(first.id, a.id);
        q.admit(c.clone());
        q.push_front(first);
        assert_eq!(q.try_pop().unwrap().id, a.id);
        assert_eq!(q.try_pop().unwrap().id, b.id);
        assert_eq!(q.try_pop().unwrap().id, c.id);
    }

    #[test]
    fn window_defers_to_the_database() {
        let q = JobQueue::new(2);
        assert_eq!(q.admit(job()), Admit::Queued);
        assert_eq!(q.admit(job()), Admit::Queued);
        assert_eq!(q.admit(job()), Admit::Deferred);
        assert_eq!(q.overflow(), 1);
        assert!(!q.has_room());
    }

    #[tokio::test]
    async fn every_waiting_worker_is_woken() {
        let q = std::sync::Arc::new(JobQueue::new(100));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let q = q.clone();
            handles.push(tokio::spawn(async move { q.pop().await.id }));
        }
        tokio::task::yield_now().await;
        for _ in 0..8 {
            q.admit(job());
        }
        let mut ids = HashSet::new();
        for h in handles {
            ids.insert(tokio::time::timeout(std::time::Duration::from_secs(2), h).await.expect("worker starved").unwrap());
        }
        assert_eq!(ids.len(), 8);
    }
}
