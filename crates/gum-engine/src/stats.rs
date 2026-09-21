//! Live transaction counters behind `/v1/analytics/transactions`.
//!
//! Kept in memory so the endpoint never touches the jobs table. Terminal buckets are seeded at boot from
//! `stats_rollup` (maintained transactionally with each terminal state change); non-terminal buckets are
//! rebuilt from the live rows that boot loads anyway.

use std::collections::BTreeMap;

use alloy::primitives::Address;
use parking_lot::Mutex;
use serde::Serialize;

use crate::domain::{addr_hex, ChainId};

/// Where a job currently is. `Processing` = popped by a signer but not yet bound to a nonce.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Bucket {
    Queued,
    Processing,
    InFlight,
    Included,
    Succeeded,
    Reverted,
    Failed,
}

#[derive(Debug, Clone, Copy, Default, Serialize, PartialEq, Eq)]
pub struct Counts {
    pub queued: u64,
    pub processing: u64,
    pub in_flight: u64,
    pub included: u64,
    pub succeeded: u64,
    pub reverted: u64,
    pub failed: u64,
    pub total: u64,
}

impl Counts {
    fn slot(&mut self, b: Bucket) -> &mut u64 {
        match b {
            Bucket::Queued => &mut self.queued,
            Bucket::Processing => &mut self.processing,
            Bucket::InFlight => &mut self.in_flight,
            Bucket::Included => &mut self.included,
            Bucket::Succeeded => &mut self.succeeded,
            Bucket::Reverted => &mut self.reverted,
            Bucket::Failed => &mut self.failed,
        }
    }

    fn add(&mut self, b: Bucket, n: u64) {
        *self.slot(b) += n;
        self.total += n;
    }

    fn sub(&mut self, b: Bucket, n: u64) {
        let slot = self.slot(b);
        *slot = slot.saturating_sub(n);
        self.total = self.total.saturating_sub(n);
    }

    fn merge(&mut self, other: &Counts) {
        self.queued += other.queued;
        self.processing += other.processing;
        self.in_flight += other.in_flight;
        self.included += other.included;
        self.succeeded += other.succeeded;
        self.reverted += other.reverted;
        self.failed += other.failed;
        self.total += other.total;
    }
}

#[derive(Default)]
pub struct Stats {
    /// Keyed by (chain, signer); `None` holds jobs that have not reached a signer.
    inner: Mutex<BTreeMap<(ChainId, Option<Address>), Counts>>,
}

#[derive(Debug, Serialize)]
pub struct ChainCounts {
    pub chain_id: ChainId,
    #[serde(flatten)]
    pub counts: Counts,
}

#[derive(Debug, Serialize)]
pub struct SignerCounts {
    pub signer: String,
    #[serde(flatten)]
    pub counts: Counts,
}

#[derive(Debug, Serialize)]
pub struct ChainSignerCounts {
    pub chain_id: ChainId,
    pub signer: String,
    #[serde(flatten)]
    pub counts: Counts,
}

#[derive(Debug, Serialize)]
pub struct Snapshot {
    pub totals: Counts,
    pub by_chain: Vec<ChainCounts>,
    pub by_signer: Vec<SignerCounts>,
    pub by_chain_signer: Vec<ChainSignerCounts>,
}

impl Stats {
    /// Forgets everything; used when an instance becomes leader and rebuilds from the store.
    pub fn reset(&self) {
        self.inner.lock().clear();
    }

    pub fn add(&self, chain: ChainId, signer: Option<Address>, bucket: Bucket, n: u64) {
        self.inner.lock().entry((chain, signer)).or_default().add(bucket, n);
    }

    /// Moves one job between buckets, possibly attributing it to a signer along the way.
    pub fn transition(&self, chain: ChainId, from: (Option<Address>, Bucket), to: (Option<Address>, Bucket)) {
        let mut inner = self.inner.lock();
        inner.entry((chain, from.0)).or_default().sub(from.1, 1);
        inner.entry((chain, to.0)).or_default().add(to.1, 1);
    }

    pub fn queued(&self, chain: ChainId) -> u64 {
        self.inner.lock().get(&(chain, None)).map(|c| c.queued).unwrap_or(0)
    }

    pub fn snapshot(&self) -> Snapshot {
        let inner = self.inner.lock();
        let mut totals = Counts::default();
        let mut by_chain: BTreeMap<ChainId, Counts> = BTreeMap::new();
        let mut by_signer: BTreeMap<Address, Counts> = BTreeMap::new();
        let mut by_chain_signer = Vec::new();
        for ((chain, signer), counts) in inner.iter() {
            totals.merge(counts);
            by_chain.entry(*chain).or_default().merge(counts);
            if let Some(signer) = signer {
                by_signer.entry(*signer).or_default().merge(counts);
                by_chain_signer.push(ChainSignerCounts { chain_id: *chain, signer: addr_hex(signer), counts: *counts });
            }
        }
        Snapshot {
            totals,
            by_chain: by_chain.into_iter().map(|(chain_id, counts)| ChainCounts { chain_id, counts }).collect(),
            by_signer: by_signer.into_iter().map(|(s, counts)| SignerCounts { signer: addr_hex(&s), counts }).collect(),
            by_chain_signer,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_is_counted_exactly_once_through_its_life() {
        let stats = Stats::default();
        let signer = Address::repeat_byte(1);
        stats.add(1, None, Bucket::Queued, 1);
        stats.transition(1, (None, Bucket::Queued), (Some(signer), Bucket::Processing));
        stats.transition(1, (Some(signer), Bucket::Processing), (Some(signer), Bucket::InFlight));
        stats.transition(1, (Some(signer), Bucket::InFlight), (Some(signer), Bucket::Included));
        stats.transition(1, (Some(signer), Bucket::Included), (Some(signer), Bucket::Succeeded));
        let snap = stats.snapshot();
        assert_eq!(snap.totals.total, 1);
        assert_eq!(snap.totals.succeeded, 1);
        assert_eq!(snap.totals.queued + snap.totals.processing + snap.totals.in_flight + snap.totals.included, 0);
        assert_eq!(snap.by_signer[0].counts.succeeded, 1);
    }
}
