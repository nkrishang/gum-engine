//! Per-pair balance ledger.
//!
//! An RPC balance is a true but lagging indicator: it knows nothing about what in-flight transactions
//! are about to spend. The ledger therefore leads and the chain follows:
//!
//! - `confirmed` moves when *we* learn something — a receipt debits it, a top-up credits it;
//! - `reserved` is derived from the live set: for each nonce in flight, the worst-case cost of its most
//!   expensive attempt (a replacement does not add to the reservation, it can only raise it);
//! - a balance read from the chain is applied only if nothing was settled since the read began and the
//!   read is not older than the newest receipt, so a lagging node can never overwrite fresher knowledge.

use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};

use alloy::primitives::U256;
use parking_lot::Mutex;

#[derive(Debug, Default)]
pub struct Ledger {
    inner: Mutex<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    confirmed: U256,
    /// Worst-case cost per in-flight nonce.
    reservations: BTreeMap<u64, U256>,
    /// Block of the newest receipt (or accepted reconcile) reflected in `confirmed`.
    as_of_block: u64,
    /// Bumped on every settlement or credit; lets a reconcile detect that it raced one.
    epoch: u64,
    initialised: bool,
    /// Credits that are on-chain but not yet spendable (chains with delayed execution).
    maturing: Vec<(Instant, U256)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerView {
    pub confirmed: U256,
    pub reserved: U256,
    pub initialised: bool,
}

/// Handed out before a balance read and presented with its result.
#[derive(Debug, Clone, Copy)]
pub struct ReconcileTicket {
    epoch: u64,
}

impl Ledger {
    pub fn view(&self) -> LedgerView {
        let inner = self.inner.lock();
        LedgerView { confirmed: inner.confirmed, reserved: inner.reserved(), initialised: inner.initialised }
    }

    /// Sets the balance unconditionally (boot, after receipts have been settled).
    pub fn initialise(&self, balance: U256, block: u64) {
        let mut inner = self.inner.lock();
        inner.confirmed = balance;
        inner.as_of_block = inner.as_of_block.max(block);
        inner.initialised = true;
        inner.epoch += 1;
    }

    /// Reserves the worst-case cost of an attempt at `nonce`. Several attempts at one nonce share one
    /// reservation sized for the most expensive of them — only one of them can ever be mined.
    pub fn reserve(&self, nonce: u64, worst_case: U256) {
        let mut inner = self.inner.lock();
        let slot = inner.reservations.entry(nonce).or_insert(U256::ZERO);
        *slot = (*slot).max(worst_case);
    }

    /// Drops a reservation without spending (the transaction provably never executed).
    pub fn release(&self, nonce: u64) {
        self.inner.lock().reservations.remove(&nonce);
    }

    /// A transaction at `nonce` was mined: release its reservation and debit what it actually cost.
    pub fn settle(&self, nonce: u64, spent: U256, block: u64) {
        let mut inner = self.inner.lock();
        inner.reservations.remove(&nonce);
        inner.confirmed = inner.confirmed.saturating_sub(spent);
        inner.as_of_block = inner.as_of_block.max(block);
        inner.epoch += 1;
    }

    /// Reverses a `settle` whose block was re-orged away.
    pub fn unsettle(&self, nonce: u64, spent: U256, worst_case: U256) {
        let mut inner = self.inner.lock();
        inner.confirmed = inner.confirmed.saturating_add(spent);
        inner.reservations.insert(nonce, worst_case);
        inner.epoch += 1;
    }

    /// An incoming transfer was mined in `block`; it becomes spendable after `maturity` (zero on most
    /// chains). Time-based rather than head-based: an idle chain yields no head observations, and a
    /// credit must not wait for one.
    pub fn credit(&self, amount: U256, block: u64, maturity: Duration) {
        let mut inner = self.inner.lock();
        inner.epoch += 1;
        inner.as_of_block = inner.as_of_block.max(block);
        if maturity.is_zero() {
            inner.confirmed = inner.confirmed.saturating_add(amount);
        } else {
            inner.maturing.push((Instant::now() + maturity, amount));
        }
    }

    /// Moves matured credits into the confirmed balance.
    pub fn mature(&self) {
        let mut inner = self.inner.lock();
        let now = Instant::now();
        let mut matured = U256::ZERO;
        inner.maturing.retain(|(at, amount)| {
            if *at <= now {
                matured = matured.saturating_add(*amount);
                false
            } else {
                true
            }
        });
        inner.confirmed = inner.confirmed.saturating_add(matured);
    }

    /// Credits that are mined but not yet spendable.
    pub fn maturing(&self) -> U256 {
        self.inner.lock().maturing.iter().fold(U256::ZERO, |acc, (_, a)| acc.saturating_add(*a))
    }

    pub fn begin_reconcile(&self) -> ReconcileTicket {
        ReconcileTicket { epoch: self.inner.lock().epoch }
    }

    /// Applies a balance read at `block`. Returns false (and changes nothing) when the read raced a
    /// settlement or is older than what the ledger already reflects.
    pub fn reconcile(&self, ticket: ReconcileTicket, balance: U256, block: u64) -> bool {
        let mut inner = self.inner.lock();
        if inner.epoch != ticket.epoch || block < inner.as_of_block {
            return false;
        }
        // The chain already contains credits that are still maturing locally; do not count them twice.
        let maturing = inner.maturing.iter().fold(U256::ZERO, |acc, (_, a)| acc.saturating_add(*a));
        inner.confirmed = balance.saturating_sub(maturing);
        inner.as_of_block = block;
        inner.initialised = true;
        true
    }
}

impl Inner {
    fn reserved(&self) -> U256 {
        self.reservations.values().fold(U256::ZERO, |acc, v| acc.saturating_add(*v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn u(n: u64) -> U256 {
        U256::from(n)
    }

    #[test]
    fn replacement_does_not_double_reserve() {
        let l = Ledger::default();
        l.initialise(u(1_000), 1);
        l.reserve(7, u(100));
        l.reserve(7, u(130)); // fee bump at the same nonce
        l.reserve(7, u(90)); // a cheaper cancel never lowers the reservation
        assert_eq!(l.view().reserved, u(130));
        l.settle(7, u(60), 2);
        let v = l.view();
        assert_eq!((v.confirmed, v.reserved), (u(940), u(0)));
    }

    #[test]
    fn reconcile_is_rejected_when_it_raced_a_settlement() {
        let l = Ledger::default();
        l.initialise(u(1_000), 10);
        let ticket = l.begin_reconcile();
        l.reserve(1, u(100));
        l.settle(1, u(50), 11); // receipt arrives while the balance read is in flight
        assert!(!l.reconcile(ticket, u(1_000), 10), "stale read must not overwrite the receipt");
        assert_eq!(l.view().confirmed, u(950));
    }

    #[test]
    fn reconcile_from_a_lagging_node_is_rejected() {
        let l = Ledger::default();
        l.initialise(u(1_000), 10);
        l.reserve(1, u(100));
        l.settle(1, u(50), 12);
        let ticket = l.begin_reconcile();
        assert!(!l.reconcile(ticket, u(1_000), 11), "block 11 is older than the receipt at 12");
        assert!(l.reconcile(ticket, u(949), 13));
        assert_eq!(l.view().confirmed, u(949));
    }

    #[test]
    fn busy_pairs_can_still_reconcile() {
        let l = Ledger::default();
        l.initialise(u(1_000), 10);
        l.reserve(1, u(100)); // in flight, not settled: reservations do not block a reconcile
        let ticket = l.begin_reconcile();
        assert!(l.reconcile(ticket, u(990), 11));
        let v = l.view();
        assert_eq!((v.confirmed, v.reserved), (u(990), u(100)));
    }

    #[test]
    fn credits_mature_before_they_can_be_spent() {
        let l = Ledger::default();
        l.initialise(u(10), 100);
        l.credit(u(500), 101, Duration::from_millis(40));
        l.mature();
        assert_eq!(l.view().confirmed, u(10));
        // A chain read taken while maturing must not double count the credit.
        let t = l.begin_reconcile();
        assert!(l.reconcile(t, u(510), 102));
        assert_eq!(l.view().confirmed, u(10));
        std::thread::sleep(Duration::from_millis(60));
        l.mature();
        assert_eq!(l.view().confirmed, u(510));
        // Chains without delayed execution credit immediately.
        l.credit(u(5), 103, Duration::ZERO);
        assert_eq!(l.view().confirmed, u(515));
    }

    #[test]
    fn unsettle_restores_the_reservation() {
        let l = Ledger::default();
        l.initialise(u(1_000), 1);
        l.reserve(3, u(200));
        l.settle(3, u(120), 5);
        l.unsettle(3, u(120), u(200));
        let v = l.view();
        assert_eq!((v.confirmed, v.reserved), (u(1_000), u(200)));
    }
}
