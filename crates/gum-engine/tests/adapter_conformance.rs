//! Conformance suite every chain kind must pass. A new kind is not done until this is green for it.
//!
//! It runs each registered adapter against recorded fixtures (`fixtures/<kind>/errors.json` and
//! `receipts.json` — real payloads, not invented ones) and checks the invariants the pipeline relies on:
//!
//! - every recorded send error classifies exactly as recorded, and anything unrecognised is
//!   `Indeterminate` (the only safe default: it never frees a nonce);
//! - `actual_cost` reproduces what the chain charged, and `worst_case_cost` is never below it — the
//!   ledger must reserve at least what a transaction can cost;
//! - a replacement quote is one the chain's own pool rule accepts, and never exceeds the cap;
//! - the stuck ladder only contains steps the chain can actually perform;
//! - a failed gas estimate asks for funds only when funds are missing, never for a revert.

use std::{path::PathBuf, str::FromStr};

use alloy::primitives::U256;
use gum_engine::{
    chain::{
        adapter::{ChainAdapter, CostContext, FeeQuote, SendErrorClass, StuckStep, TxShape},
        registry,
    },
    error::RpcError,
    rpc::types::Receipt,
};
use serde::Deserialize;

#[derive(Deserialize)]
struct ErrorFixture {
    code: i64,
    message: String,
    #[serde(default)]
    data: Option<String>,
    class: String,
}

#[derive(Deserialize)]
struct ReceiptFixture {
    note: String,
    gas_limit: u64,
    max_fee_per_gas: String,
    value: String,
    receipt: Receipt,
    expect_fee_paid: String,
    #[serde(default)]
    expect_l1_fee: Option<String>,
    #[serde(default)]
    l1_fee_per_byte: Option<String>,
}

fn fixtures_dir(kind: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures").join(kind)
}

fn load<T: serde::de::DeserializeOwned>(kind: &str, file: &str) -> Vec<T> {
    let path = fixtures_dir(kind).join(file);
    let text = std::fs::read_to_string(&path).unwrap_or_else(|_| panic!("kind `{kind}` has no {file}: every kind must ship fixtures at {}", path.display()));
    serde_json::from_str(&text).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

fn class_name(c: SendErrorClass) -> &'static str {
    match c {
        SendErrorClass::AlreadyKnown => "AlreadyKnown",
        SendErrorClass::AcceptedPending => "AcceptedPending",
        SendErrorClass::NonceTooLow => "NonceTooLow",
        SendErrorClass::NonceTooHigh => "NonceTooHigh",
        SendErrorClass::FeeTooLow => "FeeTooLow",
        SendErrorClass::InsufficientFunds => "InsufficientFunds",
        SendErrorClass::PoolBusy => "PoolBusy",
        SendErrorClass::Deterministic => "Deterministic",
        SendErrorClass::Indeterminate => "Indeterminate",
    }
}

fn u(s: &str) -> U256 {
    U256::from_str(s).unwrap_or_else(|_| panic!("bad number in fixture: {s}"))
}

fn each_kind(mut f: impl FnMut(&str, &dyn ChainAdapter)) {
    for kind in registry::known_kinds() {
        let adapter = registry::adapter_for(kind).unwrap_or_else(|| panic!("kind `{kind}` is listed but not registered"));
        assert_eq!(adapter.kind(), *kind, "adapter reports a different kind than it is registered under");
        f(kind, adapter.as_ref());
    }
}

#[test]
fn recorded_send_errors_classify_as_recorded() {
    each_kind(|kind, adapter| {
        let fixtures: Vec<ErrorFixture> = load(kind, "errors.json");
        assert!(fixtures.len() >= 5, "kind `{kind}`: record at least the common send errors");
        for fx in fixtures {
            let err = RpcError::Response { code: fx.code, message: fx.message.clone(), data: fx.data.clone() };
            let got = class_name(adapter.classify_send_error(&err));
            assert_eq!(got, fx.class, "kind `{kind}`: `{}` classified as {got}, fixture says {}", fx.message, fx.class);
        }
    });
}

#[test]
fn anything_uncertain_is_indeterminate() {
    each_kind(|kind, adapter| {
        let uncertain = [
            RpcError::Transport("connection reset by peer".into()),
            RpcError::Transport("operation timed out".into()),
            RpcError::Decode("unexpected end of input".into()),
            RpcError::Response { code: -32000, message: "a message this adapter has never seen".into(), data: None },
        ];
        for err in uncertain {
            assert_eq!(adapter.classify_send_error(&err), SendErrorClass::Indeterminate, "kind `{kind}`: `{err}` must never be treated as a verdict");
        }
    });
}

#[test]
fn only_missing_funds_send_an_estimate_to_the_treasury() {
    each_kind(|kind, adapter| {
        let not_funds = [
            RpcError::Transport("operation timed out".into()),
            RpcError::Decode("unexpected end of input".into()),
            RpcError::Response { code: 3, message: "execution reverted".into(), data: Some("0x08c379a0".into()) },
        ];
        for err in not_funds {
            assert!(!adapter.estimate_lacks_funds(&err), "kind `{kind}`: `{err}` is not a funding problem; treating it as one would top up a signer for a call that reverts");
        }
        let geth_style = RpcError::Response { code: -32000, message: "insufficient funds for gas * price + value".into(), data: None };
        assert!(adapter.estimate_lacks_funds(&geth_style), "kind `{kind}`: the geth-family wording must still be recognised");
    });
}

#[test]
fn costs_match_receipts_and_reserves_cover_them() {
    each_kind(|kind, adapter| {
        let fixtures: Vec<ReceiptFixture> = load(kind, "receipts.json");
        assert!(!fixtures.is_empty(), "kind `{kind}`: record at least one receipt");
        for fx in fixtures {
            let cost = adapter.actual_cost(fx.gas_limit, &fx.receipt);
            assert_eq!(cost.fee_paid, u(&fx.expect_fee_paid), "kind `{kind}` ({}): fee_paid", fx.note);
            assert_eq!(cost.l1_fee, fx.expect_l1_fee.as_deref().map(u), "kind `{kind}` ({}): l1_fee", fx.note);

            // The price actually paid can never exceed max_fee, so reserving at max_fee must cover it.
            let max_fee: u128 = fx.max_fee_per_gas.parse().unwrap();
            assert!(fx.receipt.effective_gas_price_u128() <= max_fee, "fixture `{}`: effective price above max fee", fx.note);
            let fees = FeeQuote { max_fee_per_gas: max_fee, max_priority_fee_per_gas: 0 };
            let ctx = CostContext { l1_fee_per_byte: fx.l1_fee_per_byte.as_deref().map(u) };
            let shape = TxShape { gas_limit: fx.gas_limit, value: u(&fx.value), encoded_len: 200 };
            let reserve = adapter.worst_case_cost(&shape, &fees, &ctx);
            assert!(reserve >= cost.fee_paid + u(&fx.value), "kind `{kind}` ({}): reserve {reserve} is below the real cost {}", fx.note, cost.fee_paid + u(&fx.value));
        }
    });
}

#[test]
fn replacement_quotes_are_acceptable_and_capped() {
    each_kind(|kind, adapter| {
        let t = adapter.defaults();
        let caps = adapter.capabilities();
        let gwei = 1_000_000_000u128;
        for prev_fee in [1u128, 7, gwei, 3 * gwei + 1, t.max_fee_cap_wei / 2] {
            for base_fee in [0u128, prev_fee / 2, prev_fee, prev_fee * 3] {
                let prev = FeeQuote { max_fee_per_gas: prev_fee, max_priority_fee_per_gas: prev_fee.min(t.priority_fee_wei.max(1)) };
                if let Some(next) = adapter.replacement(&prev, base_fee, t.max_fee_cap_wei, &t) {
                    assert!(caps.replacement, "kind `{kind}` quotes replacements but says it has none");
                    assert!(adapter.accepts_replacement(&prev, &next), "kind `{kind}`: its own pool rule rejects {next:?} as a replacement for {prev:?}");
                    assert!(next.max_fee_per_gas <= t.max_fee_cap_wei, "kind `{kind}`: replacement exceeds the cap");
                    assert!(next.max_priority_fee_per_gas <= next.max_fee_per_gas, "kind `{kind}`: tip above max fee");
                }
            }
        }
        // At the cap there is nowhere left to go for a job…
        let at_cap = FeeQuote { max_fee_per_gas: t.max_fee_cap_wei, max_priority_fee_per_gas: t.priority_fee_wei.min(t.max_fee_cap_wei) };
        assert!(adapter.replacement(&at_cap, 0, t.max_fee_cap_wei, &t).is_none(), "kind `{kind}`: a job at the cap must not be bumped past it");
        // …but a cancel must still be able to out-bid it, or a stuck nonce could never be freed.
        if caps.replacement {
            let cancel = adapter.replacement(&at_cap, 0, t.cancel_fee_cap_wei, &t);
            assert!(
                cancel.is_some_and(|c| adapter.accepts_replacement(&at_cap, &c)),
                "kind `{kind}`: cancel_fee_cap_wei leaves no room to replace a job sitting at max_fee_cap_wei"
            );
        }
    });
}

#[test]
fn fresh_quotes_respect_the_cap() {
    each_kind(|kind, adapter| {
        let t = adapter.defaults();
        for base in [0u128, 1, 1_000_000_000, t.max_fee_cap_wei, t.max_fee_cap_wei * 10] {
            let q = adapter.fee_quote(base, &t);
            assert!(q.max_fee_per_gas <= t.max_fee_cap_wei.max(t.priority_fee_wei), "kind `{kind}`: quote above cap at base fee {base}");
            assert!(q.max_priority_fee_per_gas <= q.max_fee_per_gas, "kind `{kind}`: tip above max fee at base fee {base}");
        }
    });
}

#[test]
fn stuck_ladder_matches_capabilities() {
    each_kind(|kind, adapter| {
        let ladder = adapter.stuck_ladder();
        let caps = adapter.capabilities();
        assert!(!ladder.is_empty(), "kind `{kind}`: an empty ladder would send every slow transaction straight to an operator");
        assert!(ladder.contains(&StuckStep::Diagnose), "kind `{kind}`: escalation must diagnose before it spends");
        if !caps.replacement {
            assert!(!ladder.contains(&StuckStep::Bump) && !ladder.contains(&StuckStep::Noop), "kind `{kind}`: Bump/Noop need same-nonce replacement, which this chain lacks");
        }
        if let (Some(d), Some(b)) = (ladder.iter().position(|s| *s == StuckStep::Diagnose), ladder.iter().position(|s| *s == StuckStep::Bump)) {
            assert!(d < b, "kind `{kind}`: diagnose before bumping");
        }
        if let Some(n) = ladder.iter().position(|s| *s == StuckStep::Noop) {
            assert_eq!(n, ladder.len() - 1, "kind `{kind}`: the cancel is the last resort");
        }
    });
}

#[test]
fn defaults_are_sane() {
    each_kind(|kind, adapter| {
        let t = adapter.defaults();
        assert!(t.block_time_ms > 0 && t.stuck_after_blocks > 0, "kind `{kind}`");
        assert!(t.estimate_multiplier_bps >= 10_000, "kind `{kind}`: an estimate multiplier below 1.0 guarantees out-of-gas");
        assert!(t.max_fee_multiplier_bps >= 10_000, "kind `{kind}`");
        assert!(t.stall_outage_ms >= t.stall_warn_ms, "kind `{kind}`");
        assert!(t.cancel_fee_cap_wei >= t.max_fee_cap_wei, "kind `{kind}`");
        let five = U256::from(5);
        assert!(adapter.spendable(five, U256::ZERO) <= five, "kind `{kind}`: spendable can never exceed the balance");
        assert!(adapter.spendable(five, U256::from(1)) <= adapter.spendable(five, U256::ZERO), "kind `{kind}`: moving value can never unlock more than paying gas");
    });
}
