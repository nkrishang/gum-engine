# Adding a chain

Adding chains is the change this codebase is shaped around. Everything that differs between chains sits
behind one trait, `ChainAdapter` (`crates/gum-engine/src/chain/adapter.rs`). The pipeline, funds, store and
API never branch on a chain id, a kind or a name — `tests/core_is_chain_agnostic.rs` fails the build if
they start to.

## 1. Is it a kind we already have? Then it is configuration only.

| Kind | Use it for | What makes it that kind |
|---|---|---|
| `opstack` | Base, Optimism, Unichain, any OP Stack rollup | geth-style pool; L1 data fee charged on top (`l1Fee` in receipts) |
| `arbitrum` | Arbitrum One, Orbit chains | no pool, no replacement; L1 cost inside the gas limit; tips ignored |
| `monad` | Monad | charged on the gas *limit*; 10 MON reserve; accepts-then-drops; `finalized` tag confirmation |
| `geth` | vanilla EIP-1559 chains, local Anvil | geth-style pool, +10% replacement rule |

Add a block to the config and an env var for the endpoint. No code, no rebuild of anything but config:

```toml
[chains.optimism]
chain_id = 10
kind = "opstack"
rpc_url_env = "RPC_URL_OPTIMISM"
treasury_key_id = "<kms key id>"
signer_min_balance = "2000000000000000"
topup_amount = "10000000000000000"
treasury_min_balance = "200000000000000000"
# Optional: override any kind default (see ChainTunables in src/config.rs). Unknown keys are a boot error.
# confirmation_delay_ms = 4000
# max_fee_cap_wei = 50000000000
# credits_per_call = 20
```

At boot the engine verifies `eth_chainId` against `chain_id`, probes `eth_sendRawTransactionSync` and
Multicall3, registers every signer on the new chain and tops them up from the chain's treasury.

Local drill (no code, proves the claim): add a second `[chains.*]` block pointing at another Anvil with a
different `--chain-id`, then `cargo run -p gum-bench -- run mixed`.

## 2. A genuinely new kind: one file, fixtures, one registry line

1. **Research the chain first.** The questions that matter (they are the trait's methods):
   - What exactly is the sender charged — gas used or gas limit? Anything on top (L1 data fee)?
   - Is part of the balance unspendable (a reserve)? When does an incoming transfer become spendable?
   - Is there a pool? Is same-nonce replacement possible, and what is the acceptance rule?
   - What are the node's *exact* error strings for: already known, nonce too low/high, underpriced,
     insufficient funds, pool full, stateless rejects? Does the node reject up front, or accept and drop later?
   - What is a sane soft-confirmation signal and delay (block hash, tx membership, a `finalized` tag)?
   - Does the chain always produce blocks (`head_advance`) or only under load (`rpc_responsive`)?
2. **Record fixtures** in `fixtures/<kind>/errors.json` and `receipts.json` — real payloads from the chain's
   testnet, not invented ones. Each error fixture states the class it must map to; each receipt fixture
   states what the sender was actually charged.
3. **Write `crates/gum-engine/src/chain/kinds/<kind>.rs`** implementing `ChainAdapter`. Adapters are pure
   functions (no I/O, no clocks). Reuse the helpers in `geth.rs` if the chain is geth-derived. Unknown
   error messages must fall through to `SendErrorClass::Indeterminate` — it is the only class that can
   never free a nonce wrongly.
4. **Register it**: one line in `chain/registry.rs` (`adapter_for` and `known_kinds`), one in `kinds/mod.rs`.
5. **Run the conformance suite**: `cargo test -p gum-engine --test adapter_conformance --test core_is_chain_agnostic`.
   It checks every fixture, that reserves always cover real costs, that your replacement quotes satisfy
   your own pool rule and the cap, that a cancel can out-bid a job at the cap, and that the stuck ladder
   only contains steps the chain can perform.
6. **Testnet smoke** with real KMS keys: `cargo run -p gum-bench -- run smoke --target <engine url>` against
   an engine configured for the testnet. Then compare `/v1/chains` → `rpc.by_method` with the provider's
   own usage report to validate `credits_per_call`.
7. Mainnet: add the `[chains.*]` block, fund the treasury, deploy.

## What you should never need to touch

`pipeline/`, `funds/`, `store/`, `api.rs`, `queue.rs`, `leader.rs`. If a new chain seems to need a change
there, the missing piece is a `ChainAdapter` method or a tunable — add that instead, give it a default so
existing kinds are unaffected, and extend the conformance suite to cover it.
