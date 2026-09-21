# gum-bench

Black-box load-testing, benchmarking and **correctness** harness for `gum-engine`.

It has no dependency on engine code. Everything it knows comes from `docs/api-contract.md`: the HTTP
API, webhook deliveries (incl. the HMAC signature), the observability routes, the TOML config schema and
the process contract (`GUM_CONFIG`, `PORT`, `/healthz`, `/readyz`). Verdicts never look at the engine's
database or logs; logs are captured only to debug failed runs.

```
cargo run --release -p gum-bench -- run smoke                 # spawn target/release/gum-engine (built if missing)
cargo run --release -p gum-bench -- run smoke --fake-engine   # validate the harness itself
cargo run --release -p gum-bench -- scenarios                 # list scenarios
cargo run --release -p gum-bench -- compare bench/baselines/smoke.json bench/results/<new>.json
cargo run --release -p gum-bench -- sink --port 9900 --secret dev-secret   # manual webhook receiver
```

Exit codes: `0` clean · `1` correctness violations (or a `compare` regression) · `2` the harness itself
failed (no report is written — a partial report is worse than none) · `130` interrupted (children
killed, run database dropped).

## Requirements

* `anvil` on `PATH` with `eth_sendRawTransactionSync` support (verified with 1.5.1). Anvil is spawned directly, never via
  docker, as `anvil --port <free> --block-time 1 --chain-id <id> --accounts <N+2> --balance 10000
  --state <file> --silent` — no latency flags, ever.
* A Postgres server on which the current user may `CREATE DATABASE`. Default
  `postgres://localhost:5432/postgres`; override with `--postgres-url` / `GUM_BENCH_POSTGRES_URL`, e.g.
  the compose instance: `postgres://gum:gum@127.0.0.1:54329/gum`. Every run gets a fresh
  `gum_bench_<id>` database, dropped afterwards unless `--keep-db`.
* No `forge` needed: the `BenchTarget` creation bytecode is committed (`contracts/BenchTarget.hex`).

## What a run does

1. **Rig** — starts 1–3 Anvils (chain ids 31337, 31338, 31339), deploys `BenchTarget` to each, starts
   one counting RPC proxy per chain and the webhook sink, creates the database, writes the engine
   config (`signers` = dev accounts `1..=N` of the `test … junk` mnemonic, treasury = account 0,
   `rpc_url` = the proxy; account `N+1` is the harness's own deployer and is never given to the engine),
   spawns the engine with `GUM_CONFIG`, `PORT` and `DATABASE_URL` (inherited `GUM_*` variables are
   scrubbed so the generated config is what actually runs), waits for `/healthz` then `/readyz`
   (timeout → error with the tail of the engine log). Engine CPU% / RSS are sampled once per second.
2. **Load** — open-loop: request *i* fires at `t0 + i/rate` no matter how many responses are
   outstanding. Latency is measured from that *intended* instant (no coordinated omission); how late
   the harness really fired is reported as *schedule lag* and flagged if p99 > 50 ms.
3. **Drain** — polls `GET /v1/transactions/{id}` for every accepted job until all are terminal (gives
   up after `--drain-stall-timeout` seconds without progress).
4. **Oracle** — reads ground truth *directly* from Anvil (not through the proxy): every block of the
   run with full transactions and receipts, `Hit` logs via `eth_getLogs`, nonces (latest + pending) and
   balances of treasury and signers.
5. **Verdict + report** — see below. Any violation ⇒ exit code 1.

### Why the chain is an exactly-once oracle

Every job is attributable on-chain by construction: `hit(id)` / `burn(id, n)` carry a unique random
`bytes32` (and `BenchTarget` reverts a repeat with `"dup"`); `fail()` calls get the id appended as
trailing calldata (ignored by Solidity); value transfers go to a unique never-used recipient. Any second
inclusion — even one that reverted — is visible.

Job kinds (`--mix mixed|gas|nogas|hit_gas=3,transfer=1,…`):

| kind | request | expected end state |
|---|---|---|
| `hit_gas` | `hit(id)`, `gas_limit` 120000 | confirmed / success |
| `hit_nogas` | `hit(id)`, no gas limit (engine estimates) | confirmed / success |
| `burn_gas` | `burn(id, 200)`, gas limit | confirmed / success |
| `fail_gas` | `fail()`, `gas_limit` 60000 | confirmed / **reverted** (included on-chain) |
| `fail_nogas` | `fail()`, no gas limit | **failed** / `simulation_reverted`, never on-chain |
| `transfer` | 1000 wei to a unique address, `gas_limit` 21000 | confirmed / success |

A configurable share of jobs carries an `Idempotency-Key`; some of those are deliberately replayed
(same body → must be `200 replayed:true` with the same id; different body → must be `409
idempotency_conflict`).

## Correctness verdict

| id | check |
|---|---|
| a | every accepted (202) job reached `confirmed` or `failed` |
| b | confirmed ⇒ exactly one tx on-chain with matching outcome; failed ⇒ zero; nobody ever twice (`Hit` logs cross-checked against the tx scan); a definitively rejected job must not execute |
| c | nonces: `pending == latest` for every signer at rest, API `(signer, nonce, tx_hash, block_number)` equals the executing tx, no `(chain, signer, nonce)` claimed by two jobs, `/v1/signers.next_nonce` equals the on-chain nonce, no pair busy/paused at rest |
| d | webhooks: ≥1 `transaction.included` and ≥1 `transaction.confirmed` per confirmed job, `transaction.failed` per failed job, nothing contradictory, outcome matches the chain, `sequence` consistent (stable per `event_id`, unique per event, included < confirmed, starts at 1), every `X-Gum-Signature` valid, `X-Gum-Event-Id`/`X-Gum-Job-Id` match the body |
| e | `GET /v1/analytics/transactions` totals (delta since run start) equal ground truth at rest; nothing queued/in flight; `by_chain` sums to `totals` |
| f | `GET /v1/signers/balances` `confirmed` equals `eth_getBalance` for treasury and every signer at rest |
| g | job-mix expectations from the table above |
| h | idempotency replays behave per contract |
| i | API hygiene: valid POSTs only get 202 / 503 (or 200 on a keyed retry), observability routes parse, all pairs listed, engine process did not die |
| s | scenario-specific assertions |

At-rest checks (c/e/f) are retried for `--settle-timeout` seconds (default 15) before they count.
Coverage in (d) is not demanded from receivers that can never acknowledge (`fail`, `dead`).
POSTs that ended without an HTTP response are *indeterminate*: the engine may legitimately have the
job, so an on-chain execution is tolerated — but never two.

Each violation class lists a count, up to 50 job ids and a few detailed examples.

## Scenarios

`gum-bench run <scenario> [--rate R] [--duration S] [--signers N] [--chains C] [--mix …] [--set k=v]`

| scenario | default shape | purpose / extra assertions |
|---|---|---|
| `smoke` | 1 chain, 5 signers, 10 jobs/s × 20 s, all kinds | CI gate (~45 s). Offered above capacity on purpose so throughput = capacity, stable across machines. |
| `steady` | 10 signers, 8 jobs/s × 60 s | latency baseline; engine must keep up below capacity |
| `saturation` | 10 signers, `hit_gas` only, ramp from 5/s in steps of capacity/8 every 10 s | stops when queue depth grew monotonically for 5 samples; reports `max_sustainable_throughput_per_s` (peak on-chain jobs/s of any step), `max_sustainable_rate` (last offered rate that ended a step without backlog), `saturated_at_rate`, per-step numbers; asserts peak ≥ 60 % of `signers / block_time`. `--set step_secs= step= growth_samples=` |
| `burst` | base 2/s, 3 s bursts of 40/s every 15 s | `--set burst_rate= burst_secs= period_secs=` |
| `mixed` | 3 chains × 5 signers, 19.5 jobs/s total | asserts **each** chain reaches ≥ 75 % of its own capacity — a signer busy on chain A must still serve chain B |
| `webhook-hostile` | 40 % ok / 20 % slow 3 s / 15 % always-500 / 15 % flaky / 10 % dead | finished jobs/s inside the load window ≥ 80 % of accepted/s. Each receiver mode listens on its own port so per-host breakers are not confused. |
| `funds` | 3 signers start at 0.5 ETH (< min 1 ETH) | phase 1: every signer must be topped up. Phase 2: treasury + signers set to 0 via `anvil_setBalance` → a pair must show `pause.reason` ≈ `InsufficientFunds` on `/v1/signers` and jobs must stay queued → treasury refilled → all jobs complete |
| `rpc-faults` | 3 jobs/s × 60 s | scripted windows of HTTP 429, JSON-RPC −32007, 500, 503, **drop-after-forward on the send methods**, delay-then-close, full blackhole. Verdict must stay clean. |
| `chaos` | 3 jobs/s × 75 s, all jobs keyed, client retries with the same key | `kill -9` + restart the engine 3× (`--set kills=`), stop Anvil ~10 s and restart it on the same port with its state, restart Postgres only with `--postgres-container <name>` (otherwise a logged no-op). Verdict must stay clean. |
| `soak` | 8 jobs/s × 600 s | long steady run; watch RSS in the report |

New scenario = one file in `src/scenarios/` implementing the `Scenario` trait (`defaults`, `drive`,
optional `assess`) plus one line in `scenarios::all()`.

## The counting proxy

Sits between engine and each Anvil. Counts every JSON-RPC call per method (single and batch bodies),
records per-method latency, and injects faults per method / probability / time window: HTTP 429 (body
`{"jsonrpc":"2.0","error":{"code":429,…},"id":…}`), JSON-RPC −32007, HTTP 500/503, delay-then-close,
**drop response after forwarding** (the indeterminate-send case: Anvil got the tx, the engine sees a dead
connection) and blackhole. `--rpc-rtt-ms` (default **0**) adds a symmetric delay to emulate WAN
distance; nothing else in the harness adds latency. Oracle traffic bypasses the proxy and is not counted.

`calls/tx` = calls between load start and the last job turning terminal ÷ jobs that reached a terminal
state (boot-time calls are reported separately). The with/without-gas-limit split is only attributable
when the whole run used one kind (`--mix gas` / `--mix nogas`).

## Reports, baselines, compare

Each run writes `bench/results/<git short sha|nogit>-<scenario>-<timestamp>.json` and a `.md` next to it
(also printed to stdout), plus a run directory `bench/results/runs/<ts>-<scenario>/` with `engine.toml`,
`engine.log`, Anvil logs and state. `bench/results/` is git-ignored.

Report schema (v1), top-level keys: `schema_version`, `scenario`, `params`, `started_at`, `finished_at`,
`duration_s`, `git{sha,dirty}`, `machine{os,os_version,arch,cpu_model,cores,ram_bytes,fingerprint}`,
`engine{mode,label,build_profile,restarts}`, `load{offered_jobs,offered_rate,accepted_jobs,
achieved_accept_rate,segments[],schedule_lag_ms}`, `throughput{confirmed_jobs,confirmed_per_s,window_s,
basis,finished_per_s_in_load_window,on_chain_per_s_in_load_window(+_by_chain)}`, `latency_ms{accept,
accept_to_included,accept_to_included_webhook,accept_to_confirmed_webhook}` (each `count,min,mean,p50,
p90,p95,p99,p999,max`), `rpc{source,total_calls,boot_calls,tx_count,calls_per_tx,
calls_per_tx_with_gas_limit,calls_per_tx_without_gas_limit,credits_per_tx_at_20,credits_per_tx_at_30,
by_method{calls,calls_per_tx,faulted,upstream_errors,latency_ms},faults_injected}`,
`engine_process{cpu_pct_avg,cpu_pct_max,rss_mb_avg,rss_mb_max}`, `http{<status>|transport:<kind>: n}`,
`jobs`, `webhooks`, `drain`, `scenario_metrics`, `notes`, `harness_warnings`,
`verdict{pass,violations[{check,code,message,count,job_ids,examples}]}`, `series[]` (1 Hz: counts,
per-chain counts, queue depth, busy/paused/total pairs, signer utilisation = busy pairs / total pairs).

Latency definitions: `accept` = POST response − intended start (202s only). `accept_to_included` =
`included_at − created_at` from the status API (engine clock on both sides). The two webhook metrics =
first arrival at the sink − intended start (harness monotonic clock).

**Baselines** live in `bench/baselines/<scenario>.json` and are committed. Refreshing one is a
deliberate, reviewed act:

```
cargo run --release -p gum-bench -- run smoke --save-baseline     # refused if the verdict fails
```

`gum-bench compare <baseline> <new> [--throughput-tolerance 0.05] [--p99-tolerance 0.10]
[--rpc-tolerance 0] [--ignore-latency] [--force-latency]` prints a table and exits 1 on: any
correctness violation in the new run · RPC calls/tx increased (strict, compared to 3 decimals) ·
throughput dropped more than the tolerance · p99 of accept latency or accept→included grew more than the
tolerance. Throughput/latency gates are skipped with a warning when the machine fingerprints differ
(unless `--force-latency`).

Intended CI (no workflow file is shipped):

```
cargo test   -p gum-bench                       # unit tests + harness self-checks against the fake engine
cargo build  --release -p gum-engine -p gum-bench
target/release/gum-bench run smoke
target/release/gum-bench compare bench/baselines/smoke.json "$(ls -t bench/results/*-smoke-*.json | head -1)"
```

`saturation`, `mixed`, `chaos`, `rpc-faults`, `funds`, `webhook-hostile`, `soak` run on demand and
before releases.

## Running against an engine you started yourself

```
gum-bench run smoke --target http://127.0.0.1:8080 --webhook-secret dev-secret \
    --target-rpc 31337=http://127.0.0.1:8545 [--deployer-key 0x…]
```

No process control (so no `chaos`) and no counting proxy: RPC numbers then come from the engine's own
`/v1/chains` meter and are labelled `source: engine_reported` (`compare` refuses to gate across sources).
Signers are still assumed to be dev accounts `1..=--signers`. The mix seed is randomised so ids never
repeat on a long-lived chain.

## The fake engine (harness self-validation)

`gum-bench fake-engine` (hidden subcommand; `run … --fake-engine`) is a ~700-line in-memory
implementation of the contract: one in-flight tx per (signer, chain), `eth_sendRawTransactionSync`
through the proxy, signed webhooks with retries, top-ups, InsufficientFunds pause, all routes. No
persistence — so `chaos` is *expected* to fail against it. `--fake-bug` proves the oracle catches what
it claims to catch:

| `--fake-bug` | must produce |
|---|---|
| `double-send` | `b:executed_more_than_once` |
| `skip-webhook` | `d:missing_confirmed_webhook` |
| `lose-job` | `a:not_terminal` (+ `e:analytics_mismatch`) |
| `bad-signature` | `d:invalid_signature` |
| `wrong-analytics` | `e:analytics_mismatch` |

These run as `cargo test -p gum-bench` (`tests/selfcheck.rs`); they skip with a message when `anvil` or
Postgres is unavailable.

## Regenerating the contract bytecode

`contracts/BenchTarget.sol` → `contracts/BenchTarget.hex` (embedded with `include_str!`):

```
crates/gum-bench/scripts/regen-bytecode.sh     # needs forge; solc 0.8.30, optimizer 200 runs, evm paris, no metadata hash
```

## Taking a baseline that means something

Correctness, RPC calls/tx and throughput are deterministic on Anvil — eight consecutive `smoke` runs against
gum-engine gave 0 violations, exactly 1.305 calls/tx and 4.96–5.03 confirmed jobs/s every time. Latency is
not: on a laptop the accept-latency p90 moved between 7.1 and 8.5 ms (±10% around the median) with nothing
changing, and more right after a compile. So:

- **Baseline on the median, never the best run.** Run the scenario at least three times on a quiet machine
  and save the run whose p90 is in the middle. A baseline taken from a lucky run makes the gate flap.
- **Percentile gates need samples.** `compare` gates on p99 only when both runs have ≥ 1000 samples; below
  that it gates on p90 and prints p99 for information (at n = 200 the p99 is the second-worst request).
  Use `steady` (≥ 1000 jobs) when a change is about tail latency.
- **A failed latency gate on a busy machine is a prompt to re-run**, not a verdict. A failed correctness,
  RPC-calls or throughput gate is a verdict.
- On shared CI runners use `compare --ignore-latency` for `smoke` and keep the latency gates for a
  dedicated machine.
