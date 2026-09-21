# gum-engine

A transaction relayer for EVM chains, written in Rust. You `POST` an unsigned transaction and get a
job id back immediately. A pool of signers broadcasts jobs concurrently, the engine tracks each one to a
receipt, and your webhook is told the result.

Supports Monad, Base and Arbitrum One, plus Anvil for local development.

> **Status:** passes its full benchmark and fault-injection suite on Anvil. It has not yet been run
> against Monad, Base or Arbitrum, or against real AWS KMS keys. See [Status](#status).

## Quickstart

You need Rust 1.95+, Docker and [Foundry](https://getfoundry.sh) (for `anvil`).

```bash
git clone https://github.com/nkrishang/gum-engine && cd gum-engine
docker compose up -d                     # Postgres on :54330, Anvil on :8545 with 1s blocks
cargo run --release -p gum-engine        # reads config/default.toml, serves on :8080
```

In a second terminal, start a webhook receiver and send a transaction:

```bash
cargo run --release -p gum-bench -- sink --port 9000 &

curl -s localhost:8080/v1/transactions -H 'content-type: application/json' -d '{
  "chain_id": 31337,
  "to": "0x000000000000000000000000000000000000dEaD",
  "value": "1",
  "gas_limit": 21000,
  "webhook": { "url": "http://127.0.0.1:9000/ok" }
}'
# => {"job_id":"…"}

curl -s localhost:8080/v1/transactions/<job_id>
```

The receiver prints `transaction.included` and then `transaction.confirmed`. The default config uses
Anvil's public dev keys: account 0 is the treasury and accounts 1 to 5 are the signers.

## API

| Route | Purpose |
|---|---|
| `POST /v1/transactions` | submit an unsigned transaction, returns `202 {"job_id"}` |
| `GET /v1/transactions/{job_id}` | job status, attempts, receipt fields and webhook deliveries |
| `GET /v1/signers/balances` | balance of every signer and treasury on every chain |
| `GET /v1/signers` | whether each signer is idle, busy or paused, and why |
| `GET /v1/chains` | chain health and RPC usage |
| `GET /v1/analytics/transactions`, `/v1/analytics/gas` | counts and gas spent by chain, signer and both |

- If you send `gas_limit`, it is used as given and nothing is simulated. If you omit it, the engine
  estimates gas, and a reverting simulation fails the job before it costs anything.
- Each transaction produces two webhooks: `transaction.included` on receipt, and
  `transaction.confirmed` after a per-chain delay. Jobs that never reach the chain get
  `transaction.failed`. Webhooks are HMAC-signed and delivered at least once.
- An `Idempotency-Key` header makes retries safe.

Full contract: [`docs/api-contract.md`](docs/api-contract.md).

## How it works

```
POST /v1/transactions ─► Postgres (durable) ─► per-chain queue
                                                   │
                  one worker per (signer, chain) ◄─┘
                  gas ► fees ► funds check ► sign ► persist ► send ► receipt
                                                                       │
                          webhook dispatcher (own pool) ◄── outbox ◄───┘
```

- Each (signer, chain) pair has one transaction in flight at a time, and pairs never wait on each
  other. Throughput per chain is roughly signers ÷ block time.
- A job is written to Postgres before the `202`. The signed transaction and its nonce are written
  before the first broadcast. A restart or crash therefore never loses a job, sends one twice or reuses
  a nonce.
- Background jobs watch chain health and signer balances. A signer that runs low is topped up from the
  chain's treasury. If a chain has an outage or a re-org, or the treasury runs dry, the affected pairs
  pause, recover on their own, and resume.

## Infrastructure choices

| Choice | Why |
|---|---|
| **Rust, tokio, axum** | One small process runs every signer concurrently. It uses about 3% of a core and 17 MB at 5 tx/s. |
| **Postgres only, no Redis** | Postgres is the source of truth for jobs, nonces and webhooks. Hot state lives in process memory and is rebuilt from Postgres on boot. One datastore means fewer ways to disagree. |
| **Single instance with a leader lease** | One process owns all nonces, so there is no distributed locking on the send path. A Postgres advisory lock plus a fencing epoch covers deploys, where two instances briefly overlap. |
| **AWS KMS signers** | Private keys never leave KMS. Each transaction costs exactly one `Sign` call. Local keys sit behind the same interface for development. |
| **QuickNode RPC** | The engine is built around its per-response billing: it sends with `eth_sendRawTransactionSync`, which returns the receipt in the same call, and tracks nonces, fees and balances locally. That is about 1.3 RPC calls per transaction. |
| **Own JSON-RPC client** | `alloy` is used for primitives and signing only. A thin client over `reqwest` gives exact control of what goes over the wire, and separates "the node said no" from "we don't know if the node saw this", which nonce safety depends on. |
| **Railway** | Dockerfile deploy on a private network, with no public domain since the API has no auth. The engine runs its own migrations at boot. |

Chain-specific behaviour lives behind one trait, `ChainAdapter`. Adding a chain of a supported kind
(OP Stack, Arbitrum Orbit, any standard EIP-1559 chain) is a config block. A new kind is one file plus
recorded fixtures. See [`docs/adding-a-chain.md`](docs/adding-a-chain.md).

## Tests and benchmarks

```bash
cargo test -p gum-engine                       # unit tests, adapter conformance, chain-agnostic guard

cargo build --release -p gum-engine -p gum-bench
./target/release/gum-bench run smoke --postgres-url postgres://gum_engine:gum_engine@127.0.0.1:54330/postgres
./target/release/gum-bench compare bench/baselines/smoke.json bench/results/<new>.json
```

`gum-bench` uses the service only as an outside consumer would: the HTTP API, its own webhook receiver,
and the chain. It starts its own Anvil, a fresh database and the engine binary. Every run ends with a
correctness verdict (each job executed on-chain exactly once, no nonce gaps or reuse, webhooks complete
and in order, analytics and balances equal to the chain), and `compare` fails on a regression in
correctness, RPC calls per transaction, throughput or latency.

Scenarios: `smoke`, `steady`, `saturation`, `burst`, `mixed` (several chains), `webhook-hostile`,
`funds`, `rpc-faults`, `chaos` (`kill -9` under load) and `soak`. Details in
[`crates/gum-bench/README.md`](crates/gum-bench/README.md).

Measured on Anvil with 1s blocks, on an M1 Pro:

| | |
|---|---|
| Throughput | 10 signers sustain 10.0 tx/s, the ceiling for one in-flight transaction per signer |
| Accept latency | p50 4.8 ms, p99 14 ms |
| Accept to included | p50 470 ms, p99 947 ms, which is the block time |
| RPC calls per transaction | 1.00 with a gas limit, 1.31 on a mixed load |

## Deploying

See [`docs/deploying.md`](docs/deploying.md) for the Railway, AWS KMS and QuickNode setup, scaling the
signer pool, the RPC budget, and the operator routes.

## Status

Verified on Anvil: every benchmark scenario above passes with a clean verdict, including three
`kill -9`s under load, an RPC outage, 84 injected RPC faults, a drained treasury and a re-org.

Not yet verified:

- No run against Monad, Base or Arbitrum, and none with real KMS keys. Those adapters pass the
  conformance suite against fixtures assembled from chain docs and client source, not payloads captured
  from live nodes. A testnet run is the next step.
- A Postgres restart under load has not been tested.

## Repository layout

```
crates/gum-engine/   the service
  src/chain/         ChainAdapter and one file per chain kind
  src/pipeline/      workers, the per-nonce send loop, recovery
  src/store/         Postgres: jobs, attempts, leader lease, webhook outbox
  src/funds/         balance ledger, treasury, balance monitor
  src/rpc/           JSON-RPC client, rate limiter, credit meter
crates/gum-bench/    benchmark and fault-injection harness
bench/baselines/     committed benchmark baselines
fixtures/<kind>/     recorded errors and receipts per chain kind
config/              default.toml (local), production.toml
docs/                API contract, adding a chain, deploying
```
