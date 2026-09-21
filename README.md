# gum-engine

A durable, concurrent EVM transaction processing service. Callers `POST` an unsigned transaction and get a
job id back immediately; a pool of N AWS KMS signers broadcasts the jobs concurrently, each job is tracked
to a receipt, and the caller's webhook is told the result.

Chains: Monad, Base, Arbitrum One (QuickNode) and Anvil for local development. Deploys on Railway.

```
POST /v1/transactions ─► group-commit INSERT (durable) ─► per-chain in-memory FIFO ─┐
                                                                                   ▼
        N signers × C chains = independent pair workers (one task per (signer, chain))
        pop ► gas (given | estimated) ► fees from local oracle ► ledger check / treasury top-up
            ► sign (1 KMS call) ► BIND + persist attempt (1 txn) ► sync-send ► receipt ► signer free
                                                                                   │
        webhook dispatcher (DB outbox, own pool) ◄─────────────────────────────────┘
        background: chain monitor · confirmation verifier · balance monitor + treasury · queue sweep
```

- API and webhook contract: [`docs/api-contract.md`](docs/api-contract.md)
- Adding a chain: [`docs/adding-a-chain.md`](docs/adding-a-chain.md)
- Benchmark harness: [`crates/gum-bench/README.md`](crates/gum-bench/README.md)

## Guarantees, and what enforces them

| Guarantee | Mechanism |
|---|---|
| An accepted job is never lost | The row is committed before the `202` (group commit). Restart = reload from Postgres. |
| A job never executes twice | A job is bound to one `(signer, nonce)` for life in the same transaction that stores the signed bytes, *before* the first broadcast (`store::bind_attempt`). Rebroadcasts send the stored bytes; nothing is ever re-signed to "recreate" a transaction. |
| A nonce is never reused | `nonce_slots` primary key is a compare-and-set; the local nonce is `max(chain nonce, highest bound + 1)`; the chain is a floor, never the truth. |
| `failed` means it did not execute | After broadcast, a job fails only on proof: a stateless node reject (on chains where that is trustworthy) or a *confirmed* different transaction at its nonce. Otherwise it stays non-terminal and the pair asks for an operator. |
| Only one instance drives signers | Postgres session advisory lock + `lease_epoch` fencing on every write that can lead to a broadcast. Railway deploys always overlap; the standby serves the API (jobs stay durable) and takes over when the lease frees. |
| Signers never block each other | One task per (signer, chain); the only shared structures are the queue and the RPC limiter. |
| Webhooks never slow transactions | Outbox row in the same transaction as the state change; delivery is a separate pool with per-host caps and breakers. |

Pair states are visible at `GET /v1/signers`: `idle | busy | draining | paused`, with the reason
(`chain_outage`, `reorg`, `nonce_drift`, `insufficient_funds`, `stuck_unresolved`, `signer_unavailable`,
`manual`, `booting`) and what is being done about it (`recovery_step`).

## RPC economy

QuickNode bills per successful response and a JSON-RPC batch of N costs N — so savings come from not
calling at all:

| Need | How | Calls |
|---|---|---|
| Broadcast + receipt | `eth_sendRawTransactionSync` (probed at boot; loud fallback to send + poll) | 1 / tx |
| Nonce | local | 0 |
| Fees | learned from our own receipts; one block read only when stale | ~0 |
| Gas limit | caller's value is used as-is; estimated only when absent (revert ⇒ early failure) | 0–1 |
| Balances | local ledger; one Multicall3 read per chain per sweep | 1 / 5 min |
| Soft confirmation | one block read per inclusion block (none on chains that finalize on inclusion) | ≤ 1 / tx |
| Liveness | passive (every receipt is a head sighting); probe only after 60s of silence | ~0 busy, 1/min idle |

At ~2 calls per transaction the Build plan (80M credits/month, 50 RPS) carries roughly 2M Base/Arbitrum or
1.3M Monad transactions a month. `GET /v1/chains` shows calls and credits per method and the projected
monthly burn; `rpc.monthly_credit_budget` alerts and `rpc.daily_credit_cap` can shed *new* jobs.

## Run it locally

```bash
docker compose up -d                 # Postgres :54329, Anvil :8545 (1s blocks)
cargo run -p gum-engine              # reads config/default.toml (local Anvil + the compose Postgres)

cargo run -p gum-bench -- sink --port 9000 &    # a webhook receiver that prints events
curl -s localhost:8080/v1/transactions -H 'content-type: application/json' -d '{
  "chain_id": 31337, "to": "0x000000000000000000000000000000000000dEaD", "value": "1",
  "gas_limit": 21000, "webhook": {"url": "http://127.0.0.1:9000/ok"}}'
```

Anvil is the local chain and finalizes immediately: confirmation delay is 0, both webhooks fire back to
back, and nothing about it is delayed or interpreted.

## Test and benchmark first

```bash
cargo test -p gum-engine                          # unit + adapter conformance + chain-agnostic guard
cargo build --release -p gum-engine
cargo run --release -p gum-bench -- run smoke     # black-box: real engine, real Anvil, real Postgres
cargo run --release -p gum-bench -- compare bench/baselines/smoke.json bench/results/<new>.json
```

`gum-bench` uses the service only as an external consumer would. Every run ends with a correctness verdict
(each job executed on-chain exactly once, no nonce gaps or reuse, webhooks complete and in order,
analytics and balances equal ground truth) and `compare` fails on any violation, any increase in RPC calls
per transaction, or a throughput / p99 regression. Perf-affecting changes attach a `compare` report.

## Deploying on Railway — checklist

1. **Postgres**: add Railway Postgres; enable volume backups and point-in-time recovery. Use the private
   `DATABASE_URL`. Never put PgBouncer in front of it (the lease is a session-level advisory lock).
2. **No public domain.** The API has no authentication; do not click "Generate Domain" or enable a TCP
   proxy. Callers reach it at `http://<service>.railway.internal:$PORT`.
3. **Sealed variables**: `DATABASE_URL`, `GUM_WEBHOOK__SIGNING_SECRET`, `GUM_SIGNERS__KMS_KEY_IDS`,
   `GUM_RPC_*`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`.
4. **AWS**: an IAM user limited to `kms:Sign` + `kms:GetPublicKey` on the signer and treasury key ARNs.
   Keys are `ECC_SECG_P256K1` / sign-verify. Reference keys by id or ARN, never by alias. The ECC signing
   quota is 1,000 requests/second per account-region, shared by all keys — adding signers adds nonce
   lanes, not signing throughput.
5. **QuickNode**: turn **MEV Protection off** on the Base and Arbitrum endpoints (it is on by default, hides
   transactions from the public path and has no fallback if the relay drops one). Confirm the Arbitrum
   endpoint has the "Synchronous SendTransaction" add-on.
6. **Fund the treasuries** on each chain. On Monad every account keeps an unspendable 10 MON reserve —
   fund signers and treasury well above it.
7. `railway.toml` already sets `healthcheckPath=/healthz`, `drainingSeconds=30`, `numReplicas=1`,
   migrations in `preDeployCommand`.

Scaling the pool: add KMS key ids to `GUM_SIGNERS__KMS_KEY_IDS` and redeploy. New signers are registered,
funded from each chain's treasury, and start taking jobs. Peak throughput per chain ≈ signers ÷ inclusion
time.

## Operating it

```
GET  /v1/signers                 pair states, pause reasons, recovery steps
GET  /v1/signers/balances        confirmed / reserved / available per signer and treasury
GET  /v1/chains                  chain health, send mode, RPC calls + credits by method
GET  /v1/analytics/transactions  queued / processing / in-flight / included / succeeded / reverted / failed
GET  /v1/analytics/gas           gas used + native spent by chain, signer, both, and purpose
POST /v1/admin/pairs/{chain}/{signer}/pause|resume|recover
POST /v1/admin/chains/{chain}/pause|resume|probe        POST /v1/admin/pause|resume  (global kill switch)
POST /v1/admin/jobs/{job_id}/cancel                     POST /v1/admin/webhooks/{event_id}/redeliver
```

Logs are single-line JSON with a stable `event` code and `chain` / `signer` / `job_id` / `nonce` /
`tx_hash` fields (Railway: `@event:pair.paused`, `@alert:true`). There is one `info` line per job, at the
moment its transaction is mined, carrying the timing breakdown; everything an operator must act on is
`alert=true` and is also POSTed to `alerts.webhook_url` when set.
