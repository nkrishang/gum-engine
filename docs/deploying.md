# Deploying and operating

gum-engine deploys to [Railway](https://railway.com) as a single service next to a Railway Postgres,
built from the `Dockerfile` in the repo root. The engine applies its own database migrations at boot.

## Setup checklist

1. **Postgres.** Add Railway Postgres to the project. Enable volume backups and point-in-time recovery.
   Use the private `DATABASE_URL`. Do not put PgBouncer in front of it: the leader lease is a
   session-level advisory lock, which transaction pooling breaks.
2. **No public domain.** The API has no authentication. Do not generate a domain or enable a TCP proxy.
   Callers reach the service at `http://<service>.railway.internal:$PORT`.
3. **Variables** (seal all of them):

   | Variable | Value |
   |---|---|
   | `DATABASE_URL` | Railway Postgres, private network URL |
   | `GUM_WEBHOOK__SIGNING_SECRET` | HMAC secret for webhook signatures |
   | `GUM_SIGNERS__KMS_KEY_IDS` | JSON array of KMS key ids or ARNs, e.g. `["1234abcd-…","…"]` |
   | `GUM_CHAINS__MONAD__TREASURY_KEY_ID`, `…__BASE__…`, `…__ARBITRUM__…` | KMS key id of each chain's treasury (one key may serve all chains) |
   | `RPC_URL_MONAD`, `RPC_URL_BASE`, `RPC_URL_ARBITRUM` | QuickNode endpoint URLs |
   | `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | IAM user for KMS |

   Every `GUM_*` variable is parsed as a config override (`__` separates nesting), and an unknown one is
   a boot error. That is why the RPC URL variables are deliberately not prefixed with `GUM_`.
4. **AWS KMS.** Keys are `ECC_SECG_P256K1`, usage sign/verify. The IAM user needs only `kms:Sign` and
   `kms:GetPublicKey` on the signer and treasury key ARNs. Reference keys by id or ARN, never by alias.
   Railway has no OIDC or role assumption, so credentials are IAM user keys.
5. **QuickNode.**
   - Turn **MEV Protection off** on the Base and Arbitrum endpoints. It is on by default, routes sends
     through a private relay, and has no fallback if the relay drops a transaction.
   - Confirm the Arbitrum endpoint has the "Synchronous SendTransaction" add-on. Without it the engine
     falls back to send-and-poll, which costs about three times the RPC calls, and raises an alert at boot.
6. **Fund the treasury** on each chain. Signers are never funded by hand: the treasury gives each one
   `initial_topup_amount` the first time, then `topup_amount` whenever it falls below
   `signer_min_balance`, and alerts when its own balance drops under `treasury_min_balance`.

   Monad needs care. Each account's reserve is `min(10 MON, its balance)`: a signer holding less than
   10 MON pays gas normally, but only balance *above* 10 MON can leave an account as value. So the
   treasury's last 10 MON can never be handed out, and a job that transfers MON needs its signer to hold
   more than 10 MON. Fees are also charged on the gas *limit*, so they are far larger than on the L2s.
7. **Service settings.** Railway ignores `railway.toml` for newly created services, and its replacement
   (`.railway/railway.ts`) has no keys for draining, overlap or restart policy. Set these on the service,
   in the dashboard under Settings or with the `serviceInstanceUpdate` API mutation:

   | Setting | Value | Why |
   |---|---|---|
   | Healthcheck path | `/healthz` | without one, Railway marks a crash-looping deploy as successful |
   | Healthcheck timeout | `120` | |
   | Draining seconds | `30` | the default is 0, an immediate SIGKILL with no graceful drain |
   | Overlap seconds | `0` | |
   | Restart policy | `ALWAYS` | the default gives up after 10 crashes |
   | Replicas | `1` | one instance owns the signers |
   | Region | same as Postgres, close to the KMS region | every send waits on one KMS call and one Postgres write |

## Deploys and restarts

Railway starts the new instance and waits for it to be healthy before it stops the old one, so two
instances overlap on every deploy. Only the one holding the Postgres lease drives signers. The standby
still serves the API, and the jobs it accepts are durable in Postgres until it takes over.

- `/healthz` never depends on holding the lease. If it did, every deploy would deadlock until the
  healthcheck timeout.
- On SIGTERM the engine stops taking jobs, lets in-flight sends settle, flushes pending writes and
  releases the lease. Keep `drainingSeconds` above `server.drain_timeout_ms` (25s by default).
- A `kill -9` is equally safe, only slower to hand over. Every broadcast was persisted first, and the
  next leader recovers in-flight transactions from Postgres.

## Scaling the signer pool

Add KMS key ids to `GUM_SIGNERS__KMS_KEY_IDS` and redeploy. New signers are registered, funded from
each chain's treasury, and start taking jobs.

- Peak throughput per chain is roughly signers ÷ inclusion time, because each (signer, chain) pair has
  one transaction in flight at a time.
- The KMS ECC signing quota is 1,000 requests per second per account and region, shared by all keys.
  More signers add nonce lanes, not signing throughput.

## RPC budget

QuickNode bills per successful response, and a JSON-RPC batch of N costs N, so the engine saves credits
by not calling. Steady state is about 1 call per transaction with a caller-supplied gas limit and about
2 without one, plus the confirmation check on chains that have a confirmation delay.

- `GET /v1/chains` reports calls and credits per method, and the projected monthly burn.
- `rpc.monthly_credit_budget` raises an alert when the projection exceeds it.
- `rpc.daily_credit_cap` refuses new jobs with `503 shedding` once reached. In-flight work always
  finishes.
- `rpc.account_rps` (default 45) is the client-side rate limit shared by every chain.

## Operating

| Route | Shows |
|---|---|
| `GET /v1/signers` | state of every (signer, chain) pair: `idle`, `busy`, `draining` or `paused`, with the reason and the recovery step |
| `GET /v1/signers/balances` | confirmed, reserved and available balance per signer and treasury |
| `GET /v1/chains` | chain health, send mode, queue depth, RPC calls and credits by method |
| `GET /v1/analytics/transactions` | counts by chain, signer and both: queued, processing, in-flight, included, succeeded, reverted, failed |
| `GET /v1/analytics/gas` | gas used and native token spent by chain, signer, both, and purpose |
| `GET /readyz` | leader, database and per-chain boot status |
| `GET /metrics` | Prometheus text |

Admin routes:

```
POST /v1/admin/pairs/{chain_id}/{signer}/pause|resume|recover
POST /v1/admin/chains/{chain_id}/pause|resume|probe
POST /v1/admin/pause | /v1/admin/resume              global kill switch
POST /v1/admin/jobs/{job_id}/cancel                  queued jobs only
POST /v1/admin/webhooks/{event_id}/redeliver
```

A pair pauses itself and reports why: `chain_outage`, `reorg`, `nonce_drift`, `insufficient_funds`,
`stuck_unresolved`, `signer_unavailable`, `manual` or `booting`. Most pauses clear on their own once the
cause is gone. A `recovery_step` of `needs_operator` means the engine could not determine a
transaction's fate. Inspect it, then resume the pair.

## Logs and alerts

Logs are single-line JSON with a stable `event` code plus `chain`, `signer`, `job_id`, `nonce` and
`tx_hash`. In Railway's log explorer, filter with `@event:pair.paused` or `@alert:true`.

- There is one `info` line per job, written when its transaction is mined, with the timing breakdown.
- Repeating errors are collapsed with a `suppressed` count, because Railway drops everything above 500
  lines per second.
- Lines marked `alert=true` need an operator. Set `alerts.webhook_url` to also receive them as JSON.

Railway cannot scrape Prometheus. To use `/metrics`, deploy a Prometheus and Grafana template into the
same project and scrape `http://<service>.railway.internal:$PORT/metrics` over the private network.
