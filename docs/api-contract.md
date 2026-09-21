# gum-engine external contract (v1)

This is the only surface `gum-bench` (and any consumer) may rely on: the HTTP API, webhook deliveries,
and the engine's config file. Changes here are breaking changes.

Conventions: JSON everywhere. Addresses, hashes and byte strings are `0x`-prefixed lowercase hex. Wei
amounts are **decimal strings**. Timestamps are RFC 3339 UTC. Unknown request fields are rejected.

## POST /v1/transactions

```json
{
  "chain_id": 31337,
  "to": "0x5fbdb2315678afecb367f032d93f642f64180aa3",
  "data": "0x",                      // optional, default "0x"
  "value": "0",                      // optional, wei decimal string, default "0"
  "gas_limit": 100000,               // optional. Present => used as-is, never simulated.
                                     //           Absent  => engine estimates; a reverting simulation fails the job early.
  "deadline": "2026-09-21T10:00:00Z",// optional. Not yet bound to a signer by then => failed(expired)
  "webhook": { "url": "http://consumer.internal/hooks/gum" }
}
```

Header `Idempotency-Key: <≤128 chars>` is optional and strongly recommended.

| Status | Body | Meaning |
|---|---|---|
| 202 | `{"job_id":"<uuid>"}` | durably accepted and queued |
| 200 | `{"job_id":"<uuid>","replayed":true}` | same `Idempotency-Key` + same body seen before |
| 409 | error `idempotency_conflict` | same key, different body |
| 400 | error `invalid_request` / `unsupported_chain` / `job_too_expensive` | validation failed |
| 503 | error `queue_full` / `store_unavailable` / `shedding` | back off and retry |

Error body: `{"error":{"code":"<code>","message":"<human text>"}}`.

## GET /v1/transactions/{job_id}

```json
{
  "job_id": "…", "chain_id": 31337,
  "status": "queued | sent | included | confirmed | cancelling | failed",
  "outcome": null,                   // "success" | "reverted" once included
  "request": {"to":"0x…","data":"0x…","value":"0","gas_limit":100000,"deadline":null,"webhook_url":"http://…"},
  "signer": "0x…", "nonce": 12,      // null until bound
  "tx_hash": "0x…", "block_number": 100, "block_hash": "0x…",
  "gas_used": "21000", "effective_gas_price": "1000000007", "fee_paid": "21000000147000", "l1_fee": null,
  "error": null,                     // {"code":"simulation_reverted|invalid_tx|expired|stuck_cancelled|cancelled|internal","message":"…","revert_data":"0x…"}
  "attempts": [{"tx_hash":"0x…","nonce":12,"purpose":"job|cancel","status":"…","gas_limit":"100000",
                "max_fee_per_gas":"…","max_priority_fee_per_gas":"…","created_at":"…"}],
  "webhooks": [{"event":"transaction.included","sequence":1,"status":"pending|delivered|failed|dead",
                "attempts":1,"last_status_code":200,"delivered_at":"…"}],
  "timestamps": {"created_at":"…","sent_at":null,"included_at":null,"confirmed_at":null,"failed_at":null}
}
```

404 → error `not_found`. Terminal statuses are `confirmed` and `failed`; they never change.

## Webhooks

`POST <webhook.url>` with `content-type: application/json`. Delivery is **at-least-once**; dedupe on
`event_id`. A 2xx response acknowledges; anything else is retried with exponential backoff.

| Event | When |
|---|---|
| `transaction.included` | immediately when the receipt arrives (`outcome` = `success` or `reverted`) |
| `transaction.confirmed` | after the chain's `confirmation_delay_ms` re-check (immediately on Anvil) |
| `transaction.failed` | the job reached `failed` (never executed on-chain) |

```json
{
  "event_id": "<uuid>", "event": "transaction.included", "sequence": 1,
  "job_id": "…", "chain_id": 31337, "status": "included", "outcome": "success",
  "tx_hash": "0x…", "block_number": 100, "block_hash": "0x…", "signer": "0x…", "nonce": 12,
  "gas_used": "21000", "effective_gas_price": "…", "fee_paid": "…",
  "reincluded": false, "error": null, "timestamp": "…"
}
```

`sequence` increases per job (1 = first event). Headers: `X-Gum-Event-Id`, `X-Gum-Job-Id`,
`X-Gum-Signature: t=<unix seconds>,v1=<hex hmac_sha256(signing_secret, "<t>.<raw body>")>`.

## Observability routes

- `GET /healthz` → `200 {"status":"ok"}` as soon as the process serves HTTP (never depends on leadership).
- `GET /readyz` → `200`/`503` `{"leader":true,"db":true,"chains":{"31337":"healthy"},"booting":[]}`.
- `GET /metrics` → Prometheus text.
- `GET /v1/signers` →
  `{"pairs":[{"chain_id":31337,"signer":"0x…","role":"signer|treasury","state":"idle|busy|draining|paused",
  "pause":null|{"reason":"…","recovery_step":"…","since":"…","detail":"…"},"current_job":null|"<uuid>","next_nonce":12}]}`
- `GET /v1/signers/balances` →
  `{"chains":[{"chain_id":31337,"name":"anvil","last_reconciled_at":"…","treasury":{BAL},"signers":[{BAL}]}]}`
  where `BAL = {"address":"0x…","confirmed":"…","reserved":"…","available":"…","min_balance":"…","low":false}`.
- `GET /v1/chains` →
  `{"chains":[{"chain_id":31337,"name":"anvil","kind":"geth","status":"healthy|degraded|down","head_number":100,
  "last_observed_at":"…","send_mode":"sync|async","queue_depth":0,
  "rpc":{"calls":0,"errors":0,"credits_used":0,"credits_projected_month":0,"by_method":{"eth_sendRawTransactionSync":0}}}]}`
- `GET /v1/analytics/transactions` →
  `{"totals":{COUNTS},"by_chain":[{"chain_id":31337,COUNTS}],"by_signer":[{"signer":"0x…",COUNTS}],
  "by_chain_signer":[{"chain_id":31337,"signer":"0x…",COUNTS}]}` where
  `COUNTS = "queued","processing","in_flight","included","succeeded","reverted","failed","total"` (integers;
  `queued` is 0 in signer breakdowns — a queued job has no signer yet).
- `GET /v1/analytics/gas` →
  `{"by_chain":[{"chain_id":31337,GAS}],"by_signer":[{"signer":"0x…",GAS}],"by_chain_signer":[{…}]}` where
  `GAS = {"tx_count":0,"gas_used":"0","fee_paid":"0","by_purpose":{"job":{…},"topup":{…},"cancel":{…}}}`.
- Admin: `POST /v1/admin/pairs/{chain_id}/{signer}/pause|resume|recover`,
  `POST /v1/admin/chains/{chain_id}/pause|resume`, `POST /v1/admin/jobs/{job_id}/cancel` (queued only),
  `POST /v1/admin/webhooks/{event_id}/redeliver`.

## Clarifications

- **Analytics `COUNTS`** are buckets a job is in *right now*; every job is in exactly one:
  `queued` (durable, not yet picked) → `processing` (picked by a signer, no nonce bound yet) →
  `in_flight` (bound + broadcast; includes `cancelling`) → `included` (mined, awaiting the confirmation
  re-check) → `succeeded` | `reverted` (confirmed, by outcome) or `failed`. At rest only the last three are
  non-zero. On chains with `confirmation_delay_ms = 0`, `included` is never observable.
- **Webhook `sequence`** starts at 1 per job and rises by one per event, whatever the event is
  (`transaction.failed` on a job that never reached the chain has sequence 1). Events of one job are
  delivered in sequence order; different jobs never wait on each other. After a re-org a job gets a second
  `transaction.included` with `reincluded: true` and the next sequence number.
- **Signature timestamp**: every delivery attempt is signed afresh (`t` = time of the attempt), so
  receivers can enforce a tolerance window; `event_id` is stable across attempts.
- **`pause.reason`** ∈ `booting | chain_outage | reorg | nonce_drift | insufficient_funds |
  stuck_unresolved | signer_unavailable | manual`. **`pause.recovery_step`** ∈ `awaiting_chain_head |
  reconciling_nonce | rebroadcasting | awaiting_treasury_topup | awaiting_treasury_refill |
  awaiting_operator_resume | needs_operator`.
- **`/readyz`** is 200 only when this instance is the leader, the database answers, and every chain's pairs
  have finished their boot reconciliation (`"booting": []`). `/healthz` has none of these conditions.
- **Balances** are the engine's ledger, which leads the chain (it already reflects receipts the RPC may
  not show yet) and is reconciled with it every `balance_sweep_interval_ms` (5 min by default), or within
  seconds while any pair is waiting for funds.
- **Send method**: `eth_sendRawTransactionSync` when the endpoint supports it (probed at boot, reported as
  `send_mode` on `/v1/chains`), otherwise `eth_sendRawTransaction` + receipt polling.

## Engine process contract

`gum-engine` reads a TOML file from `$GUM_CONFIG` (default `config/default.toml`), then applies
`GUM_*` env overrides (`__` separates nesting: `GUM_DATABASE__URL`). `$PORT` overrides `server.port`.
`$DATABASE_URL` overrides `database.url`. Logs are single-line JSON on stdout. SIGTERM drains gracefully.

```toml
[server]
port = 8080

[database]
url = "postgres://gum:gum@127.0.0.1:54329/gum"
auto_migrate = true            # run embedded migrations at boot

[webhook]
signing_secret = "dev-secret"
allow_private_hosts = true

[rpc]
account_rps = 45               # global budget across all chains

[signers]
mode = "local"                 # "local" (private keys) | "kms"
local_private_keys = ["0x…"]   # mode = local
kms_key_ids = []               # mode = kms

[chains.anvil]                 # table name = display name
chain_id = 31337
kind = "geth"                  # geth | opstack | arbitrum | monad
rpc_url = "http://127.0.0.1:8545"      # or rpc_url_env = "GUM_RPC_ANVIL"
treasury_private_key = "0x…"           # local mode; kms mode: treasury_key_id = "…"
signer_min_balance = "1000000000000000000"
topup_amount = "5000000000000000000"
treasury_min_balance = "100000000000000000000"
# any kind-level tunable may be overridden here, e.g. confirmation_delay_ms, stuck_after_blocks, max_fee_cap_wei
```
