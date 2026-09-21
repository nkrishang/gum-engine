-- gum-engine initial schema.
-- Conventions: addresses and hashes are lowercase 0x-hex TEXT; wei amounts are NUMERIC(78,0);
-- calldata and raw signed transactions are BYTEA.
-- Migrations run while an older leader may still be live: only expand/contract changes after this one.

-- Fencing token. Bumped each time an instance acquires the leader lease; every write that can lead to a
-- broadcast is gated on it, so a zombie leader can never bind a nonce.
CREATE TABLE lease_epoch (
    id    BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (id),
    epoch BIGINT  NOT NULL
);
INSERT INTO lease_epoch (id, epoch) VALUES (TRUE, 0);

CREATE TABLE signers (
    address       TEXT PRIMARY KEY,
    key_ref       TEXT        NOT NULL,           -- KMS key id, or 'local'
    role          TEXT        NOT NULL,           -- signer | treasury
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE jobs (
    id                  UUID PRIMARY KEY,
    seq                 BIGSERIAL   NOT NULL UNIQUE,
    chain_id            BIGINT      NOT NULL,
    idempotency_key     TEXT UNIQUE,
    request_hash        TEXT        NOT NULL,     -- detects same-key / different-body replays
    to_addr             TEXT        NOT NULL,
    data                BYTEA       NOT NULL,
    value               NUMERIC(78, 0) NOT NULL,
    gas_limit           BIGINT,                   -- caller supplied; NULL => engine estimates
    deadline            TIMESTAMPTZ,
    webhook_url         TEXT        NOT NULL,
    status              TEXT        NOT NULL,     -- queued | sent | included | confirmed | cancelling | failed
    outcome             TEXT,                     -- success | reverted
    requeue_rank        BIGINT      NOT NULL DEFAULT 0,  -- lower pops first; front-requeue uses negatives
    requeue_count       INT         NOT NULL DEFAULT 0,
    signer              TEXT,
    nonce               BIGINT,
    tx_hash             TEXT,
    block_number        BIGINT,
    block_hash          TEXT,
    gas_used            NUMERIC(78, 0),
    effective_gas_price NUMERIC(78, 0),
    fee_paid            NUMERIC(78, 0),
    l1_fee              NUMERIC(78, 0),
    error_code          TEXT,
    error_message       TEXT,
    revert_data         BYTEA,
    webhook_seq         INT         NOT NULL DEFAULT 0,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    sent_at             TIMESTAMPTZ,
    included_at         TIMESTAMPTZ,
    confirmed_at        TIMESTAMPTZ,
    failed_at           TIMESTAMPTZ
);
CREATE INDEX jobs_queue_idx ON jobs (chain_id, requeue_rank, seq) WHERE status = 'queued';
CREATE INDEX jobs_live_idx ON jobs (chain_id, signer) WHERE status IN ('sent', 'included', 'cancelling');

-- Compare-and-set binding of a nonce to exactly one owner (a job, a top-up or a cancel).
CREATE TABLE nonce_slots (
    chain_id   BIGINT      NOT NULL,
    signer     TEXT        NOT NULL,
    nonce      BIGINT      NOT NULL,
    owner_kind TEXT        NOT NULL,             -- job | topup
    owner_id   UUID        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, signer, nonce)
);

CREATE TABLE topups (
    id         UUID PRIMARY KEY,
    chain_id   BIGINT         NOT NULL,
    treasury   TEXT           NOT NULL,
    signer     TEXT           NOT NULL,
    amount     NUMERIC(78, 0) NOT NULL,
    status     TEXT           NOT NULL,          -- requested | sent | included | confirmed | failed
    error      TEXT,
    created_at TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX topups_live_idx ON topups (chain_id) WHERE status IN ('requested', 'sent', 'included');

-- Every signed transaction is persisted here BEFORE its first broadcast.
CREATE TABLE tx_attempts (
    id                       UUID PRIMARY KEY,
    chain_id                 BIGINT         NOT NULL,
    signer                   TEXT           NOT NULL,
    nonce                    BIGINT         NOT NULL,
    purpose                  TEXT           NOT NULL, -- job | topup | cancel
    job_id                   UUID REFERENCES jobs (id),
    topup_id                 UUID REFERENCES topups (id),
    tx_hash                  TEXT           NOT NULL UNIQUE,
    raw_tx                   BYTEA,                   -- nulled once confirmed
    gas_limit                BIGINT         NOT NULL,
    max_fee_per_gas          NUMERIC(78, 0) NOT NULL,
    max_priority_fee_per_gas NUMERIC(78, 0) NOT NULL,
    value                    NUMERIC(78, 0) NOT NULL,
    status                   TEXT           NOT NULL, -- broadcast | included | confirmed | replaced | dropped
    block_number             BIGINT,
    block_hash               TEXT,
    gas_used                 NUMERIC(78, 0),          -- kept per attempt so a re-org demotion can reverse the ledger exactly
    fee_paid                 NUMERIC(78, 0),
    created_at               TIMESTAMPTZ    NOT NULL DEFAULT now(),
    updated_at               TIMESTAMPTZ    NOT NULL DEFAULT now()
);
CREATE INDEX tx_attempts_nonce_idx ON tx_attempts (chain_id, signer, nonce);
CREATE INDEX tx_attempts_live_idx ON tx_attempts (chain_id, signer) WHERE status IN ('broadcast', 'included');
-- At most one attempt per nonce can ever be mined.
CREATE UNIQUE INDEX tx_attempts_mined_idx ON tx_attempts (chain_id, signer, nonce)
    WHERE status IN ('included', 'confirmed');

CREATE TABLE pairs (
    chain_id      BIGINT      NOT NULL,
    signer        TEXT        NOT NULL,
    role          TEXT        NOT NULL,           -- signer | treasury
    nonce_hwm     BIGINT,                         -- highest nonce ever bound; NULL = none yet
    manual_pause  BOOLEAN     NOT NULL DEFAULT FALSE,
    pause_reason  TEXT,
    recovery_step TEXT,
    pause_detail  TEXT,
    paused_at     TIMESTAMPTZ,
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (chain_id, signer)
);

CREATE TABLE webhook_deliveries (
    id               UUID PRIMARY KEY,            -- event id
    job_id           UUID        NOT NULL REFERENCES jobs (id),
    event            TEXT        NOT NULL,
    version          INT         NOT NULL,        -- bumps when an event is legitimately re-issued (re-inclusion)
    sequence         INT         NOT NULL,
    url              TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    status           TEXT        NOT NULL,        -- pending | delivered | dead
    attempts         INT         NOT NULL DEFAULT 0,
    next_attempt_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_status_code INT,
    last_error       TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at     TIMESTAMPTZ,
    UNIQUE (job_id, event, version)
);
CREATE INDEX webhook_pending_idx ON webhook_deliveries (next_attempt_at) WHERE status = 'pending';
CREATE INDEX webhook_job_idx ON webhook_deliveries (job_id);

-- Cumulative gas accounting, updated in the same transaction as the inclusion it describes.
CREATE TABLE gas_ledger (
    chain_id BIGINT         NOT NULL,
    signer   TEXT           NOT NULL,
    purpose  TEXT           NOT NULL,             -- job | topup | cancel
    tx_count BIGINT         NOT NULL DEFAULT 0,
    gas_used NUMERIC(78, 0) NOT NULL DEFAULT 0,
    fee_paid NUMERIC(78, 0) NOT NULL DEFAULT 0,
    PRIMARY KEY (chain_id, signer, purpose)
);

-- Terminal-state counters so boot never has to scan the jobs table.
CREATE TABLE stats_rollup (
    chain_id BIGINT NOT NULL,
    signer   TEXT   NOT NULL,                     -- '' when the job never reached a signer
    bucket   TEXT   NOT NULL,                     -- succeeded | reverted | failed
    count    BIGINT NOT NULL DEFAULT 0,
    PRIMARY KEY (chain_id, signer, bucket)
);

CREATE TABLE dead_letters (
    id         UUID PRIMARY KEY,
    kind       TEXT        NOT NULL,
    payload    JSONB       NOT NULL,
    error      TEXT        NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
