-- Event log: every webhook delivery from either side, exactly once.
-- UNIQUE (source, delivery_id) is the idempotency key: a redelivered webhook
-- hits the constraint and is dropped before any work happens.
CREATE TABLE IF NOT EXISTS events (
    id              BIGSERIAL PRIMARY KEY,
    source          TEXT        NOT NULL,                  -- github | devrev | reconcile
    delivery_id     TEXT        NOT NULL,                  -- X-GitHub-Delivery or the DevRev event id
    event_type      TEXT        NOT NULL,
    object_key      TEXT        NOT NULL,                  -- owner/repo#123
    payload         JSONB       NOT NULL,
    status          TEXT        NOT NULL DEFAULT 'pending', -- pending | done | dead
    attempts        INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_until    TIMESTAMPTZ,                           -- lease held by a worker
    last_error      TEXT,
    coalesced       BOOLEAN     NOT NULL DEFAULT false,    -- finished by a later sync of the same object
    received_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    done_at         TIMESTAMPTZ,
    UNIQUE (source, delivery_id)
);
CREATE INDEX IF NOT EXISTS events_ready ON events (next_attempt_at) WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS events_object ON events (object_key) WHERE status = 'pending';

-- One row per synced GitHub issue or pull request.
CREATE TABLE IF NOT EXISTS links (
    github_key TEXT PRIMARY KEY,           -- owner/repo#123
    kind       TEXT        NOT NULL,       -- issue | pull
    work_id    TEXT        NOT NULL UNIQUE,
    gh_seen_at TIMESTAMPTZ NOT NULL,       -- GitHub updated_at when last in sync
    dv_seen_at TIMESTAMPTZ NOT NULL,       -- DevRev modified_date when last in sync
    gh_status  TEXT        NOT NULL,       -- status each side had when last in sync: the merge base
    dv_status  TEXT        NOT NULL,
    synced_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
