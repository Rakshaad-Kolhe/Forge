-- NOTE: not executed; migrations run from OUTBOX_EVENTS_SQL in migrator.ts. Kept for directory consistency.
-- Forge V2: PR 21 - Durable Transactional Outbox
CREATE TABLE IF NOT EXISTS outbox_events (
  id                     VARCHAR(255) PRIMARY KEY,
  event_id               VARCHAR(255) NOT NULL,
  event_type             VARCHAR(64)  NOT NULL,
  version                INTEGER      NOT NULL,
  occurred_at            TIMESTAMPTZ  NOT NULL,
  pipeline_id            VARCHAR(255),
  run_id                 VARCHAR(255),
  job_id                 VARCHAR(255),
  attempt_id             VARCHAR(255),
  worker_id              VARCHAR(255),
  payload                JSONB        NOT NULL,
  status                 VARCHAR(20)  NOT NULL DEFAULT 'PENDING',
  delivery_attempt_count INTEGER      NOT NULL DEFAULT 0,
  dispatch_count         INTEGER      NOT NULL DEFAULT 0,
  available_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  claimed_at             TIMESTAMPTZ,
  claimed_by             VARCHAR(255),
  claim_token            VARCHAR(255),
  published_at           TIMESTAMPTZ,
  last_error             TEXT,
  created_at             TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
  CONSTRAINT uq_outbox_events_event_id UNIQUE (event_id),
  CONSTRAINT chk_outbox_events_status CHECK (status IN ('PENDING','CLAIMED','PUBLISHED','DEAD'))
);

CREATE INDEX IF NOT EXISTS idx_outbox_events_claimable
  ON outbox_events (available_at, occurred_at, id) WHERE status = 'PENDING';
CREATE INDEX IF NOT EXISTS idx_outbox_events_claimed
  ON outbox_events (claimed_at) WHERE status = 'CLAIMED';
CREATE INDEX IF NOT EXISTS idx_outbox_events_retention
  ON outbox_events (published_at) WHERE status = 'PUBLISHED';
