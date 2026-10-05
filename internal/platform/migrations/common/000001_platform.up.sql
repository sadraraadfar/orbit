-- Platform tables shared by every Orbit service database: the transactional
-- outbox and the processed-message log used for idempotent consumption.
-- These live in a version range (1) reserved for platform migrations so that
-- per-service migrations can start at version 2.

CREATE TABLE outbox (
    seq            bigserial PRIMARY KEY,
    id             uuid NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    aggregate_type text NOT NULL,
    aggregate_id   text NOT NULL,
    event_type     text NOT NULL,
    event_version  int  NOT NULL,
    topic          text NOT NULL,
    payload        jsonb NOT NULL,
    headers        jsonb NOT NULL DEFAULT '{}'::jsonb,
    occurred_at    timestamptz NOT NULL,
    created_at     timestamptz NOT NULL DEFAULT now(),
    available_at   timestamptz NOT NULL DEFAULT now(),
    published_at   timestamptz,
    attempts       int NOT NULL DEFAULT 0,
    last_error     text
);

CREATE INDEX outbox_pending_idx ON outbox (available_at, seq) WHERE published_at IS NULL;
CREATE INDEX outbox_aggregate_idx ON outbox (aggregate_type, aggregate_id);

CREATE TABLE processed_messages (
    consumer     text NOT NULL,
    event_id     uuid NOT NULL,
    topic        text NOT NULL,
    processed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (consumer, event_id)
);
