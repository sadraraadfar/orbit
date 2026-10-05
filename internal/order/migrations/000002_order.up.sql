-- Order service schema: the order aggregate, its distributed timeline, and
-- idempotency keys for order creation.

CREATE TABLE orders (
    id                 uuid PRIMARY KEY,
    customer_id        text NOT NULL,
    items              jsonb NOT NULL,
    total_cents        bigint NOT NULL,
    currency           text NOT NULL,
    status             text NOT NULL,
    inventory_state    text NOT NULL DEFAULT '',
    payment_state      text NOT NULL DEFAULT '',
    fulfillment_state  text NOT NULL DEFAULT '',
    cancel_stage       text NOT NULL DEFAULT '',
    cancel_reason      text NOT NULL DEFAULT '',
    inventory_released boolean NOT NULL DEFAULT false,
    payment_refunded   boolean NOT NULL DEFAULT false,
    faults             jsonb NOT NULL DEFAULT '{}'::jsonb,
    correlation_id     text NOT NULL DEFAULT '',
    created_at         timestamptz NOT NULL,
    updated_at         timestamptz NOT NULL
);

CREATE INDEX orders_customer_idx ON orders (customer_id);
CREATE INDEX orders_status_idx ON orders (status);
CREATE INDEX orders_created_idx ON orders (created_at DESC, id DESC);

CREATE TABLE order_timeline (
    id             bigserial PRIMARY KEY,
    order_id       uuid NOT NULL REFERENCES orders (id) ON DELETE CASCADE,
    event_id       uuid NOT NULL,
    event_type     text NOT NULL,
    occurred_at    timestamptz NOT NULL,
    correlation_id text NOT NULL DEFAULT '',
    causation_id   text NOT NULL DEFAULT '',
    summary        jsonb NOT NULL DEFAULT '{}'::jsonb,
    recorded_at    timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, event_id)
);

CREATE INDEX order_timeline_order_idx ON order_timeline (order_id, occurred_at);

CREATE TABLE idempotency_keys (
    key             text PRIMARY KEY,
    request_hash    text NOT NULL,
    state           text NOT NULL,
    response_status int,
    response_body   jsonb,
    created_at      timestamptz NOT NULL DEFAULT now()
);
