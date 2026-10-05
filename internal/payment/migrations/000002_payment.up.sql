-- Payment service schema: one payment record per order.

CREATE TABLE payments (
    id           uuid PRIMARY KEY,
    order_id     uuid NOT NULL UNIQUE,
    status       text NOT NULL,
    amount_cents bigint NOT NULL DEFAULT 0,
    currency     text NOT NULL DEFAULT '',
    reason       text NOT NULL DEFAULT '',
    class        text NOT NULL DEFAULT '',
    created_at   timestamptz NOT NULL DEFAULT now(),
    updated_at   timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX payments_status_idx ON payments (status);
