-- Fulfillment service schema: one fulfillment record per order.

CREATE TABLE fulfillments (
    id         uuid PRIMARY KEY,
    order_id   uuid NOT NULL UNIQUE,
    status     text NOT NULL,
    reason     text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX fulfillments_status_idx ON fulfillments (status);
