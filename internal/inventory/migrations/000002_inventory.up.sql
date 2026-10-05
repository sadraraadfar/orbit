-- Inventory service schema: stock levels and per-order reservations.

CREATE TABLE inventory_items (
    sku        text PRIMARY KEY,
    available  int NOT NULL DEFAULT 0 CHECK (available >= 0),
    reserved   int NOT NULL DEFAULT 0 CHECK (reserved >= 0),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE reservations (
    id         uuid PRIMARY KEY,
    order_id   uuid NOT NULL UNIQUE,
    status     text NOT NULL,
    items      jsonb NOT NULL,
    reason     text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX reservations_status_idx ON reservations (status);
