-- Notification service schema: one notification per (order, topic).

CREATE TABLE notifications (
    id         uuid PRIMARY KEY,
    order_id   uuid NOT NULL,
    channel    text NOT NULL,
    topic      text NOT NULL,
    status     text NOT NULL,
    reason     text NOT NULL DEFAULT '',
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (order_id, topic)
);

CREATE INDEX notifications_order_idx ON notifications (order_id);
