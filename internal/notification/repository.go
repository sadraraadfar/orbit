// Package notification simulates delivering customer notifications.
package notification

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/outbox"
)

// Repository persists notifications.
type Repository struct {
	store *outbox.Store
}

// NewRepository constructs a notification repository.
func NewRepository(store *outbox.Store) *Repository {
	return &Repository{store: store}
}

// Store exposes the outbox store to the handler.
func (r *Repository) Store() *outbox.Store { return r.store }

// Send records a delivered notification. It is idempotent on (order_id, topic).
func (r *Repository) Send(ctx context.Context, tx pgx.Tx, orderID, channel, topic string) (string, bool, error) {
	id := uuid.NewString()
	tag, err := tx.Exec(ctx, `
		INSERT INTO notifications (id, order_id, channel, topic, status)
		VALUES ($1, $2, $3, $4, 'sent')
		ON CONFLICT (order_id, topic) DO NOTHING`, id, orderID, channel, topic)
	if err != nil {
		return "", false, fmt.Errorf("notification: insert: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return id, true, nil
	}
	if err := tx.QueryRow(ctx, `
		SELECT id FROM notifications WHERE order_id = $1 AND topic = $2`, orderID, topic).Scan(&id); err != nil {
		return "", false, fmt.Errorf("notification: read existing: %w", err)
	}
	return id, false, nil
}
