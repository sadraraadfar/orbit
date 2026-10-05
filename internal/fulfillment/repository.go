// Package fulfillment performs the physical fulfillment step. It is triggered
// by a successful payment authorization.
package fulfillment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/outbox"
)

// ErrFulfillmentNotFound indicates a completion/failure for an unknown order.
var ErrFulfillmentNotFound = errors.New("fulfillment not found")

// Repository persists fulfillments.
type Repository struct {
	store *outbox.Store
}

// NewRepository constructs a fulfillment repository.
func NewRepository(store *outbox.Store) *Repository {
	return &Repository{store: store}
}

// Store exposes the outbox store to the handler.
func (r *Repository) Store() *outbox.Store { return r.store }

// Start creates a started fulfillment for an order. It is idempotent on
// order_id.
func (r *Repository) Start(ctx context.Context, tx pgx.Tx, orderID string) (string, error) {
	id := uuid.NewString()
	tag, err := tx.Exec(ctx, `
		INSERT INTO fulfillments (id, order_id, status)
		VALUES ($1, $2, 'started')
		ON CONFLICT (order_id) DO NOTHING`, id, orderID)
	if err != nil {
		return "", fmt.Errorf("fulfillment: insert: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return id, nil
	}
	if err := tx.QueryRow(ctx, `SELECT id FROM fulfillments WHERE order_id = $1`, orderID).Scan(&id); err != nil {
		return "", fmt.Errorf("fulfillment: read existing: %w", err)
	}
	return id, nil
}

// Complete transitions a started fulfillment to completed. It returns
// changed=false when the transition was already applied.
func (r *Repository) Complete(ctx context.Context, tx pgx.Tx, orderID string) (string, bool, error) {
	return r.transition(ctx, tx, orderID, "started", "completed", "")
}

// Fail transitions a started fulfillment to failed.
func (r *Repository) Fail(ctx context.Context, tx pgx.Tx, orderID, reason string) (string, bool, error) {
	return r.transition(ctx, tx, orderID, "started", "failed", reason)
}

func (r *Repository) transition(ctx context.Context, tx pgx.Tx, orderID, from, to, reason string) (string, bool, error) {
	var (
		id     string
		status string
	)
	err := tx.QueryRow(ctx, `SELECT id, status FROM fulfillments WHERE order_id = $1 FOR UPDATE`, orderID).Scan(&id, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("%w: order %s", ErrFulfillmentNotFound, orderID)
	}
	if err != nil {
		return "", false, fmt.Errorf("fulfillment: lock: %w", err)
	}
	if status == to {
		return id, false, nil // idempotent no-op
	}
	if status != from {
		return id, false, fmt.Errorf("fulfillment: cannot transition from %s to %s", status, to)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE fulfillments SET status = $2, reason = $3, updated_at = now()
		WHERE order_id = $1`, orderID, to, reason); err != nil {
		return "", false, fmt.Errorf("fulfillment: update: %w", err)
	}
	return id, true, nil
}
