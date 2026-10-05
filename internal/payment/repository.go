// Package payment authorizes and refunds payments. Refunds are the compensating
// action for a fulfillment failure.
package payment

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/outbox"
)

// ErrPaymentNotFound indicates a compensation for an unknown payment.
var ErrPaymentNotFound = errors.New("payment not found")

// ErrPaymentNotRefundable indicates a refund was requested for a payment that
// is not in an authorized state.
var ErrPaymentNotRefundable = errors.New("payment not refundable")

// Repository persists payments.
type Repository struct {
	store *outbox.Store
}

// NewRepository constructs a payment repository.
func NewRepository(store *outbox.Store) *Repository {
	return &Repository{store: store}
}

// Store exposes the outbox store to the handler.
func (r *Repository) Store() *outbox.Store { return r.store }

// Authorize records an authorized payment. It is idempotent on order_id.
func (r *Repository) Authorize(ctx context.Context, tx pgx.Tx, orderID string, amountCents int64, currency string) (string, error) {
	return r.upsert(ctx, tx, orderID, "authorized", amountCents, currency, "", "")
}

// Fail records a failed payment. It is idempotent on order_id.
func (r *Repository) Fail(ctx context.Context, tx pgx.Tx, orderID, reason, class string) (string, error) {
	return r.upsert(ctx, tx, orderID, "failed", 0, "", reason, class)
}

func (r *Repository) upsert(ctx context.Context, tx pgx.Tx, orderID, status string, amountCents int64, currency, reason, class string) (string, error) {
	id := uuid.NewString()
	tag, err := tx.Exec(ctx, `
		INSERT INTO payments (id, order_id, status, amount_cents, currency, reason, class)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (order_id) DO NOTHING`,
		id, orderID, status, amountCents, currency, reason, class)
	if err != nil {
		return "", fmt.Errorf("payment: insert: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return id, nil
	}
	// A payment already exists for this order; return its id.
	if err := tx.QueryRow(ctx, `SELECT id FROM payments WHERE order_id = $1`, orderID).Scan(&id); err != nil {
		return "", fmt.Errorf("payment: read existing: %w", err)
	}
	return id, nil
}

// Refund marks an authorized payment as refunded. Already-refunded payments are
// a safe no-op.
func (r *Repository) Refund(ctx context.Context, tx pgx.Tx, orderID, reason string) (paymentID string, refunded bool, err error) {
	var status string
	err = tx.QueryRow(ctx, `
		SELECT id, status FROM payments WHERE order_id = $1 FOR UPDATE`, orderID,
	).Scan(&paymentID, &status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("%w: order %s", ErrPaymentNotFound, orderID)
	}
	if err != nil {
		return "", false, fmt.Errorf("payment: lock: %w", err)
	}
	switch status {
	case "refunded":
		return paymentID, false, nil
	case "authorized":
		if _, err := tx.Exec(ctx, `
			UPDATE payments SET status = 'refunded', reason = $2, updated_at = now()
			WHERE order_id = $1`, orderID, reason); err != nil {
			return "", false, fmt.Errorf("payment: refund: %w", err)
		}
		return paymentID, true, nil
	default:
		return paymentID, false, fmt.Errorf("%w: status %s", ErrPaymentNotRefundable, status)
	}
}
