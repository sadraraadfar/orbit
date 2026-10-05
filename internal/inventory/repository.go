package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
)

// ErrInsufficientStock indicates that at least one requested SKU lacks stock.
var ErrInsufficientStock = errors.New("insufficient stock")

// ErrReservationNotFound indicates a compensation for an unknown order.
var ErrReservationNotFound = errors.New("reservation not found")

// Repository persists inventory items and reservations.
type Repository struct {
	store *outbox.Store
}

// NewRepository constructs an inventory repository.
func NewRepository(store *outbox.Store) *Repository {
	return &Repository{store: store}
}

// Store exposes the outbox store to the handler.
func (r *Repository) Store() *outbox.Store { return r.store }

// Reserve atomically reserves every item for an order. It locks the relevant
// rows, verifies availability, and only then mutates stock, so a partial
// reservation can never be persisted.
func (r *Repository) Reserve(ctx context.Context, tx pgx.Tx, orderID string, items []events.OrderItem) (string, error) {
	skus := make([]string, 0, len(items))
	for _, it := range items {
		skus = append(skus, it.SKU)
	}

	rows, err := tx.Query(ctx, `SELECT sku, available FROM inventory_items WHERE sku = ANY($1) FOR UPDATE`, skus)
	if err != nil {
		return "", fmt.Errorf("inventory: lock items: %w", err)
	}
	available := make(map[string]int, len(skus))
	for rows.Next() {
		var sku string
		var qty int
		if err := rows.Scan(&sku, &qty); err != nil {
			rows.Close()
			return "", fmt.Errorf("inventory: scan item: %w", err)
		}
		available[sku] = qty
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("inventory: iterate items: %w", err)
	}

	for _, it := range items {
		avail, ok := available[it.SKU]
		if !ok {
			return "", fmt.Errorf("%w: unknown sku %q", ErrInsufficientStock, it.SKU)
		}
		if avail < it.Quantity {
			return "", fmt.Errorf("%w: sku %q requested %d available %d", ErrInsufficientStock, it.SKU, it.Quantity, avail)
		}
	}

	for _, it := range items {
		if _, err := tx.Exec(ctx, `
			UPDATE inventory_items
			SET available = available - $2, reserved = reserved + $2, updated_at = now()
			WHERE sku = $1`, it.SKU, it.Quantity); err != nil {
			return "", fmt.Errorf("inventory: decrement %s: %w", it.SKU, err)
		}
	}

	reservationID := uuid.NewString()
	raw, err := json.Marshal(items)
	if err != nil {
		return "", fmt.Errorf("inventory: encode items: %w", err)
	}
	if _, err := tx.Exec(ctx, `
		INSERT INTO reservations (id, order_id, status, items)
		VALUES ($1, $2, 'reserved', $3)`, reservationID, orderID, raw); err != nil {
		return "", fmt.Errorf("inventory: insert reservation: %w", err)
	}
	return reservationID, nil
}

// Release restores stock for a reservation. It is idempotent: releasing an
// already-released reservation returns released=false without side effects.
func (r *Repository) Release(ctx context.Context, tx pgx.Tx, orderID, reason string) (reservationID string, released bool, err error) {
	var (
		status   string
		itemsRaw []byte
	)
	err = tx.QueryRow(ctx, `
		SELECT id, status, items FROM reservations WHERE order_id = $1 FOR UPDATE`, orderID,
	).Scan(&reservationID, &status, &itemsRaw)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, fmt.Errorf("%w: order %s", ErrReservationNotFound, orderID)
	}
	if err != nil {
		return "", false, fmt.Errorf("inventory: lock reservation: %w", err)
	}
	if status == "released" {
		return reservationID, false, nil
	}

	var items []events.OrderItem
	if err := json.Unmarshal(itemsRaw, &items); err != nil {
		return "", false, fmt.Errorf("inventory: decode reservation items: %w", err)
	}
	for _, it := range items {
		if _, err := tx.Exec(ctx, `
			UPDATE inventory_items
			SET available = available + $2, reserved = GREATEST(reserved - $2, 0), updated_at = now()
			WHERE sku = $1`, it.SKU, it.Quantity); err != nil {
			return "", false, fmt.Errorf("inventory: restore %s: %w", it.SKU, err)
		}
	}
	if _, err := tx.Exec(ctx, `
		UPDATE reservations SET status = 'released', reason = $2, updated_at = now()
		WHERE order_id = $1`, orderID, reason); err != nil {
		return "", false, fmt.Errorf("inventory: mark released: %w", err)
	}
	return reservationID, true, nil
}
