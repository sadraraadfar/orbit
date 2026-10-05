// Package inventory reserves and releases stock in response to workflow
// events. It owns the inventory database and never reads other services'
// tables.
package inventory

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
)

// Handler implements the inventory choreography reactions.
type Handler struct {
	repo *Repository
	log  *slog.Logger
}

// NewHandler constructs an inventory Handler.
func NewHandler(repo *Repository, log *slog.Logger) *Handler {
	return &Handler{repo: repo, log: log}
}

// Handle dispatches an event to the appropriate reaction.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	switch env.EventType {
	case events.OrderCreatedV1:
		return h.onOrderCreated(ctx, tx, env)
	case events.PaymentFailedV1:
		var p events.PaymentFailedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.release(ctx, tx, env, p.OrderID, "payment_failed")
	case events.FulfillmentFailedV1:
		var p events.FulfillmentFailedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.release(ctx, tx, env, p.OrderID, "fulfillment_failed")
	default:
		// Other event families share these topics; they are not inventory's
		// responsibility and must be acknowledged without action.
		return consumer.Ignore()
	}
}

func (h *Handler) onOrderCreated(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	var p events.OrderCreatedPayload
	if err := env.UnmarshalPayload(&p); err != nil {
		return consumer.Permanent(err)
	}

	if events.FaultEnabled(p.Faults, events.FaultInventoryReject) {
		h.log.WarnContext(ctx, "inventory rejection injected", slog.String("order_id", p.OrderID))
		return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateInventory, p.OrderID,
			events.InventoryRejectedV1, events.InventoryRejectedPayload{
				OrderID: p.OrderID, Reason: "inventory unavailable (fault injection)",
			}, env.CorrelationID, env.EventID)
	}

	reservationID, err := h.repo.Reserve(ctx, tx, p.OrderID, p.Items)
	if err != nil {
		if errors.Is(err, ErrInsufficientStock) {
			// Insufficient stock is a normal business outcome, not a failure.
			return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateInventory, p.OrderID,
				events.InventoryRejectedV1, events.InventoryRejectedPayload{
					OrderID: p.OrderID, Reason: err.Error(),
				}, env.CorrelationID, env.EventID)
		}
		return consumer.Transient(err)
	}

	return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateInventory, p.OrderID,
		events.InventoryReservedV1, events.InventoryReservedPayload{
			OrderID: p.OrderID, ReservationID: reservationID, Items: p.Items, Currency: p.Currency, Faults: p.Faults,
		}, env.CorrelationID, env.EventID)
}

func (h *Handler) release(ctx context.Context, tx pgx.Tx, env events.Envelope, orderID, reason string) error {
	reservationID, released, err := h.repo.Release(ctx, tx, orderID, reason)
	if err != nil {
		if errors.Is(err, ErrReservationNotFound) {
			return consumer.Permanent(err)
		}
		return consumer.Transient(err)
	}
	if !released {
		return nil // already released; idempotent no-op
	}
	return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateInventory, orderID,
		events.InventoryReleasedV1, events.InventoryReleasedPayload{
			OrderID: orderID, ReservationID: reservationID, Reason: reason,
		}, env.CorrelationID, env.EventID)
}
