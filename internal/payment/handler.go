package payment

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
)

// Handler implements the payment choreography reactions.
type Handler struct {
	repo *Repository
	log  *slog.Logger
}

// NewHandler constructs a payment Handler.
func NewHandler(repo *Repository, log *slog.Logger) *Handler {
	return &Handler{repo: repo, log: log}
}

// Handle dispatches an event to the appropriate reaction.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	switch env.EventType {
	case events.InventoryReservedV1:
		return h.onInventoryReserved(ctx, tx, env)
	case events.FulfillmentFailedV1:
		return h.onFulfillmentFailed(ctx, tx, env)
	default:
		return consumer.Ignore()
	}
}

func (h *Handler) onInventoryReserved(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	var p events.InventoryReservedPayload
	if err := env.UnmarshalPayload(&p); err != nil {
		return consumer.Permanent(err)
	}

	switch {
	case events.FaultEnabled(p.Faults, events.FaultPaymentReject):
		h.log.WarnContext(ctx, "payment rejection injected", slog.String("order_id", p.OrderID))
		id, err := h.repo.Fail(ctx, tx, p.OrderID, "card declined (fault injection)", "permanent")
		if err != nil {
			return consumer.Transient(err)
		}
		return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregatePayment, p.OrderID,
			events.PaymentFailedV1, events.PaymentFailedPayload{
				OrderID: p.OrderID, PaymentID: id, Reason: "card declined (fault injection)", Class: "permanent",
			}, env.CorrelationID, env.EventID)

	case events.FaultEnabled(p.Faults, events.FaultPaymentTimeout):
		// Simulate an upstream timeout. The consumer retries with backoff and
		// eventually routes the event to the dead-letter topic.
		return consumer.Transient(errors.New("payment gateway timeout (fault injection)"))
	}

	amount := int64(0)
	for _, it := range p.Items {
		amount += it.UnitPriceCents * int64(it.Quantity)
	}
	currency := p.Currency
	if currency == "" {
		currency = "USD"
	}
	id, err := h.repo.Authorize(ctx, tx, p.OrderID, amount, currency)
	if err != nil {
		return consumer.Transient(err)
	}
	return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregatePayment, p.OrderID,
		events.PaymentAuthorizedV1, events.PaymentAuthorizedPayload{
			OrderID: p.OrderID, PaymentID: id, AmountCents: amount, Currency: currency, Faults: p.Faults,
		}, env.CorrelationID, env.EventID)
}

func (h *Handler) onFulfillmentFailed(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	var p events.FulfillmentFailedPayload
	if err := env.UnmarshalPayload(&p); err != nil {
		return consumer.Permanent(err)
	}
	paymentID, refunded, err := h.repo.Refund(ctx, tx, p.OrderID, "fulfillment_failed")
	if err != nil {
		if errors.Is(err, ErrPaymentNotFound) || errors.Is(err, ErrPaymentNotRefundable) {
			return consumer.Permanent(err)
		}
		return consumer.Transient(err)
	}
	if !refunded {
		return nil // already refunded; idempotent no-op
	}
	return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregatePayment, p.OrderID,
		events.PaymentRefundedV1, events.PaymentRefundedPayload{
			OrderID: p.OrderID, PaymentID: paymentID, Reason: "fulfillment_failed",
		}, env.CorrelationID, env.EventID)
}
