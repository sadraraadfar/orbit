package fulfillment

import (
	"context"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
)

// Handler implements the fulfillment choreography reactions.
type Handler struct {
	repo *Repository
	log  *slog.Logger
}

// NewHandler constructs a fulfillment Handler.
func NewHandler(repo *Repository, log *slog.Logger) *Handler {
	return &Handler{repo: repo, log: log}
}

// Handle reacts to a payment authorization by starting fulfillment and
// immediately completing or failing it.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	if env.EventType != events.PaymentAuthorizedV1 {
		return consumer.Ignore()
	}
	var p events.PaymentAuthorizedPayload
	if err := env.UnmarshalPayload(&p); err != nil {
		return consumer.Permanent(err)
	}

	fulfillmentID, err := h.repo.Start(ctx, tx, p.OrderID)
	if err != nil {
		return consumer.Transient(err)
	}
	if err := outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateFulfillment, p.OrderID,
		events.FulfillmentStartedV1, events.FulfillmentStartedPayload{
			OrderID: p.OrderID, FulfillmentID: fulfillmentID,
		}, env.CorrelationID, env.EventID); err != nil {
		return consumer.Transient(err)
	}

	if events.FaultEnabled(p.Faults, events.FaultFulfillmentFail) {
		h.log.WarnContext(ctx, "fulfillment failure injected", slog.String("order_id", p.OrderID))
		if _, changed, err := h.repo.Fail(ctx, tx, p.OrderID, "warehouse error (fault injection)"); err != nil {
			return consumer.Transient(err)
		} else if !changed {
			return nil
		}
		return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateFulfillment, p.OrderID,
			events.FulfillmentFailedV1, events.FulfillmentFailedPayload{
				OrderID: p.OrderID, FulfillmentID: fulfillmentID, Reason: "warehouse error (fault injection)",
			}, env.CorrelationID, env.EventID)
	}

	if _, changed, err := h.repo.Complete(ctx, tx, p.OrderID); err != nil {
		return consumer.Transient(err)
	} else if !changed {
		return nil
	}
	return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateFulfillment, p.OrderID,
		events.FulfillmentCompletedV1, events.FulfillmentCompletedPayload{
			OrderID: p.OrderID, FulfillmentID: fulfillmentID,
		}, env.CorrelationID, env.EventID)
}
