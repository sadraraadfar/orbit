package order

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
)

// EventHandler advances the order aggregate in response to events produced by
// downstream services. It implements consumer.Handler.
type EventHandler struct {
	svc *Service
	log *slog.Logger
}

// NewEventHandler constructs an EventHandler.
func NewEventHandler(svc *Service) *EventHandler {
	return &EventHandler{svc: svc, log: svc.Log()}
}

// Handle applies a workflow event to its order.
func (h *EventHandler) Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	switch env.EventType {
	case events.InventoryReservedV1:
		var p events.InventoryReservedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyInventoryReserved(p) })

	case events.InventoryRejectedV1:
		var p events.InventoryRejectedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyInventoryRejected(p) })

	case events.InventoryReleasedV1:
		var p events.InventoryReleasedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyInventoryReleased(p) })

	case events.PaymentAuthorizedV1:
		var p events.PaymentAuthorizedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyPaymentAuthorized(p) })

	case events.PaymentFailedV1:
		var p events.PaymentFailedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyPaymentFailed(p) })

	case events.PaymentRefundedV1:
		var p events.PaymentRefundedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyPaymentRefunded(p) })

	case events.FulfillmentStartedV1:
		var p events.FulfillmentStartedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyFulfillmentStarted(p) })

	case events.FulfillmentCompletedV1:
		var p events.FulfillmentCompletedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyFulfillmentCompleted(p) })

	case events.FulfillmentFailedV1:
		var p events.FulfillmentFailedPayload
		if err := env.UnmarshalPayload(&p); err != nil {
			return consumer.Permanent(err)
		}
		return h.apply(ctx, tx, env, func(o *Order) (bool, []PendingEvent) { return o.ApplyFulfillmentFailed(p) })

	default:
		return consumer.Ignore()
	}
}

func (h *EventHandler) apply(
	ctx context.Context,
	tx pgx.Tx,
	env events.Envelope,
	transition func(*Order) (bool, []PendingEvent),
) error {
	o, err := h.svc.repo.Lock(ctx, tx, env.AggregateID)
	if errors.Is(err, ErrOrderNotFound) {
		return consumer.Permanent(fmt.Errorf("order %s referenced by %s not found", env.AggregateID, env.EventType))
	}
	if err != nil {
		return consumer.Transient(err)
	}

	changed, next := transition(o)
	if !changed {
		return nil // duplicate or out-of-order event: safe no-op
	}
	if err := h.svc.repo.Update(ctx, tx, o); err != nil {
		return consumer.Transient(err)
	}
	if err := h.svc.repo.AddTimeline(ctx, tx, o.ID, TimelineEntry{
		EventID:       env.EventID,
		EventType:     env.EventType,
		OccurredAt:    env.OccurredAt,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Summary:       summaryFor(env.EventType, o),
	}); err != nil {
		return consumer.Transient(err)
	}

	for _, pe := range next {
		outEnv, err := events.New(pe.EventType, events.AggregateOrder, o.ID, pe.Payload, env.CorrelationID, env.EventID)
		if err != nil {
			return consumer.Permanent(err)
		}
		if err := h.svc.repo.AppendOutbox(ctx, tx, outEnv); err != nil {
			return consumer.Transient(err)
		}
	}
	return nil
}

func summaryFor(eventType string, o *Order) map[string]any {
	summary := map[string]any{
		"event_type": eventType,
		"status":     string(o.Status),
	}
	if o.CancelStage != "" {
		summary["cancel_stage"] = o.CancelStage
	}
	if o.CancelReason != "" {
		summary["cancel_reason"] = o.CancelReason
	}
	return summary
}
