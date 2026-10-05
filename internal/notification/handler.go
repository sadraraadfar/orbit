package notification

import (
	"context"
	"errors"
	"log/slog"

	"github.com/jackc/pgx/v5"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
)

// Handler implements the notification choreography reaction.
type Handler struct {
	repo *Repository
	log  *slog.Logger
}

// NewHandler constructs a notification Handler.
func NewHandler(repo *Repository, log *slog.Logger) *Handler {
	return &Handler{repo: repo, log: log}
}

// Handle sends the notification requested by notification.scheduled.v1.
func (h *Handler) Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	if env.EventType != events.NotificationScheduledV1 {
		return consumer.Ignore()
	}
	var p events.NotificationScheduledPayload
	if err := env.UnmarshalPayload(&p); err != nil {
		return consumer.Permanent(err)
	}

	if events.FaultEnabled(p.Faults, events.FaultNotificationFail) {
		// A permanent failure: the provider rejected the request and retrying
		// will not help, so the message is dead-lettered immediately.
		return consumer.Permanent(errors.New("notification provider rejected request (fault injection)"))
	}

	id, sent, err := h.repo.Send(ctx, tx, p.OrderID, p.Channel, p.Topic)
	if err != nil {
		return consumer.Transient(err)
	}
	if !sent {
		return nil // already sent; idempotent no-op
	}
	return outbox.Emit(ctx, tx, h.repo.Store(), events.AggregateNotification, p.OrderID,
		events.NotificationSentV1, events.NotificationSentPayload{
			OrderID: p.OrderID, NotificationID: id, Channel: p.Channel,
		}, env.CorrelationID, env.EventID)
}
