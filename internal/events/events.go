// Package events defines the versioned event envelope exchanged between Orbit
// services.
//
// Every message placed on the broker is an Envelope. The envelope carries the
// metadata required for tracing, idempotency, and correlation; the payload is
// an event-type-specific JSON document defined in payloads.go and registered in
// registry.go.
package events

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Event type names. The trailing .vN is part of the contract and is mirrored by
// Envelope.EventVersion.
const (
	OrderCreatedV1   = "order.created.v1"
	OrderCompletedV1 = "order.completed.v1"
	OrderCancelledV1 = "order.cancelled.v1"

	InventoryReservedV1 = "inventory.reserved.v1"
	InventoryRejectedV1 = "inventory.rejected.v1"
	InventoryReleasedV1 = "inventory.released.v1"

	PaymentAuthorizedV1 = "payment.authorized.v1"
	PaymentFailedV1     = "payment.failed.v1"
	PaymentRefundedV1   = "payment.refunded.v1"

	FulfillmentStartedV1   = "fulfillment.started.v1"
	FulfillmentCompletedV1 = "fulfillment.completed.v1"
	FulfillmentFailedV1    = "fulfillment.failed.v1"

	NotificationScheduledV1 = "notification.scheduled.v1"
	NotificationSentV1      = "notification.sent.v1"
)

// Envelope is the wire representation of every Orbit event.
type Envelope struct {
	EventID       string          `json:"event_id"`
	EventType     string          `json:"event_type"`
	EventVersion  int             `json:"event_version"`
	OccurredAt    time.Time       `json:"occurred_at"`
	CorrelationID string          `json:"correlation_id"`
	CausationID   string          `json:"causation_id,omitempty"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	Producer      string          `json:"producer"`
	TraceParent   string          `json:"traceparent,omitempty"`
	Payload       json.RawMessage `json:"payload"`
}

// New builds an envelope for the supplied event type. The payload is marshalled
// immediately so callers cannot accidentally mutate it after construction.
func New(eventType, aggregateType, aggregateID string, payload Payload, correlationID, causationID string) (Envelope, error) {
	entry, ok := Lookup(eventType)
	if !ok {
		return Envelope{}, fmt.Errorf("events: unknown event type %q", eventType)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("events: marshal payload for %s: %w", eventType, err)
	}
	return Envelope{
		EventID:       uuid.NewString(),
		EventType:     eventType,
		EventVersion:  entry.Version,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: correlationID,
		CausationID:   causationID,
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		Producer:      entry.Owner,
		Payload:       raw,
	}, nil
}

// Encode marshals the envelope for the broker.
func (e Envelope) Encode() ([]byte, error) {
	return json.Marshal(e)
}

// Decode parses and validates an envelope received from the broker.
func Decode(data []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return Envelope{}, fmt.Errorf("events: decode envelope: %w", err)
	}
	if err := env.Validate(); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Validate checks structural invariants and delegates payload validation to the
// registry. It is called by both Decode and the outbox writer.
func (e Envelope) Validate() error {
	if _, err := uuid.Parse(e.EventID); err != nil {
		return fmt.Errorf("events: invalid event_id %q: %w", e.EventID, err)
	}
	if e.EventType == "" {
		return fmt.Errorf("events: event_type is required")
	}
	entry, ok := Lookup(e.EventType)
	if !ok {
		return fmt.Errorf("events: unknown event_type %q", e.EventType)
	}
	if e.EventVersion != entry.Version {
		return fmt.Errorf("events: %s expects version %d, got %d", e.EventType, entry.Version, e.EventVersion)
	}
	if e.OccurredAt.IsZero() {
		return fmt.Errorf("events: occurred_at is required")
	}
	if e.CorrelationID == "" {
		return fmt.Errorf("events: correlation_id is required")
	}
	if e.AggregateType == "" || e.AggregateID == "" {
		return fmt.Errorf("events: aggregate_type and aggregate_id are required")
	}
	if len(e.Payload) == 0 {
		return fmt.Errorf("events: payload is required")
	}
	return entry.ValidateRaw(e.Payload)
}

// UnmarshalPayload decodes the event payload into dst after checking that dst's
// type matches the registered contract for the event type.
func (e Envelope) UnmarshalPayload(dst Payload) error {
	entry, ok := Lookup(e.EventType)
	if !ok {
		return fmt.Errorf("events: unknown event_type %q", e.EventType)
	}
	if err := json.Unmarshal(e.Payload, dst); err != nil {
		return fmt.Errorf("events: decode payload for %s: %w", e.EventType, err)
	}
	if entry.ValidatePayload != nil {
		return entry.ValidatePayload(dst)
	}
	return nil
}

// TopicFor maps an event type onto the topic that carries its family. Events
// are keyed by aggregate ID on the broker so ordering is preserved per
// aggregate.
func TopicFor(prefix, eventType string) string {
	family := eventType
	if idx := strings.IndexByte(eventType, '.'); idx >= 0 {
		family = eventType[:idx]
	}
	return prefix + "." + family + ".events"
}

// StreamTopic returns the topic that carries a family of events, for example
// StreamTopic("orbit", "inventory") == "orbit.inventory.events".
func StreamTopic(prefix, family string) string { return prefix + "." + family + ".events" }

// AllStreamTopics returns the topics for every event family.
func AllStreamTopics(prefix string) []string {
	return []string{
		StreamTopic(prefix, "order"),
		StreamTopic(prefix, "inventory"),
		StreamTopic(prefix, "payment"),
		StreamTopic(prefix, "fulfillment"),
		StreamTopic(prefix, "notification"),
	}
}

// AllTopics returns every stream topic plus its dead-letter counterpart.
func AllTopics(prefix string) []string {
	streams := AllStreamTopics(prefix)
	topics := make([]string, 0, len(streams)*2)
	for _, t := range streams {
		topics = append(topics, t, DLQTopic(t))
	}
	return topics
}

// DLQTopic returns the dead-letter topic paired with a source topic.
func DLQTopic(topic string) string { return topic + ".dlq" }

// Aggregate types.
const (
	AggregateOrder        = "order"
	AggregateInventory    = "inventory"
	AggregatePayment      = "payment"
	AggregateFulfillment  = "fulfillment"
	AggregateNotification = "notification"
)
