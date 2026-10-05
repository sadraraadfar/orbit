package events_test

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/example/orbit/internal/events"
)

// goldenPayloads are frozen representations of the published v1 contracts. They
// are intentionally hand-written rather than generated from the Go structs so
// that a rename, removal, or type change in a payload fails the test and forces
// a deliberate versioning decision.
var goldenPayloads = map[string]string{
	events.OrderCreatedV1:          `{"order_id":"o-1","customer_id":"c-1","items":[{"sku":"SKU-WIDGET","quantity":2,"unit_price_cents":1500}],"total_cents":3000,"currency":"USD"}`,
	events.OrderCompletedV1:        `{"order_id":"o-1","total_cents":3000,"currency":"USD"}`,
	events.OrderCancelledV1:        `{"order_id":"o-1","reason":"payment_failed","stage":"payment"}`,
	events.InventoryReservedV1:     `{"order_id":"o-1","reservation_id":"r-1","items":[{"sku":"SKU-WIDGET","quantity":2,"unit_price_cents":1500}],"currency":"USD"}`,
	events.InventoryRejectedV1:     `{"order_id":"o-1","reason":"insufficient stock"}`,
	events.InventoryReleasedV1:     `{"order_id":"o-1","reservation_id":"r-1","reason":"payment_failed"}`,
	events.PaymentAuthorizedV1:     `{"order_id":"o-1","payment_id":"p-1","amount_cents":3000,"currency":"USD"}`,
	events.PaymentFailedV1:         `{"order_id":"o-1","payment_id":"p-1","reason":"card declined","class":"permanent"}`,
	events.PaymentRefundedV1:       `{"order_id":"o-1","payment_id":"p-1","reason":"fulfillment_failed"}`,
	events.FulfillmentStartedV1:    `{"order_id":"o-1","fulfillment_id":"f-1"}`,
	events.FulfillmentCompletedV1:  `{"order_id":"o-1","fulfillment_id":"f-1"}`,
	events.FulfillmentFailedV1:     `{"order_id":"o-1","fulfillment_id":"f-1","reason":"warehouse error"}`,
	events.NotificationScheduledV1: `{"order_id":"o-1","channel":"email","topic":"order_completed"}`,
	events.NotificationSentV1:      `{"order_id":"o-1","notification_id":"n-1","channel":"email"}`,
}

func envelopeFor(t *testing.T, eventType, payload string) events.Envelope {
	t.Helper()
	spec, ok := events.Lookup(eventType)
	if !ok {
		t.Fatalf("event type %s is not registered", eventType)
	}
	return events.Envelope{
		EventID:       uuid.NewString(),
		EventType:     spec.Type,
		EventVersion:  spec.Version,
		OccurredAt:    time.Now().UTC(),
		CorrelationID: "corr-1",
		AggregateType: "order",
		AggregateID:   "o-1",
		Producer:      spec.Owner,
		Payload:       json.RawMessage(payload),
	}
}

// TestGoldenContracts ensures every registered event type has a frozen payload
// that decodes and validates against the current contract.
func TestGoldenContracts(t *testing.T) {
	for _, spec := range events.All() {
		spec := spec
		t.Run(spec.Type, func(t *testing.T) {
			payload, ok := goldenPayloads[spec.Type]
			if !ok {
				t.Fatalf("no golden payload for %s; add one when registering a contract", spec.Type)
			}
			env := envelopeFor(t, spec.Type, payload)
			raw, err := env.Encode()
			if err != nil {
				t.Fatalf("encode: %v", err)
			}
			if _, err := events.Decode(raw); err != nil {
				t.Fatalf("golden payload no longer matches contract: %v", err)
			}
		})
	}
}

// TestVersionSuffixMatchesSpec requires the .vN suffix of every event type to
// equal the registered version.
func TestVersionSuffixMatchesSpec(t *testing.T) {
	for _, spec := range events.All() {
		idx := strings.LastIndex(spec.Type, ".v")
		if idx < 0 {
			t.Errorf("%s does not carry a .vN suffix", spec.Type)
			continue
		}
		want := spec.Type[idx+2:]
		got := string(rune('0' + spec.Version))
		if want != got {
			t.Errorf("%s: suffix v%s does not match version field %d", spec.Type, want, spec.Version)
		}
	}
}

// TestAdditiveChangesAreBackwardCompatible documents the versioning policy:
// adding an unknown field to a v1 payload must not break existing consumers.
func TestAdditiveChangesAreBackwardCompatible(t *testing.T) {
	payload := goldenPayloads[events.OrderCreatedV1]
	var asMap map[string]any
	if err := json.Unmarshal([]byte(payload), &asMap); err != nil {
		t.Fatal(err)
	}
	asMap["future_optional_field"] = "added-in-a-later-release"
	extended, _ := json.Marshal(asMap)

	env := envelopeFor(t, events.OrderCreatedV1, string(extended))
	raw, _ := env.Encode()
	if _, err := events.Decode(raw); err != nil {
		t.Fatalf("additive change broke decoding: %v", err)
	}
}

func TestEnvelopeValidationRejectsBadInput(t *testing.T) {
	valid := envelopeFor(t, events.OrderCreatedV1, goldenPayloads[events.OrderCreatedV1])

	cases := map[string]func(e events.Envelope) events.Envelope{
		"unknown type":    func(e events.Envelope) events.Envelope { e.EventType = "order.exploded.v1"; return e },
		"wrong version":   func(e events.Envelope) events.Envelope { e.EventVersion = 2; return e },
		"missing corr":    func(e events.Envelope) events.Envelope { e.CorrelationID = ""; return e },
		"bad event id":    func(e events.Envelope) events.Envelope { e.EventID = "not-a-uuid"; return e },
		"missing payload": func(e events.Envelope) events.Envelope { e.Payload = nil; return e },
		"bad payload":     func(e events.Envelope) events.Envelope { e.Payload = json.RawMessage(`{}`); return e },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			if err := mutate(valid).Validate(); err == nil {
				t.Fatalf("expected validation error for %s", name)
			}
		})
	}
}

func TestTopicRouting(t *testing.T) {
	cases := map[string]string{
		events.OrderCreatedV1:          "orbit.order.events",
		events.InventoryReservedV1:     "orbit.inventory.events",
		events.PaymentFailedV1:         "orbit.payment.events",
		events.FulfillmentCompletedV1:  "orbit.fulfillment.events",
		events.NotificationScheduledV1: "orbit.notification.events",
	}
	for eventType, want := range cases {
		if got := events.TopicFor("orbit", eventType); got != want {
			t.Errorf("TopicFor(%s) = %s, want %s", eventType, got, want)
		}
	}
	if got := events.StreamTopic("orbit", "payment"); got != "orbit.payment.events" {
		t.Errorf("StreamTopic = %s", got)
	}
	if got := events.DLQTopic("orbit.payment.events"); got != "orbit.payment.events.dlq" {
		t.Errorf("DLQTopic = %s", got)
	}
}
