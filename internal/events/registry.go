package events

import (
	"encoding/json"
	"fmt"
	"sort"
)

// Spec describes a single event type registered in the contract registry.
type Spec struct {
	Type            string
	Version         int
	Owner           string
	NewPayload      func() Payload
	ValidatePayload func(p Payload) error
	// Deprecated marks a contract that is still accepted for decoding but must
	// not be produced by new code.
	Deprecated bool
}

// ValidateRaw decodes a raw payload and runs its validation rules.
func (s Spec) ValidateRaw(raw []byte) error {
	p := s.NewPayload()
	if err := json.Unmarshal(raw, p); err != nil {
		return fmt.Errorf("events: %s payload does not match contract: %w", s.Type, err)
	}
	if s.ValidatePayload != nil {
		return s.ValidatePayload(p)
	}
	return nil
}

// registry is the single source of truth for event contracts. Adding an event
// type requires adding it here; unknown types are rejected by Envelope.Validate.
var registry = map[string]Spec{
	OrderCreatedV1: {
		Type: OrderCreatedV1, Version: 1, Owner: "order", NewPayload: func() Payload { return &OrderCreatedPayload{} },
		ValidatePayload: func(p Payload) error {
			v, ok := p.(*OrderCreatedPayload)
			if !ok {
				return fmt.Errorf("events: %s payload type mismatch", OrderCreatedV1)
			}
			if err := requireFields(map[string]string{"order_id": v.OrderID, "customer_id": v.CustomerID, "currency": v.Currency}); err != nil {
				return err
			}
			if len(v.Items) == 0 {
				return fmt.Errorf("events: order.created.v1 requires at least one item")
			}
			if v.TotalCents <= 0 {
				return fmt.Errorf("events: order.created.v1 total_cents must be positive")
			}
			return nil
		},
	},
	OrderCompletedV1: {
		Type: OrderCompletedV1, Version: 1, Owner: "order", NewPayload: func() Payload { return &OrderCompletedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*OrderCompletedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "currency": v.Currency})
		},
	},
	OrderCancelledV1: {
		Type: OrderCancelledV1, Version: 1, Owner: "order", NewPayload: func() Payload { return &OrderCancelledPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*OrderCancelledPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "reason": v.Reason, "stage": v.Stage})
		},
	},
	InventoryReservedV1: {
		Type: InventoryReservedV1, Version: 1, Owner: "inventory", NewPayload: func() Payload { return &InventoryReservedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*InventoryReservedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "reservation_id": v.ReservationID})
		},
	},
	InventoryRejectedV1: {
		Type: InventoryRejectedV1, Version: 1, Owner: "inventory", NewPayload: func() Payload { return &InventoryRejectedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*InventoryRejectedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "reason": v.Reason})
		},
	},
	InventoryReleasedV1: {
		Type: InventoryReleasedV1, Version: 1, Owner: "inventory", NewPayload: func() Payload { return &InventoryReleasedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*InventoryReleasedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "reservation_id": v.ReservationID, "reason": v.Reason})
		},
	},
	PaymentAuthorizedV1: {
		Type: PaymentAuthorizedV1, Version: 1, Owner: "payment", NewPayload: func() Payload { return &PaymentAuthorizedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*PaymentAuthorizedPayload)
			if err := requireFields(map[string]string{"order_id": v.OrderID, "payment_id": v.PaymentID, "currency": v.Currency}); err != nil {
				return err
			}
			return requirePositive(map[string]int64{"amount_cents": v.AmountCents})
		},
	},
	PaymentFailedV1: {
		Type: PaymentFailedV1, Version: 1, Owner: "payment", NewPayload: func() Payload { return &PaymentFailedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*PaymentFailedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "payment_id": v.PaymentID, "reason": v.Reason, "class": v.Class})
		},
	},
	PaymentRefundedV1: {
		Type: PaymentRefundedV1, Version: 1, Owner: "payment", NewPayload: func() Payload { return &PaymentRefundedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*PaymentRefundedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "payment_id": v.PaymentID, "reason": v.Reason})
		},
	},
	FulfillmentStartedV1: {
		Type: FulfillmentStartedV1, Version: 1, Owner: "fulfillment", NewPayload: func() Payload { return &FulfillmentStartedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*FulfillmentStartedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "fulfillment_id": v.FulfillmentID})
		},
	},
	FulfillmentCompletedV1: {
		Type: FulfillmentCompletedV1, Version: 1, Owner: "fulfillment", NewPayload: func() Payload { return &FulfillmentCompletedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*FulfillmentCompletedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "fulfillment_id": v.FulfillmentID})
		},
	},
	FulfillmentFailedV1: {
		Type: FulfillmentFailedV1, Version: 1, Owner: "fulfillment", NewPayload: func() Payload { return &FulfillmentFailedPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*FulfillmentFailedPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "fulfillment_id": v.FulfillmentID, "reason": v.Reason})
		},
	},
	NotificationScheduledV1: {
		Type: NotificationScheduledV1, Version: 1, Owner: "notification", NewPayload: func() Payload { return &NotificationScheduledPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*NotificationScheduledPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "channel": v.Channel, "topic": v.Topic})
		},
	},
	NotificationSentV1: {
		Type: NotificationSentV1, Version: 1, Owner: "notification", NewPayload: func() Payload { return &NotificationSentPayload{} },
		ValidatePayload: func(p Payload) error {
			v := p.(*NotificationSentPayload)
			return requireFields(map[string]string{"order_id": v.OrderID, "notification_id": v.NotificationID, "channel": v.Channel})
		},
	},
}

// Lookup returns the spec for an event type.
func Lookup(eventType string) (Spec, bool) {
	s, ok := registry[eventType]
	return s, ok
}

// All returns every registered spec ordered by type. It is used by tooling and
// contract tests.
func All() []Spec {
	specs := make([]Spec, 0, len(registry))
	for _, s := range registry {
		specs = append(specs, s)
	}
	sort.Slice(specs, func(i, j int) bool { return specs[i].Type < specs[j].Type })
	return specs
}

// OwnedBy returns the specs whose declared owner matches the supplied service.
func OwnedBy(owner string) []Spec {
	var out []Spec
	for _, s := range All() {
		if s.Owner == owner {
			out = append(out, s)
		}
	}
	return out
}
