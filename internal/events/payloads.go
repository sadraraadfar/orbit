package events

import "fmt"

// Payload is implemented by every event payload type. The marker method keeps
// accidental use of arbitrary structs out of the registry.
type Payload interface {
	isPayload()
}

// OrderItem is a line item shared by order and inventory payloads.
type OrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

// Fault injection hints attached to an order for deterministic failure tests.
// They are only honoured when ORBIT_ENABLE_FAULT_INJECTION is enabled.
const (
	FaultInventoryReject  = "inventory_reject"
	FaultPaymentReject    = "payment_reject"
	FaultPaymentTimeout   = "payment_timeout"
	FaultFulfillmentFail  = "fulfillment_fail"
	FaultNotificationFail = "notification_fail"
)

// OrderCreatedPayload is emitted when an order is accepted.
type OrderCreatedPayload struct {
	OrderID    string            `json:"order_id"`
	CustomerID string            `json:"customer_id"`
	Items      []OrderItem       `json:"items"`
	TotalCents int64             `json:"total_cents"`
	Currency   string            `json:"currency"`
	Faults     map[string]string `json:"faults,omitempty"`
}

func (OrderCreatedPayload) isPayload() {}

// InventoryReservedPayload confirms stock was reserved. Faults are carried
// forward from order.created.v1 so downstream services can honour test hints.
type InventoryReservedPayload struct {
	OrderID       string            `json:"order_id"`
	ReservationID string            `json:"reservation_id"`
	Items         []OrderItem       `json:"items"`
	Currency      string            `json:"currency"`
	Faults        map[string]string `json:"faults,omitempty"`
}

func (InventoryReservedPayload) isPayload() {}

// InventoryRejectedPayload reports that stock could not be reserved.
type InventoryRejectedPayload struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
}

func (InventoryRejectedPayload) isPayload() {}

// InventoryReleasedPayload confirms a compensating reservation release.
type InventoryReleasedPayload struct {
	OrderID       string `json:"order_id"`
	ReservationID string `json:"reservation_id"`
	Reason        string `json:"reason"`
}

func (InventoryReleasedPayload) isPayload() {}

// PaymentAuthorizedPayload confirms funds were authorized. Faults are carried
// forward for downstream services.
type PaymentAuthorizedPayload struct {
	OrderID     string            `json:"order_id"`
	PaymentID   string            `json:"payment_id"`
	AmountCents int64             `json:"amount_cents"`
	Currency    string            `json:"currency"`
	Faults      map[string]string `json:"faults,omitempty"`
}

func (PaymentAuthorizedPayload) isPayload() {}

// PaymentFailedPayload reports a declined or timed-out authorization.
type PaymentFailedPayload struct {
	OrderID   string `json:"order_id"`
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason"`
	Class     string `json:"class"`
}

func (PaymentFailedPayload) isPayload() {}

// PaymentRefundedPayload confirms a compensating refund.
type PaymentRefundedPayload struct {
	OrderID   string `json:"order_id"`
	PaymentID string `json:"payment_id"`
	Reason    string `json:"reason"`
}

func (PaymentRefundedPayload) isPayload() {}

// FulfillmentStartedPayload reports that fulfillment began.
type FulfillmentStartedPayload struct {
	OrderID       string `json:"order_id"`
	FulfillmentID string `json:"fulfillment_id"`
}

func (FulfillmentStartedPayload) isPayload() {}

// FulfillmentCompletedPayload reports successful fulfillment.
type FulfillmentCompletedPayload struct {
	OrderID       string `json:"order_id"`
	FulfillmentID string `json:"fulfillment_id"`
}

func (FulfillmentCompletedPayload) isPayload() {}

// FulfillmentFailedPayload reports a fulfillment failure.
type FulfillmentFailedPayload struct {
	OrderID       string `json:"order_id"`
	FulfillmentID string `json:"fulfillment_id"`
	Reason        string `json:"reason"`
}

func (FulfillmentFailedPayload) isPayload() {}

// OrderCompletedPayload marks the terminal success state.
type OrderCompletedPayload struct {
	OrderID    string `json:"order_id"`
	TotalCents int64  `json:"total_cents"`
	Currency   string `json:"currency"`
}

func (OrderCompletedPayload) isPayload() {}

// OrderCancelledPayload marks a terminal cancelled state after compensation.
type OrderCancelledPayload struct {
	OrderID string `json:"order_id"`
	Reason  string `json:"reason"`
	Stage   string `json:"stage"`
}

func (OrderCancelledPayload) isPayload() {}

// NotificationScheduledPayload requests a customer notification.
type NotificationScheduledPayload struct {
	OrderID string            `json:"order_id"`
	Channel string            `json:"channel"`
	Topic   string            `json:"topic"`
	Faults  map[string]string `json:"faults,omitempty"`
}

func (NotificationScheduledPayload) isPayload() {}

// NotificationSentPayload confirms a notification was delivered.
type NotificationSentPayload struct {
	OrderID        string `json:"order_id"`
	NotificationID string `json:"notification_id"`
	Channel        string `json:"channel"`
}

func (NotificationSentPayload) isPayload() {}

// FaultEnabled reports whether a fault hint is active. A hint is active unless
// its value is one of "false", "", or "0".
func FaultEnabled(faults map[string]string, name string) bool {
	v, ok := faults[name]
	if !ok {
		return false
	}
	switch v {
	case "", "false", "0":
		return false
	default:
		return true
	}
}

func requireFields(fields map[string]string) error {
	for name, value := range fields {
		if value == "" {
			return fmt.Errorf("events: payload field %q is required", name)
		}
	}
	return nil
}

func requirePositive(fields map[string]int64) error {
	for name, value := range fields {
		if value <= 0 {
			return fmt.Errorf("events: payload field %q must be positive", name)
		}
	}
	return nil
}
