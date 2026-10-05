// Package order implements the order aggregate, its persistence, the HTTP API,
// and the choreography reactions that advance an order through the workflow.
package order

import (
	"fmt"
	"time"

	"github.com/example/orbit/internal/events"
)

// Status is the lifecycle state of an order.
type Status string

const (
	StatusPending            Status = "pending"
	StatusInventoryReserved  Status = "inventory_reserved"
	StatusPaymentAuthorized  Status = "payment_authorized"
	StatusFulfillmentStarted Status = "fulfillment_started"
	StatusCompleted          Status = "completed"
	StatusCancelling         Status = "cancelling"
	StatusCancelled          Status = "cancelled"
)

// Terminal reports whether no further transitions are expected.
func (s Status) Terminal() bool { return s == StatusCompleted || s == StatusCancelled }

// Valid reports whether s is a known status.
func (s Status) Valid() bool {
	switch s {
	case StatusPending, StatusInventoryReserved, StatusPaymentAuthorized,
		StatusFulfillmentStarted, StatusCompleted, StatusCancelling, StatusCancelled:
		return true
	default:
		return false
	}
}

// Milestone values.
const (
	inventoryReserved = "reserved"
	inventoryRejected = "rejected"

	paymentAuthorized = "authorized"
	paymentFailed     = "failed"
	paymentRefunded   = "refunded"

	fulfillmentStarted   = "started"
	fulfillmentCompleted = "completed"
	fulfillmentFailed    = "failed"

	cancelStageInventory   = "inventory"
	cancelStagePayment     = "payment"
	cancelStageFulfillment = "fulfillment"
)

// Order is the order aggregate. It records observed workflow facts rather than
// a single linear state, so that events arriving out of causal order (the
// inventory, payment, and fulfillment streams are consumed independently) still
// produce a correct result. Status is derived from those facts.
type Order struct {
	ID                string             `json:"id"`
	CustomerID        string             `json:"customer_id"`
	Items             []events.OrderItem `json:"items"`
	TotalCents        int64              `json:"total_cents"`
	Currency          string             `json:"currency"`
	Status            Status             `json:"status"`
	InventoryState    string             `json:"inventory_state,omitempty"`
	PaymentState      string             `json:"payment_state,omitempty"`
	FulfillmentState  string             `json:"fulfillment_state,omitempty"`
	InventoryReleased bool               `json:"inventory_released"`
	PaymentRefunded   bool               `json:"payment_refunded"`
	CancelStage       string             `json:"cancel_stage,omitempty"`
	CancelReason      string             `json:"cancel_reason,omitempty"`
	Faults            map[string]string  `json:"faults,omitempty"`
	CorrelationID     string             `json:"correlation_id,omitempty"`
	CreatedAt         time.Time          `json:"created_at"`
	UpdatedAt         time.Time          `json:"updated_at"`
}

// PendingEvent is an event the domain has decided to emit. The handler turns it
// into an envelope with the correct correlation and causation IDs and appends
// it to the outbox.
type PendingEvent struct {
	EventType string
	Payload   events.Payload
}

// ApplyInventoryReserved records a successful reservation.
func (o *Order) ApplyInventoryReserved(_ events.InventoryReservedPayload) (bool, []PendingEvent) {
	if o.InventoryState != "" || o.CancelStage != "" {
		return false, nil
	}
	prev := o.Status
	o.InventoryState = inventoryReserved
	return o.applyChange(true, prev)
}

// ApplyInventoryRejected cancels an order whose stock could not be reserved.
func (o *Order) ApplyInventoryRejected(p events.InventoryRejectedPayload) (bool, []PendingEvent) {
	if o.InventoryState != "" || o.CancelStage != "" || o.FulfillmentState == fulfillmentCompleted {
		return false, nil
	}
	prev := o.Status
	o.InventoryState = inventoryRejected
	o.CancelStage = cancelStageInventory
	o.CancelReason = p.Reason
	return o.applyChange(true, prev)
}

// ApplyPaymentAuthorized records a successful authorization.
func (o *Order) ApplyPaymentAuthorized(_ events.PaymentAuthorizedPayload) (bool, []PendingEvent) {
	if o.PaymentState != "" || o.CancelStage != "" || o.FulfillmentState == fulfillmentCompleted {
		return false, nil
	}
	prev := o.Status
	o.PaymentState = paymentAuthorized
	return o.applyChange(true, prev)
}

// ApplyPaymentFailed begins cancellation. Inventory release is the required
// compensation because the payment was never authorized.
func (o *Order) ApplyPaymentFailed(p events.PaymentFailedPayload) (bool, []PendingEvent) {
	if o.PaymentState == paymentFailed || o.PaymentState == paymentRefunded {
		return false, nil
	}
	if o.CancelStage == cancelStageFulfillment || o.FulfillmentState == fulfillmentCompleted {
		return false, nil
	}
	prev := o.Status
	o.PaymentState = paymentFailed
	if o.CancelStage == "" {
		o.CancelStage = cancelStagePayment
		o.CancelReason = p.Reason
	}
	return o.applyChange(true, prev)
}

// ApplyFulfillmentStarted records that fulfillment began.
func (o *Order) ApplyFulfillmentStarted(_ events.FulfillmentStartedPayload) (bool, []PendingEvent) {
	if o.FulfillmentState != "" || o.CancelStage != "" {
		return false, nil
	}
	prev := o.Status
	o.FulfillmentState = fulfillmentStarted
	return o.applyChange(true, prev)
}

// ApplyFulfillmentCompleted completes the workflow and schedules a
// notification.
func (o *Order) ApplyFulfillmentCompleted(_ events.FulfillmentCompletedPayload) (bool, []PendingEvent) {
	if o.FulfillmentState == fulfillmentCompleted || o.FulfillmentState == fulfillmentFailed || o.CancelStage != "" {
		return false, nil
	}
	prev := o.Status
	o.FulfillmentState = fulfillmentCompleted
	return o.applyChange(true, prev)
}

// ApplyFulfillmentFailed begins cancellation. Because payment was authorized,
// cancellation requires both a refund and an inventory release.
func (o *Order) ApplyFulfillmentFailed(p events.FulfillmentFailedPayload) (bool, []PendingEvent) {
	if o.FulfillmentState == fulfillmentFailed || o.FulfillmentState == fulfillmentCompleted {
		return false, nil
	}
	if o.CancelStage == cancelStageFulfillment {
		return false, nil
	}
	prev := o.Status
	o.FulfillmentState = fulfillmentFailed
	o.CancelStage = cancelStageFulfillment
	o.CancelReason = p.Reason
	return o.applyChange(true, prev)
}

// ApplyInventoryReleased records the compensating inventory release.
func (o *Order) ApplyInventoryReleased(_ events.InventoryReleasedPayload) (bool, []PendingEvent) {
	if o.InventoryReleased {
		return false, nil
	}
	prev := o.Status
	o.InventoryReleased = true
	return o.applyChange(true, prev)
}

// ApplyPaymentRefunded records the compensating refund.
func (o *Order) ApplyPaymentRefunded(_ events.PaymentRefundedPayload) (bool, []PendingEvent) {
	if o.PaymentRefunded {
		return false, nil
	}
	prev := o.Status
	o.PaymentRefunded = true
	return o.applyChange(true, prev)
}

// applyChange recomputes the derived status and returns the events implied by a
// change in status.
func (o *Order) applyChange(changed bool, prev Status) (bool, []PendingEvent) {
	if !changed {
		return false, nil
	}
	o.Status = o.deriveStatus()
	switch o.Status {
	case StatusCompleted:
		if prev != StatusCompleted {
			return true, []PendingEvent{
				{
					EventType: events.OrderCompletedV1,
					Payload: events.OrderCompletedPayload{
						OrderID: o.ID, TotalCents: o.TotalCents, Currency: o.Currency,
					},
				},
				{
					EventType: events.NotificationScheduledV1,
					Payload: events.NotificationScheduledPayload{
						OrderID: o.ID, Channel: "email", Topic: "order_completed", Faults: o.Faults,
					},
				},
			}
		}
	case StatusCancelled:
		if prev != StatusCancelled {
			reason := o.CancelReason
			if reason == "" {
				reason = "workflow failed"
			}
			return true, []PendingEvent{{
				EventType: events.OrderCancelledV1,
				Payload: events.OrderCancelledPayload{
					OrderID: o.ID, Reason: reason, Stage: o.CancelStage,
				},
			}}
		}
	}
	return true, nil
}

// deriveStatus computes the externally visible status from the observed facts.
func (o *Order) deriveStatus() Status {
	if o.CancelStage != "" {
		if o.cancelComplete() {
			return StatusCancelled
		}
		return StatusCancelling
	}
	switch {
	case o.FulfillmentState == fulfillmentCompleted:
		return StatusCompleted
	case o.FulfillmentState == fulfillmentStarted:
		return StatusFulfillmentStarted
	case o.PaymentState == paymentAuthorized:
		return StatusPaymentAuthorized
	case o.InventoryState == inventoryReserved:
		return StatusInventoryReserved
	default:
		return StatusPending
	}
}

// cancelComplete reports whether every compensation required by the current
// cancellation stage has been observed.
func (o *Order) cancelComplete() bool {
	switch o.CancelStage {
	case cancelStageInventory:
		return true
	case cancelStagePayment:
		return o.InventoryReleased
	case cancelStageFulfillment:
		return o.InventoryReleased && o.PaymentRefunded
	default:
		return true
	}
}

// Validate checks the aggregate invariants that must hold before persistence.
func (o *Order) Validate() error {
	if o.ID == "" {
		return fmt.Errorf("order: id is required")
	}
	if o.CustomerID == "" {
		return fmt.Errorf("order: customer_id is required")
	}
	if len(o.Items) == 0 {
		return fmt.Errorf("order: at least one item is required")
	}
	if o.TotalCents <= 0 {
		return fmt.Errorf("order: total must be positive")
	}
	if o.Currency == "" {
		return fmt.Errorf("order: currency is required")
	}
	if !o.Status.Valid() {
		return fmt.Errorf("order: unknown status %q", o.Status)
	}
	return nil
}

// TimelineEntry is one entry in an order's distributed timeline.
type TimelineEntry struct {
	EventID       string         `json:"event_id"`
	EventType     string         `json:"event_type"`
	OccurredAt    time.Time      `json:"occurred_at"`
	RecordedAt    time.Time      `json:"recorded_at"`
	CorrelationID string         `json:"correlation_id"`
	CausationID   string         `json:"causation_id,omitempty"`
	Summary       map[string]any `json:"summary"`
}
