package order

import (
	"testing"

	"github.com/example/orbit/internal/events"
)

func newTestOrder() *Order {
	return &Order{
		ID: "o-1", CustomerID: "c-1", TotalCents: 3000, Currency: "USD",
		Status: StatusPending,
		Items:  []events.OrderItem{{SKU: "SKU-WIDGET", Quantity: 2, UnitPriceCents: 1500}},
	}
}

func TestHappyPathInOrder(t *testing.T) {
	o := newTestOrder()

	step := func(name string, apply func() (bool, []PendingEvent)) []PendingEvent {
		changed, next := apply()
		if !changed {
			t.Fatalf("%s should change state", name)
		}
		return next
	}

	step("inventory.reserved", func() (bool, []PendingEvent) { return o.ApplyInventoryReserved(events.InventoryReservedPayload{}) })
	if o.Status != StatusInventoryReserved {
		t.Fatalf("status = %s", o.Status)
	}
	step("payment.authorized", func() (bool, []PendingEvent) { return o.ApplyPaymentAuthorized(events.PaymentAuthorizedPayload{}) })
	step("fulfillment.started", func() (bool, []PendingEvent) { return o.ApplyFulfillmentStarted(events.FulfillmentStartedPayload{}) })

	next := step("fulfillment.completed", func() (bool, []PendingEvent) {
		return o.ApplyFulfillmentCompleted(events.FulfillmentCompletedPayload{})
	})
	if o.Status != StatusCompleted {
		t.Fatalf("expected completed, got %s", o.Status)
	}
	types := eventTypes(next)
	if !contains(types, events.OrderCompletedV1) || !contains(types, events.NotificationScheduledV1) {
		t.Errorf("expected order.completed and notification.scheduled, got %v", types)
	}
	if !o.Status.Terminal() {
		t.Error("completed must be terminal")
	}
}

// TestCrossTopicReordering covers the fact that the Order service consumes the
// inventory, payment, and fulfillment streams independently, so events can
// arrive out of causal order.
func TestCrossTopicReordering(t *testing.T) {
	o := newTestOrder()

	// payment.authorized arrives before inventory.reserved.
	if changed, _ := o.ApplyPaymentAuthorized(events.PaymentAuthorizedPayload{}); !changed {
		t.Fatal("payment.authorized should be recorded")
	}
	if o.Status != StatusPaymentAuthorized {
		t.Fatalf("status = %s", o.Status)
	}

	// The late inventory.reserved must not regress the status.
	changed, next := o.ApplyInventoryReserved(events.InventoryReservedPayload{})
	if !changed {
		t.Fatal("inventory.reserved should be recorded")
	}
	if len(next) != 0 {
		t.Fatalf("late reservation must not emit events, got %v", eventTypes(next))
	}
	if o.Status != StatusPaymentAuthorized {
		t.Fatalf("status regressed to %s", o.Status)
	}

	if changed, _ := o.ApplyFulfillmentStarted(events.FulfillmentStartedPayload{}); !changed {
		t.Fatal("fulfillment.started should change state")
	}
	if changed, _ := o.ApplyFulfillmentCompleted(events.FulfillmentCompletedPayload{}); !changed {
		t.Fatal("fulfillment.completed should complete the order")
	}
	if o.Status != StatusCompleted {
		t.Fatalf("status = %s", o.Status)
	}
}

func TestInventoryRejectedCancels(t *testing.T) {
	o := newTestOrder()
	changed, next := o.ApplyInventoryRejected(events.InventoryRejectedPayload{OrderID: "o-1", Reason: "no stock"})
	if !changed {
		t.Fatal("expected state change")
	}
	if o.Status != StatusCancelled || o.CancelStage != cancelStageInventory {
		t.Fatalf("status=%s stage=%s", o.Status, o.CancelStage)
	}
	if types := eventTypes(next); !contains(types, events.OrderCancelledV1) {
		t.Errorf("expected order.cancelled, got %v", types)
	}
}

func TestPaymentFailureCompensatesInventory(t *testing.T) {
	o := newTestOrder()
	o.ApplyInventoryReserved(events.InventoryReservedPayload{})
	o.ApplyPaymentAuthorized(events.PaymentAuthorizedPayload{})

	changed, next := o.ApplyPaymentFailed(events.PaymentFailedPayload{Reason: "declined"})
	if !changed || o.Status != StatusCancelling {
		t.Fatalf("expected cancelling, got %s", o.Status)
	}
	if len(next) != 0 {
		t.Fatalf("payment failure must wait for compensation, got %d events", len(next))
	}

	changed, next = o.ApplyInventoryReleased(events.InventoryReleasedPayload{})
	if !changed || o.Status != StatusCancelled {
		t.Fatalf("expected cancelled after release, got %s", o.Status)
	}
	if types := eventTypes(next); !contains(types, events.OrderCancelledV1) {
		t.Errorf("expected order.cancelled, got %v", types)
	}
}

// TestPaymentFailureReordered covers payment.failed arriving before the order
// observes inventory.reserved.
func TestPaymentFailureReordered(t *testing.T) {
	o := newTestOrder()
	if changed, _ := o.ApplyPaymentFailed(events.PaymentFailedPayload{Reason: "declined"}); !changed {
		t.Fatal("payment.failed should start cancellation")
	}
	if o.Status != StatusCancelling {
		t.Fatalf("status = %s", o.Status)
	}
	if _, next := o.ApplyInventoryReleased(events.InventoryReleasedPayload{}); !contains(eventTypes(next), events.OrderCancelledV1) {
		t.Fatalf("expected cancellation after release, got %v", eventTypes(next))
	}
	if o.Status != StatusCancelled {
		t.Fatalf("status = %s", o.Status)
	}
}

func TestFulfillmentFailureCompensatesRefundAndRelease(t *testing.T) {
	o := newTestOrder()
	o.ApplyInventoryReserved(events.InventoryReservedPayload{})
	o.ApplyPaymentAuthorized(events.PaymentAuthorizedPayload{})
	o.ApplyFulfillmentStarted(events.FulfillmentStartedPayload{})

	changed, _ := o.ApplyFulfillmentFailed(events.FulfillmentFailedPayload{Reason: "warehouse"})
	if !changed || o.Status != StatusCancelling || o.CancelStage != cancelStageFulfillment {
		t.Fatalf("status=%s stage=%s", o.Status, o.CancelStage)
	}

	// One compensation so far: the order is not yet cancelled.
	changed, next := o.ApplyInventoryReleased(events.InventoryReleasedPayload{})
	if !changed {
		t.Fatal("the release fact should be recorded")
	}
	if len(next) != 0 {
		t.Fatalf("must wait for both refund and release, got %v", eventTypes(next))
	}
	if o.Status != StatusCancelling {
		t.Fatalf("still cancelling, got %s", o.Status)
	}

	changed, next = o.ApplyPaymentRefunded(events.PaymentRefundedPayload{})
	if !changed || o.Status != StatusCancelled {
		t.Fatalf("expected cancelled, got %s", o.Status)
	}
	if types := eventTypes(next); !contains(types, events.OrderCancelledV1) {
		t.Errorf("expected order.cancelled, got %v", types)
	}
}

func TestDuplicateEventsAreNoOps(t *testing.T) {
	o := newTestOrder()
	o.ApplyInventoryReserved(events.InventoryReservedPayload{})
	o.ApplyPaymentAuthorized(events.PaymentAuthorizedPayload{})

	if changed, _ := o.ApplyInventoryReserved(events.InventoryReservedPayload{}); changed {
		t.Error("duplicate inventory.reserved should be a no-op")
	}
	if changed, _ := o.ApplyPaymentAuthorized(events.PaymentAuthorizedPayload{}); changed {
		t.Error("duplicate payment.authorized should be a no-op")
	}
	o.ApplyFulfillmentStarted(events.FulfillmentStartedPayload{})
	o.ApplyFulfillmentCompleted(events.FulfillmentCompletedPayload{})
	if changed, _ := o.ApplyFulfillmentCompleted(events.FulfillmentCompletedPayload{}); changed {
		t.Error("duplicate fulfillment.completed should be a no-op")
	}
	if o.Status != StatusCompleted {
		t.Fatalf("status = %s", o.Status)
	}
}

func eventTypes(list []PendingEvent) []string {
	out := make([]string, 0, len(list))
	for _, e := range list {
		out = append(out, e.EventType)
	}
	return out
}

func contains(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
