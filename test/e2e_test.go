//go:build integration

package e2e_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/order"
	"github.com/example/orbit/test/harness"
)

const wait = 25 * time.Second

func request(sku string, qty int, faults map[string]string) order.CreateOrderRequest {
	return order.CreateOrderRequest{
		CustomerID: "customer-1",
		Currency:   "USD",
		Items: []order.CreateOrderItem{
			{SKU: sku, Quantity: qty, UnitPriceCents: 1500},
		},
		Faults: faults,
	}
}

func TestOrderWorkflowSucceeds(t *testing.T) {
	h := harness.Start(t)
	h.SeedInventory("SKU-WIDGET", 10)

	o, _, status := h.CreateOrder(request("SKU-WIDGET", 2, nil), "success-1")
	if status != 201 {
		t.Fatalf("create status = %d", status)
	}

	completed := h.WaitForStatus(o.ID, order.StatusCompleted, wait)
	if completed.TotalCents != 3000 {
		t.Fatalf("total = %d", completed.TotalCents)
	}

	waitFor(t, wait, func() bool { return h.NotificationCount(o.ID) == 1 }, "notification sent")
	if got := h.InventoryAvailable("SKU-WIDGET"); got != 8 {
		t.Fatalf("available = %d, want 8", got)
	}
	if got := h.PaymentStatus(o.ID); got != "authorized" {
		t.Fatalf("payment status = %s", got)
	}

	assertTimelineContains(t, h, o.ID,
		events.OrderCreatedV1,
		events.InventoryReservedV1,
		events.PaymentAuthorizedV1,
		events.FulfillmentStartedV1,
		events.FulfillmentCompletedV1,
	)
}

func TestInventoryRejectionCancelsOrder(t *testing.T) {
	h := harness.Start(t)

	o, _, _ := h.CreateOrder(request("SKU-WIDGET", 1, map[string]string{
		events.FaultInventoryReject: "true",
	}), "inv-reject-1")

	cancelled := h.WaitForStatus(o.ID, order.StatusCancelled, wait)
	if cancelled.CancelStage != "inventory" {
		t.Fatalf("cancel stage = %s", cancelled.CancelStage)
	}
	if got := h.NotificationCount(o.ID); got != 0 {
		t.Fatalf("expected no notification, got %d", got)
	}
}

func TestPaymentFailureReleasesInventory(t *testing.T) {
	h := harness.Start(t)
	h.SeedInventory("SKU-GADGET", 5)

	o, _, _ := h.CreateOrder(request("SKU-GADGET", 2, map[string]string{
		events.FaultPaymentReject: "true",
	}), "pay-reject-1")

	h.WaitForStatus(o.ID, order.StatusCancelled, wait)

	if got := h.InventoryAvailable("SKU-GADGET"); got != 5 {
		t.Fatalf("inventory was not restored: available = %d, want 5", got)
	}
	if got := h.ReservationStatus(o.ID); got != "released" {
		t.Fatalf("reservation status = %s, want released", got)
	}
	if got := h.PaymentStatus(o.ID); got != "failed" {
		t.Fatalf("payment status = %s, want failed", got)
	}
	assertTimelineContains(t, h, o.ID, events.InventoryReleasedV1, events.OrderCancelledV1)
}

func TestFulfillmentFailureRefundsAndReleases(t *testing.T) {
	h := harness.Start(t)
	h.SeedInventory("SKU-SPROCKET", 5)

	o, _, _ := h.CreateOrder(request("SKU-SPROCKET", 1, map[string]string{
		events.FaultFulfillmentFail: "true",
	}), "fulfil-fail-1")

	h.WaitForStatus(o.ID, order.StatusCancelled, wait)

	if got := h.PaymentStatus(o.ID); got != "refunded" {
		t.Fatalf("payment status = %s, want refunded", got)
	}
	if got := h.ReservationStatus(o.ID); got != "released" {
		t.Fatalf("reservation status = %s, want released", got)
	}
	if got := h.InventoryAvailable("SKU-SPROCKET"); got != 5 {
		t.Fatalf("inventory was not restored: available = %d", got)
	}
	assertTimelineContains(t, h, o.ID, events.PaymentRefundedV1, events.InventoryReleasedV1, events.OrderCancelledV1)
}

func TestIdempotentOrderCreation(t *testing.T) {
	h := harness.Start(t)
	h.SeedInventory("SKU-WIDGET", 10)

	first, _, _ := h.CreateOrder(request("SKU-WIDGET", 1, nil), "idem-key-1")
	second, resp, _ := h.CreateOrder(request("SKU-WIDGET", 1, nil), "idem-key-1")

	if first.ID != second.ID {
		t.Fatalf("expected the same order id, got %s and %s", first.ID, second.ID)
	}
	if !resp.IdempotencyReplay {
		t.Fatal("expected idempotency_replayed=true on the second request")
	}
	// The same key with a different body must be rejected.
	_, _, status := h.CreateOrder(request("SKU-WIDGET", 2, nil), "idem-key-1")
	if status != 409 {
		t.Fatalf("expected 409 for key reuse with different body, got %d", status)
	}
}

func TestDuplicateEventIsIgnored(t *testing.T) {
	h := harness.Start(t)
	h.SeedInventory("SKU-WIDGET", 10)

	o, _, _ := h.CreateOrder(request("SKU-WIDGET", 2, nil), "dup-1")
	h.WaitForStatus(o.ID, order.StatusCompleted, wait)

	// Replay the original order.created.v1 event. The processed-message marker
	// must make the redelivery a no-op.
	key, payload := h.FetchOutbox("order", events.OrderCreatedV1)
	h.PublishRaw(events.StreamTopic(h.Prefix, "order"), key, payload)

	time.Sleep(3 * time.Second)

	if got := h.InventoryAvailable("SKU-WIDGET"); got != 8 {
		t.Fatalf("duplicate event changed inventory: available = %d, want 8", got)
	}
	var reservations int
	if err := h.Pool("inventory").QueryRow(t.Context(),
		`SELECT count(*) FROM reservations WHERE order_id = $1`, o.ID).Scan(&reservations); err != nil {
		t.Fatal(err)
	}
	if reservations != 1 {
		t.Fatalf("reservations = %d, want 1", reservations)
	}
}

func TestPoisonMessageIsDeadLettered(t *testing.T) {
	h := harness.Start(t)

	// A malformed message on the inventory stream is consumed by the order and
	// payment services, which must dead-letter it rather than crash.
	h.PublishRaw(events.StreamTopic(h.Prefix, "inventory"), "poison", []byte("{not valid json"))

	messages := h.CollectDLQ(t, events.DLQTopic(events.StreamTopic(h.Prefix, "inventory")), 1, wait)
	if len(messages) == 0 {
		t.Fatal("expected a dead-letter message")
	}
}

func TestPaymentTimeoutRetriesThenDeadLetters(t *testing.T) {
	h := harness.Start(t)
	h.SeedInventory("SKU-WIDGET", 10)

	o, _, _ := h.CreateOrder(request("SKU-WIDGET", 1, map[string]string{
		events.FaultPaymentTimeout: "true",
	}), "timeout-1")

	messages := h.CollectDLQ(t, events.DLQTopic(events.StreamTopic(h.Prefix, "payment")), 1, wait)
	if len(messages) == 0 {
		t.Fatalf("expected the timing-out payment event in the dead-letter topic (order %s)", o.ID)
	}
	headers := map[string]string{}
	for _, hd := range messages[0].Headers {
		headers[hd.Key] = string(hd.Value)
	}
	if headers["x-orbit-failure-class"] != "transient" {
		t.Fatalf("failure class = %s, want transient", headers["x-orbit-failure-class"])
	}

	// The order remains at inventory_reserved because payment never resolved.
	var env map[string]any
	_ = json.Unmarshal(messages[0].Value, &env)
	if env["aggregate_id"] != o.ID {
		t.Fatalf("dead-letter aggregate_id = %v, want %s", env["aggregate_id"], o.ID)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func assertTimelineContains(t *testing.T, h *harness.Harness, orderID string, want ...string) {
	t.Helper()
	seen := map[string]bool{}
	for _, entry := range h.Timeline(orderID) {
		seen[entry.EventType] = true
	}
	for _, eventType := range want {
		if !seen[eventType] {
			t.Errorf("timeline missing %s (have %v)", eventType, seen)
		}
	}
}
