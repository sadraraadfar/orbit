//go:build integration

package harness

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"
)

// Pool exposes a service database pool for assertions.
func (h *Harness) Pool(service string) *pgxpool.Pool { return h.pools[service] }

// InventoryAvailable returns the available quantity for a SKU.
func (h *Harness) InventoryAvailable(sku string) int {
	var available int
	err := h.pools["inventory"].QueryRow(context.Background(),
		`SELECT available FROM inventory_items WHERE sku = $1`, sku).Scan(&available)
	if err != nil {
		h.T.Fatalf("inventory available: %v", err)
	}
	return available
}

// ReservationStatus returns the reservation status for an order, or an empty
// string when no reservation exists.
func (h *Harness) ReservationStatus(orderID string) string {
	var status string
	err := h.pools["inventory"].QueryRow(context.Background(),
		`SELECT status FROM reservations WHERE order_id = $1`, orderID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ""
		}
		h.T.Fatalf("reservation status: %v", err)
	}
	return status
}

// PaymentStatus returns the payment status for an order, or an empty string.
func (h *Harness) PaymentStatus(orderID string) string {
	var status string
	err := h.pools["payment"].QueryRow(context.Background(),
		`SELECT status FROM payments WHERE order_id = $1`, orderID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ""
		}
		h.T.Fatalf("payment status: %v", err)
	}
	return status
}

// FulfillmentStatus returns the fulfillment status for an order.
func (h *Harness) FulfillmentStatus(orderID string) string {
	var status string
	err := h.pools["fulfillment"].QueryRow(context.Background(),
		`SELECT status FROM fulfillments WHERE order_id = $1`, orderID).Scan(&status)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ""
		}
		h.T.Fatalf("fulfillment status: %v", err)
	}
	return status
}

// NotificationCount returns how many notifications were recorded for an order.
func (h *Harness) NotificationCount(orderID string) int {
	var count int
	if err := h.pools["notification"].QueryRow(context.Background(),
		`SELECT count(*) FROM notifications WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		h.T.Fatalf("notification count: %v", err)
	}
	return count
}

// ProcessedCount returns how many times a consumer recorded an event.
func (h *Harness) ProcessedCount(service, eventID string) int {
	var count int
	if err := h.pools[service].QueryRow(context.Background(),
		`SELECT count(*) FROM processed_messages WHERE event_id = $1`, eventID).Scan(&count); err != nil {
		h.T.Fatalf("processed count: %v", err)
	}
	return count
}

// PublishRaw publishes a raw value to a topic using the harness producer.
func (h *Harness) PublishRaw(topic, key string, value []byte) {
	if err := h.producer.Publish(context.Background(), topic, key, value, nil); err != nil {
		h.T.Fatalf("publish raw: %v", err)
	}
}

// FetchOutbox returns the most recent outbox payload for a service and event
// type along with its aggregate key.
func (h *Harness) FetchOutbox(service, eventType string) (string, []byte) {
	var aggregateID string
	var payload []byte
	err := h.pools[service].QueryRow(context.Background(), `
		SELECT aggregate_id, payload FROM outbox WHERE event_type = $1
		ORDER BY seq DESC LIMIT 1`, eventType).Scan(&aggregateID, &payload)
	if err != nil {
		h.T.Fatalf("fetch outbox %s/%s: %v", service, eventType, err)
	}
	return aggregateID, payload
}

// CollectDLQ reads up to limit messages from a dead-letter topic within timeout.
func (h *Harness) CollectDLQ(t *testing.T, topic string, limit int, timeout time.Duration) []kafka.Message {
	t.Helper()
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        h.brokers,
		GroupID:        "it-dlq-" + h.Prefix + "-" + topic,
		Topic:          topic,
		MinBytes:       1,
		MaxBytes:       10 << 20,
		MaxWait:        200 * time.Millisecond,
		StartOffset:    kafka.FirstOffset,
		CommitInterval: 0,
	})
	defer reader.Close()

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	var messages []kafka.Message
	for len(messages) < limit {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			break
		}
		messages = append(messages, msg)
	}
	return messages
}
