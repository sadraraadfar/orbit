//go:build integration

// Package harness starts a self-contained Orbit stack against real PostgreSQL
// and Redpanda instances for integration and end-to-end tests.
//
// Each harness run uses a unique database suffix and Kafka topic prefix so that
// concurrent or repeated runs never observe each other's state.
package harness

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/fulfillment"
	"github.com/example/orbit/internal/inventory"
	"github.com/example/orbit/internal/notification"
	"github.com/example/orbit/internal/order"
	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/payment"
	"github.com/example/orbit/internal/platform/broker"
	"github.com/example/orbit/internal/platform/httpx"
	"github.com/example/orbit/internal/platform/metrics"
	"github.com/example/orbit/internal/platform/migrate"
	"github.com/example/orbit/internal/platform/migrations"
)

var services = []string{"order", "inventory", "payment", "fulfillment", "notification"}

// Harness owns a running Orbit stack.
type Harness struct {
	T        *testing.T
	Prefix   string
	Order    *order.Service
	OrderAPI *httptest.Server

	brokers  []string
	log      *slog.Logger
	producer *broker.Producer
	pools    map[string]*pgxpool.Pool
	cancel   context.CancelFunc
	wg       sync.WaitGroup
}

// Start boots the harness and registers cleanup.
func Start(t *testing.T) *Harness {
	t.Helper()
	if os.Getenv("ORBIT_TEST_INTEGRATION") != "1" {
		t.Skip("set ORBIT_TEST_INTEGRATION=1 to run integration tests")
	}

	adminURL := envOrDefault("ORBIT_TEST_PG_ADMIN_URL", "postgres://orbit:orbit@localhost:5432/postgres?sslmode=disable")
	brokers := splitList(envOrDefault("ORBIT_TEST_KAFKA_BROKERS", "localhost:9092"))

	admin, err := pgxpool.New(context.Background(), adminURL)
	if err != nil {
		t.Fatalf("connect admin database: %v", err)
	}
	defer admin.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	prefix := "orbit_it_" + suffix

	h := &Harness{
		T:       t,
		Prefix:  prefix,
		brokers: brokers,
		pools:   make(map[string]*pgxpool.Pool),
	}
	h.log = slog.New(slog.NewTextHandler(io.Discard, nil))

	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel

	for _, svc := range services {
		dbName := fmt.Sprintf("orbit_it_%s_%s", svc, suffix)
		if _, err := admin.Exec(ctx, "CREATE DATABASE "+dbName); err != nil {
			t.Fatalf("create database %s: %v", dbName, err)
		}
		t.Cleanup(func() {
			cleanupAdmin, err := pgxpool.New(context.Background(), adminURL)
			if err != nil {
				return
			}
			defer cleanupAdmin.Close()
			_, _ = cleanupAdmin.Exec(context.Background(), "DROP DATABASE IF EXISTS "+dbName+" WITH (FORCE)")
		})

		databaseURL := replaceDatabase(adminURL, dbName)
		if err := migrate.Up(migrationFS(t, svc), ".", databaseURL); err != nil {
			t.Fatalf("migrate %s: %v", svc, err)
		}
		pool, err := pgxpool.New(ctx, databaseURL)
		if err != nil {
			t.Fatalf("pool %s: %v", svc, err)
		}
		h.pools[svc] = pool
		t.Cleanup(pool.Close)
	}

	producer := broker.NewProducer(brokers)
	h.producer = producer
	t.Cleanup(func() { _ = producer.Close() })

	if err := broker.EnsureTopics(ctx, brokers, events.AllTopics(prefix), 3); err != nil {
		t.Fatalf("ensure topics: %v", err)
	}

	// Order service: HTTP API plus choreography consumer.
	orderStore := outbox.NewStore(prefix)
	orderRepo := order.NewRepository(orderStore)
	h.Order = order.NewService(h.pools["order"], orderRepo, prefix, true, h.log)

	mux := http.NewServeMux()
	order.NewAPI(h.Order, h.log).Register(mux)
	h.OrderAPI = httptest.NewServer(httpx.Chain(mux, httpx.RequestID(), httpx.Recovery(h.log)))
	t.Cleanup(h.OrderAPI.Close)

	orderHandler := order.NewEventHandler(h.Order)

	// Inventory service.
	inventoryStore := outbox.NewStore(prefix)
	inventoryHandler := inventory.NewHandler(inventory.NewRepository(inventoryStore), h.log)

	// Payment service.
	paymentStore := outbox.NewStore(prefix)
	paymentHandler := payment.NewHandler(payment.NewRepository(paymentStore), h.log)

	// Fulfillment service.
	fulfillmentStore := outbox.NewStore(prefix)
	fulfillmentHandler := fulfillment.NewHandler(fulfillment.NewRepository(fulfillmentStore), h.log)

	// Notification service.
	notificationStore := outbox.NewStore(prefix)
	notificationHandler := notification.NewHandler(notification.NewRepository(notificationStore), h.log)

	for _, svc := range services {
		var store *outbox.Store
		switch svc {
		case "order":
			store = orderStore
		case "inventory":
			store = inventoryStore
		case "payment":
			store = paymentStore
		case "fulfillment":
			store = fulfillmentStore
		case "notification":
			store = notificationStore
		}
		h.publishOutbox(ctx, svc, store)
	}

	h.consume(ctx, "order", "inventory", orderHandler)
	h.consume(ctx, "order", "payment", orderHandler)
	h.consume(ctx, "order", "fulfillment", orderHandler)

	h.consume(ctx, "inventory", "order", inventoryHandler)
	h.consume(ctx, "inventory", "payment", inventoryHandler)
	h.consume(ctx, "inventory", "fulfillment", inventoryHandler)

	h.consume(ctx, "payment", "inventory", paymentHandler)
	h.consume(ctx, "payment", "fulfillment", paymentHandler)

	h.consume(ctx, "fulfillment", "payment", fulfillmentHandler)

	h.consume(ctx, "notification", "notification", notificationHandler)

	t.Cleanup(h.Stop)
	return h
}

// Stop cancels all background tasks.
func (h *Harness) Stop() {
	h.cancel()
	h.wg.Wait()
}

// SeedInventory inserts stock for the supplied SKU.
func (h *Harness) SeedInventory(sku string, available int) {
	if _, err := h.pools["inventory"].Exec(context.Background(), `
		INSERT INTO inventory_items (sku, available) VALUES ($1, $2)
		ON CONFLICT (sku) DO UPDATE SET available = EXCLUDED.available`, sku, available); err != nil {
		h.T.Fatalf("seed inventory: %v", err)
	}
}

func (h *Harness) publishOutbox(_ context.Context, service string, store *outbox.Store) {
	m := metrics.New(service)
	publisher := outbox.NewPublisher(store, h.pools[service], h.producer, h.log, m, outbox.PublisherConfig{
		PollInterval: 100 * time.Millisecond,
		BatchSize:    50,
		MaxAttempts:  5,
	})
	h.run("outbox."+service, publisher.Run)
}

func (h *Harness) consume(_ context.Context, service, family string, handler consumer.Handler) {
	group := service + "." + family + "." + h.Prefix
	runner := consumer.BuildRunner(consumer.RunnerConfig{
		Service:    service,
		Group:      group,
		Topic:      events.StreamTopic(h.Prefix, family),
		Brokers:    h.brokers,
		MaxRetries: 3,
		Backoff:    100 * time.Millisecond,
	}, handler, h.pools[service], h.producer, metrics.New(service+"-"+family), h.log)
	h.run("consumer."+group, runner.Run)
}

func (h *Harness) run(name string, fn func(context.Context) error) {
	ctx, cancel := context.WithCancel(context.Background())
	h.wg.Add(1)
	go func() {
		defer h.wg.Done()
		defer cancel()
		if err := fn(ctx); err != nil && ctx.Err() == nil {
			h.T.Logf("task %s stopped: %v", name, err)
		}
	}()
}

// --- Client helpers ----------------------------------------------------------

type createResponse struct {
	Order             order.Order `json:"order"`
	IdempotencyReplay bool        `json:"idempotency_replayed"`
}

// CreateOrder performs an HTTP order creation.
func (h *Harness) CreateOrder(req order.CreateOrderRequest, idempotencyKey string) (order.Order, createResponse, int) {
	body, _ := json.Marshal(req)
	httpReq, _ := http.NewRequest(http.MethodPost, h.OrderAPI.URL+"/v1/orders", bytes.NewReader(body))
	httpReq.Header.Set("Content-Type", "application/json")
	if idempotencyKey != "" {
		httpReq.Header.Set("Idempotency-Key", idempotencyKey)
	}
	resp, err := http.DefaultClient.Do(httpReq)
	if err != nil {
		h.T.Fatalf("create order: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var parsed createResponse
	_ = json.Unmarshal(raw, &parsed)
	return parsed.Order, parsed, resp.StatusCode
}

// GetOrder loads an order through the service layer.
func (h *Harness) GetOrder(id string) (order.Order, error) {
	o, err := h.Order.GetOrder(context.Background(), id)
	if err != nil {
		return order.Order{}, err
	}
	return *o, nil
}

// Timeline returns an order's distributed timeline.
func (h *Harness) Timeline(id string) []order.TimelineEntry {
	entries, err := h.Order.Timeline(context.Background(), id)
	if err != nil {
		h.T.Fatalf("timeline: %v", err)
	}
	return entries
}

// WaitForStatus polls until the order reaches the expected status.
func (h *Harness) WaitForStatus(id string, status order.Status, timeout time.Duration) order.Order {
	h.T.Helper()
	deadline := time.Now().Add(timeout)
	var last order.Order
	for time.Now().Before(deadline) {
		o, err := h.GetOrder(id)
		if err == nil {
			last = o
			if o.Status == status {
				return o
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	h.T.Fatalf("order %s did not reach %s before timeout; last status %s", id, status, last.Status)
	return last
}

// --- helpers -----------------------------------------------------------------

func migrationFS(t *testing.T, service string) fs.FS {
	t.Helper()
	var embedded fs.FS
	switch service {
	case "order":
		embedded = order.Migrations
	case "inventory":
		embedded = inventory.Migrations
	case "payment":
		embedded = payment.Migrations
	case "fulfillment":
		embedded = fulfillment.Migrations
	case "notification":
		embedded = notification.Migrations
	default:
		t.Fatalf("unknown service %s", service)
	}
	filesystem, err := migrations.ServiceUnion(embedded, "migrations")
	if err != nil {
		t.Fatalf("migration fs for %s: %v", service, err)
	}
	return filesystem
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func replaceDatabase(base, name string) string {
	u, err := url.Parse(base)
	if err != nil {
		return base
	}
	u.Path = "/" + name
	return u.String()
}
