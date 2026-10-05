package order

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/platform/apierror"
	"github.com/example/orbit/internal/platform/correlation"
	"github.com/example/orbit/internal/platform/postgres"
)

// Service implements order commands and queries.
type Service struct {
	pool         *pgxpool.Pool
	repo         *Repository
	prefix       string
	enableFaults bool
	log          *slog.Logger
}

// NewService constructs an order service.
func NewService(pool *pgxpool.Pool, repo *Repository, topicPrefix string, enableFaults bool, log *slog.Logger) *Service {
	return &Service{pool: pool, repo: repo, prefix: topicPrefix, enableFaults: enableFaults, log: log}
}

// CreateOrderRequest is the client-facing request body.
type CreateOrderRequest struct {
	CustomerID string            `json:"customer_id"`
	Items      []CreateOrderItem `json:"items"`
	Currency   string            `json:"currency"`
	Faults     map[string]string `json:"faults,omitempty"`
}

// CreateOrderItem is a requested line item.
type CreateOrderItem struct {
	SKU            string `json:"sku"`
	Quantity       int    `json:"quantity"`
	UnitPriceCents int64  `json:"unit_price_cents"`
}

var allowedFaults = map[string]struct{}{
	events.FaultInventoryReject:  {},
	events.FaultPaymentReject:    {},
	events.FaultPaymentTimeout:   {},
	events.FaultFulfillmentFail:  {},
	events.FaultNotificationFail: {},
}

// CreateOrder validates and persists a new order and its order.created.v1 event
// in a single transaction. When idempotencyKey is non-empty, a repeated request
// with the same body returns the original order; a different body is rejected.
func (s *Service) CreateOrder(ctx context.Context, req CreateOrderRequest, idempotencyKey string) (*Order, bool, error) {
	if err := s.validate(req); err != nil {
		return nil, false, err
	}

	var total int64
	items := make([]events.OrderItem, 0, len(req.Items))
	for _, it := range req.Items {
		items = append(items, events.OrderItem{SKU: it.SKU, Quantity: it.Quantity, UnitPriceCents: it.UnitPriceCents})
		total += it.UnitPriceCents * int64(it.Quantity)
	}

	now := time.Now().UTC()
	o := &Order{
		ID:            uuid.NewString(),
		CustomerID:    req.CustomerID,
		Items:         items,
		TotalCents:    total,
		Currency:      req.Currency,
		Status:        StatusPending,
		Faults:        req.Faults,
		CorrelationID: correlation.From(ctx),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if err := o.Validate(); err != nil {
		return nil, false, apierror.Invalid(err.Error())
	}

	env, err := events.New(events.OrderCreatedV1, events.AggregateOrder, o.ID, events.OrderCreatedPayload{
		OrderID:    o.ID,
		CustomerID: o.CustomerID,
		Items:      o.Items,
		TotalCents: o.TotalCents,
		Currency:   o.Currency,
		Faults:     req.Faults,
	}, o.CorrelationID, "")
	if err != nil {
		return nil, false, apierror.Internal("failed to build order event").WithCause(err)
	}

	requestHash := hashRequest(req)
	replayed := false

	err = postgres.WithTx(ctx, s.pool, func(tx pgx.Tx) error {
		if idempotencyKey != "" {
			rec, inserted, err := s.repo.BeginIdempotency(ctx, tx, idempotencyKey, requestHash)
			if err != nil {
				return apierror.Internal("idempotency check failed").WithCause(err)
			}
			if !inserted {
				if rec.RequestHash != requestHash {
					return apierror.IdempotencyConflict("idempotency key was used with a different request body")
				}
				if rec.State != "completed" {
					return apierror.Conflict("a request with this idempotency key is still in progress")
				}
				replayed = true
				return json.Unmarshal(rec.ResponseBody, o)
			}
		}

		if err := s.repo.Create(ctx, tx, o, env); err != nil {
			return apierror.Internal("failed to persist order").WithCause(err)
		}
		if idempotencyKey != "" {
			body, err := json.Marshal(o)
			if err != nil {
				return apierror.Internal("failed to store idempotent response").WithCause(err)
			}
			if err := s.repo.CompleteIdempotency(ctx, tx, idempotencyKey, 201, body); err != nil {
				return apierror.Internal("failed to finalise idempotency record").WithCause(err)
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return o, replayed, nil
}

// GetOrder loads an order by ID.
func (s *Service) GetOrder(ctx context.Context, id string) (*Order, error) {
	o, err := s.repo.Get(ctx, s.pool, id)
	if errors.Is(err, ErrOrderNotFound) {
		return nil, apierror.NotFound("order not found")
	}
	if err != nil {
		return nil, apierror.Internal("failed to load order").WithCause(err)
	}
	return o, nil
}

// ListOrders returns a page of orders and a cursor for the next page.
func (s *Service) ListOrders(ctx context.Context, f ListFilter) ([]*Order, string, error) {
	orders, next, err := s.repo.List(ctx, s.pool, f)
	if err != nil {
		return nil, "", apierror.Invalid(err.Error())
	}
	return orders, next, nil
}

// Timeline returns the distributed timeline for an order.
func (s *Service) Timeline(ctx context.Context, id string) ([]TimelineEntry, error) {
	if _, err := s.GetOrder(ctx, id); err != nil {
		return nil, err
	}
	entries, err := s.repo.Timeline(ctx, s.pool, id)
	if err != nil {
		return nil, apierror.Internal("failed to load timeline").WithCause(err)
	}
	return entries, nil
}

func (s *Service) validate(req CreateOrderRequest) error {
	if req.CustomerID == "" {
		return apierror.Invalid("customer_id is required")
	}
	if len(req.Items) == 0 {
		return apierror.Invalid("at least one item is required")
	}
	for i, it := range req.Items {
		if it.SKU == "" {
			return apierror.Invalid(fmt.Sprintf("items[%d].sku is required", i))
		}
		if it.Quantity <= 0 {
			return apierror.Invalid(fmt.Sprintf("items[%d].quantity must be positive", i))
		}
		if it.UnitPriceCents < 0 {
			return apierror.Invalid(fmt.Sprintf("items[%d].unit_price_cents must not be negative", i))
		}
	}
	if len(req.Currency) != 3 {
		return apierror.Invalid("currency must be a 3-letter ISO code")
	}
	if len(req.Faults) > 0 {
		if !s.enableFaults {
			return apierror.Invalid("fault injection is not enabled on this deployment")
		}
		for name := range req.Faults {
			if _, ok := allowedFaults[name]; !ok {
				return apierror.Invalid("unknown fault: " + name)
			}
		}
	}
	return nil
}

func hashRequest(req CreateOrderRequest) string {
	raw, _ := json.Marshal(req)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

// Log returns the service logger.
func (s *Service) Log() *slog.Logger { return s.log }
