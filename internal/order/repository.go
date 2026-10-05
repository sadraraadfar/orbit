package order

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
)

// ErrOrderNotFound is returned when an order does not exist.
var ErrOrderNotFound = errors.New("order not found")

// Repository persists orders, timelines, and idempotency records.
type Repository struct {
	outbox *outbox.Store
}

// NewRepository builds a repository that writes outbox records through store.
func NewRepository(store *outbox.Store) *Repository {
	return &Repository{outbox: store}
}

// Create inserts a new order, its creation timeline entry, and the supplied
// outbox event in the caller's transaction.
func (r *Repository) Create(ctx context.Context, tx pgx.Tx, o *Order, env events.Envelope) error {
	items, err := json.Marshal(o.Items)
	if err != nil {
		return fmt.Errorf("order: encode items: %w", err)
	}
	faults := o.Faults
	if faults == nil {
		faults = map[string]string{}
	}
	faultsRaw, err := json.Marshal(faults)
	if err != nil {
		return fmt.Errorf("order: encode faults: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO orders (id, customer_id, items, total_cents, currency, status,
			inventory_state, payment_state, fulfillment_state,
			cancel_stage, cancel_reason, inventory_released, payment_refunded, faults, correlation_id, created_at, updated_at)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
		o.ID, o.CustomerID, items, o.TotalCents, o.Currency, o.Status,
		o.InventoryState, o.PaymentState, o.FulfillmentState,
		o.CancelStage, o.CancelReason, o.InventoryReleased, o.PaymentRefunded,
		faultsRaw, o.CorrelationID, o.CreatedAt, o.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("order: insert: %w", err)
	}
	if err := r.outbox.Append(ctx, tx, env); err != nil {
		return err
	}
	return r.AddTimeline(ctx, tx, o.ID, TimelineEntry{
		EventID:       env.EventID,
		EventType:     env.EventType,
		OccurredAt:    env.OccurredAt,
		CorrelationID: env.CorrelationID,
		CausationID:   env.CausationID,
		Summary:       map[string]any{"status": string(o.Status), "total_cents": o.TotalCents},
	})
}

// Get loads an order by ID.
func (r *Repository) Get(ctx context.Context, pool *pgxpool.Pool, id string) (*Order, error) {
	row := pool.QueryRow(ctx, selectOrderColumns+` WHERE id = $1`, id)
	o, err := scanOrder(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOrderNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("order: get: %w", err)
	}
	return o, nil
}

// Lock loads an order with a row lock for update inside tx.
func (r *Repository) Lock(ctx context.Context, tx pgx.Tx, id string) (*Order, error) {
	row := tx.QueryRow(ctx, selectOrderColumns+` WHERE id = $1 FOR UPDATE`, id)
	o, err := scanOrder(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrOrderNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("order: lock: %w", err)
	}
	return o, nil
}

// Update persists mutable order fields.
func (r *Repository) Update(ctx context.Context, tx pgx.Tx, o *Order) error {
	o.UpdatedAt = time.Now().UTC()
	tag, err := tx.Exec(ctx, `
		UPDATE orders SET status=$2, inventory_state=$3, payment_state=$4,
			fulfillment_state=$5, cancel_stage=$6, cancel_reason=$7,
			inventory_released=$8, payment_refunded=$9, updated_at=$10
		WHERE id=$1`,
		o.ID, o.Status, o.InventoryState, o.PaymentState, o.FulfillmentState,
		o.CancelStage, o.CancelReason, o.InventoryReleased, o.PaymentRefunded, o.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("order: update: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrOrderNotFound
	}
	return nil
}

// AddTimeline appends a timeline entry. Duplicate (order_id, event_id) pairs are
// ignored so redelivery does not create duplicate timeline rows.
func (r *Repository) AddTimeline(ctx context.Context, tx pgx.Tx, orderID string, e TimelineEntry) error {
	summary := e.Summary
	if summary == nil {
		summary = map[string]any{}
	}
	raw, err := json.Marshal(summary)
	if err != nil {
		return fmt.Errorf("order: encode timeline summary: %w", err)
	}
	_, err = tx.Exec(ctx, `
		INSERT INTO order_timeline (order_id, event_id, event_type, occurred_at, correlation_id, causation_id, summary)
		VALUES ($1,$2,$3,$4,$5,$6,$7)
		ON CONFLICT (order_id, event_id) DO NOTHING`,
		orderID, e.EventID, e.EventType, e.OccurredAt, e.CorrelationID, e.CausationID, raw,
	)
	if err != nil {
		return fmt.Errorf("order: insert timeline: %w", err)
	}
	return nil
}

// AppendOutbox appends an event to the outbox using the caller's transaction.
func (r *Repository) AppendOutbox(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	return r.outbox.Append(ctx, tx, env)
}

const selectOrderColumns = `
	SELECT id, customer_id, items, total_cents, currency, status,
		inventory_state, payment_state, fulfillment_state,
		cancel_stage, cancel_reason, inventory_released, payment_refunded,
		faults, correlation_id, created_at, updated_at
	FROM orders`

func scanOrder(row pgx.Row) (*Order, error) {
	var (
		o         Order
		items     []byte
		faultsRaw []byte
	)
	if err := row.Scan(
		&o.ID, &o.CustomerID, &items, &o.TotalCents, &o.Currency, &o.Status,
		&o.InventoryState, &o.PaymentState, &o.FulfillmentState,
		&o.CancelStage, &o.CancelReason, &o.InventoryReleased, &o.PaymentRefunded,
		&faultsRaw, &o.CorrelationID, &o.CreatedAt, &o.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(items, &o.Items); err != nil {
		return nil, fmt.Errorf("order: decode items: %w", err)
	}
	if len(faultsRaw) > 0 {
		if err := json.Unmarshal(faultsRaw, &o.Faults); err != nil {
			return nil, fmt.Errorf("order: decode faults: %w", err)
		}
	}
	return &o, nil
}

// ListFilter controls order listing.
type ListFilter struct {
	Status     string
	CustomerID string
	Limit      int
	Cursor     string
	Ascending  bool
}

// List returns orders using keyset pagination. The returned cursor is empty when
// no further pages exist.
func (r *Repository) List(ctx context.Context, pool *pgxpool.Pool, f ListFilter) ([]*Order, string, error) {
	if f.Limit <= 0 || f.Limit > 100 {
		f.Limit = 20
	}
	direction := "DESC"
	comparison := "<"
	if f.Ascending {
		direction = "ASC"
		comparison = ">"
	}

	args := []any{}
	where := "WHERE 1=1"
	if f.Status != "" {
		args = append(args, f.Status)
		where += fmt.Sprintf(" AND status = $%d", len(args))
	}
	if f.CustomerID != "" {
		args = append(args, f.CustomerID)
		where += fmt.Sprintf(" AND customer_id = $%d", len(args))
	}
	if f.Cursor != "" {
		cur, err := decodeCursor(f.Cursor)
		if err != nil {
			return nil, "", fmt.Errorf("order: invalid cursor: %w", err)
		}
		args = append(args, cur.CreatedAt, cur.ID)
		where += fmt.Sprintf(" AND (created_at, id) %s ($%d, $%d)", comparison, len(args)-1, len(args))
	}

	args = append(args, f.Limit+1)
	query := fmt.Sprintf(`%s %s ORDER BY created_at %s, id %s LIMIT $%d`,
		selectOrderColumns, where, direction, direction, len(args))

	rows, err := pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("order: list: %w", err)
	}
	defer rows.Close()

	var orders []*Order
	for rows.Next() {
		o, err := scanOrder(rows)
		if err != nil {
			return nil, "", fmt.Errorf("order: scan list: %w", err)
		}
		orders = append(orders, o)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(orders) > f.Limit {
		orders = orders[:f.Limit]
		last := orders[len(orders)-1]
		nextCursor = encodeCursor(orderCursor{CreatedAt: last.CreatedAt, ID: last.ID})
	}
	return orders, nextCursor, nil
}

// Timeline returns the ordered distributed timeline for an order.
func (r *Repository) Timeline(ctx context.Context, pool *pgxpool.Pool, orderID string) ([]TimelineEntry, error) {
	rows, err := pool.Query(ctx, `
		SELECT event_id, event_type, occurred_at, correlation_id, causation_id, summary, recorded_at
		FROM order_timeline WHERE order_id = $1 ORDER BY occurred_at ASC, recorded_at ASC`, orderID)
	if err != nil {
		return nil, fmt.Errorf("order: timeline query: %w", err)
	}
	defer rows.Close()

	var entries []TimelineEntry
	for rows.Next() {
		var (
			e       TimelineEntry
			summary []byte
		)
		if err := rows.Scan(&e.EventID, &e.EventType, &e.OccurredAt, &e.CorrelationID, &e.CausationID, &summary, &e.RecordedAt); err != nil {
			return nil, fmt.Errorf("order: timeline scan: %w", err)
		}
		if err := json.Unmarshal(summary, &e.Summary); err != nil {
			return nil, fmt.Errorf("order: timeline summary: %w", err)
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

type orderCursor struct {
	CreatedAt time.Time `json:"created_at"`
	ID        string    `json:"id"`
}

func encodeCursor(c orderCursor) string {
	raw, _ := json.Marshal(c)
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeCursor(s string) (orderCursor, error) {
	var c orderCursor
	raw, err := base64.RawURLEncoding.DecodeString(s)
	if err != nil {
		return c, err
	}
	err = json.Unmarshal(raw, &c)
	return c, err
}

// IdempotencyRecord is a stored idempotent request.
type IdempotencyRecord struct {
	Key            string
	RequestHash    string
	State          string
	ResponseStatus int
	ResponseBody   []byte
}

// ErrIdempotencyInFlight indicates a concurrent request with the same key is
// still being processed.
var ErrIdempotencyInFlight = errors.New("idempotent request in flight")

// BeginIdempotency inserts a pending idempotency record. It returns the existing
// record and inserted=false when the key was already present.
func (r *Repository) BeginIdempotency(ctx context.Context, tx pgx.Tx, key, requestHash string) (*IdempotencyRecord, bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO idempotency_keys (key, request_hash, state)
		VALUES ($1, $2, 'pending')
		ON CONFLICT (key) DO NOTHING`, key, requestHash)
	if err != nil {
		return nil, false, fmt.Errorf("order: begin idempotency: %w", err)
	}
	if tag.RowsAffected() == 1 {
		return nil, true, nil
	}

	var rec IdempotencyRecord
	var body []byte
	err = tx.QueryRow(ctx, `
		SELECT key, request_hash, state, COALESCE(response_status, 0), response_body
		FROM idempotency_keys WHERE key = $1`, key,
	).Scan(&rec.Key, &rec.RequestHash, &rec.State, &rec.ResponseStatus, &body)
	if err != nil {
		return nil, false, fmt.Errorf("order: read idempotency: %w", err)
	}
	rec.ResponseBody = body
	return &rec, false, nil
}

// CompleteIdempotency stores the response for a previously begun key.
func (r *Repository) CompleteIdempotency(ctx context.Context, tx pgx.Tx, key string, status int, body []byte) error {
	_, err := tx.Exec(ctx, `
		UPDATE idempotency_keys SET state = 'completed', response_status = $2, response_body = $3
		WHERE key = $1`, key, status, body)
	if err != nil {
		return fmt.Errorf("order: complete idempotency: %w", err)
	}
	return nil
}
