// Package outbox implements the transactional outbox pattern.
//
// Domain state changes and outbox records are written in the same database
// transaction. A separate publisher worker later moves records to the broker.
// Publication is at-least-once: a record may be published more than once if the
// process crashes between the broker acknowledgement and the database update,
// so consumers must be idempotent.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/platform/broker"
)

// Record is a claimed outbox row awaiting publication.
type Record struct {
	ID            string
	AggregateType string
	AggregateID   string
	EventType     string
	EventVersion  int
	Topic         string
	Payload       []byte
	Headers       map[string]string
	Attempts      int
}

// Store persists and claims outbox records for one service database.
type Store struct {
	topicPrefix string
}

// NewStore builds an outbox store whose topics are derived from prefix.
func NewStore(topicPrefix string) *Store {
	return &Store{topicPrefix: topicPrefix}
}

// Append writes env to the outbox using the caller's transaction. It must be
// called in the same transaction as the domain state change.
func (s *Store) Append(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	if err := env.Validate(); err != nil {
		return fmt.Errorf("outbox: refusing to append invalid event: %w", err)
	}
	raw, err := env.Encode()
	if err != nil {
		return fmt.Errorf("outbox: encode envelope: %w", err)
	}
	headers, err := json.Marshal(broker.EventHeaders(env.EventType, env.EventVersion, env.CorrelationID))
	if err != nil {
		return fmt.Errorf("outbox: encode headers: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox (aggregate_type, aggregate_id, event_type, event_version, topic, payload, headers, occurred_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		env.AggregateType, env.AggregateID, env.EventType, env.EventVersion,
		events.TopicFor(s.topicPrefix, env.EventType), raw, headers, env.OccurredAt,
	)
	if err != nil {
		return fmt.Errorf("outbox: insert: %w", err)
	}
	return nil
}

// Emit builds an envelope for payload and appends it to the outbox using tx. It
// is the single entry point domain handlers use to schedule an event.
func Emit(ctx context.Context, tx pgx.Tx, store *Store, aggregateType, aggregateID, eventType string, payload events.Payload, correlationID, causationID string) error {
	env, err := events.New(eventType, aggregateType, aggregateID, payload, correlationID, causationID)
	if err != nil {
		return err
	}
	return store.Append(ctx, tx, env)
}

// Claim selects up to limit due records and leases them for the supplied
// duration. Row locks are held only for the duration of the claim transaction.
func (s *Store) Claim(ctx context.Context, pool *pgxpool.Pool, limit int, lease time.Duration) ([]Record, error) {
	rows, err := pool.Query(ctx, `
		WITH claimed AS (
			SELECT id FROM outbox
			WHERE published_at IS NULL AND available_at <= now()
			ORDER BY seq
			FOR UPDATE SKIP LOCKED
			LIMIT $1
		)
		UPDATE outbox o
		SET attempts = o.attempts + 1,
		    available_at = now() + $2::interval
		FROM claimed c
		WHERE o.id = c.id
		RETURNING o.id, o.aggregate_type, o.aggregate_id, o.event_type, o.event_version,
		          o.topic, o.payload, o.headers, o.attempts`,
		limit, fmt.Sprintf("%f seconds", lease.Seconds()),
	)
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	defer rows.Close()

	var records []Record
	for rows.Next() {
		var (
			rec     Record
			payload []byte
			headers []byte
		)
		if err := rows.Scan(
			&rec.ID, &rec.AggregateType, &rec.AggregateID, &rec.EventType, &rec.EventVersion,
			&rec.Topic, &payload, &headers, &rec.Attempts,
		); err != nil {
			return nil, fmt.Errorf("outbox: scan: %w", err)
		}
		rec.Payload = payload
		if err := json.Unmarshal(headers, &rec.Headers); err != nil {
			return nil, fmt.Errorf("outbox: decode headers: %w", err)
		}
		records = append(records, rec)
	}
	return records, rows.Err()
}

// MarkPublished records successful publication.
func (s *Store) MarkPublished(ctx context.Context, pool *pgxpool.Pool, id string) error {
	_, err := pool.Exec(ctx, `UPDATE outbox SET published_at = now(), last_error = NULL WHERE id = $1`, id)
	if err != nil {
		return fmt.Errorf("outbox: mark published: %w", err)
	}
	return nil
}

// MarkFailed records a failed attempt and reschedules the record after backoff.
func (s *Store) MarkFailed(ctx context.Context, pool *pgxpool.Pool, id, cause string, backoff time.Duration) error {
	_, err := pool.Exec(ctx, `
		UPDATE outbox SET last_error = $2, available_at = now() + $3::interval WHERE id = $1`,
		id, cause, fmt.Sprintf("%f seconds", backoff.Seconds()),
	)
	if err != nil {
		return fmt.Errorf("outbox: mark failed: %w", err)
	}
	return nil
}

// PendingCount returns the number of unpublished records. It backs a Prometheus
// gauge.
func (s *Store) PendingCount(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var n int64
	err := pool.QueryRow(ctx, `SELECT count(*) FROM outbox WHERE published_at IS NULL`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("outbox: pending count: %w", err)
	}
	return n, nil
}
