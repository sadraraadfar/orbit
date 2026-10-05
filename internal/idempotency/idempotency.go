// Package idempotency implements processed-message tracking for at-least-once
// consumers.
//
// A consumer records the (consumer, event_id) pair in the same transaction as
// its state change. The primary key on that pair makes redelivery a no-op: the
// insert conflicts, the transaction is left untouched, and the message is
// acknowledged.
package idempotency

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
)

// Store records processed messages.
type Store struct{}

// New returns a Store.
func New() *Store { return &Store{} }

// MarkIfNew inserts a processed marker for (consumer, eventID) inside tx. It
// returns true when the marker was newly inserted and false when the event was
// already processed. A false result means the handler must be skipped.
func (s *Store) MarkIfNew(ctx context.Context, tx pgx.Tx, consumer, eventID, topic string) (bool, error) {
	tag, err := tx.Exec(ctx, `
		INSERT INTO processed_messages (consumer, event_id, topic)
		VALUES ($1, $2, $3)
		ON CONFLICT (consumer, event_id) DO NOTHING`,
		consumer, eventID, topic,
	)
	if err != nil {
		return false, fmt.Errorf("idempotency: insert marker: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}
