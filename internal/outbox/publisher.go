package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orbit/internal/platform/metrics"
)

// PublisherClient is the subset of the broker producer used by the publisher.
type PublisherClient interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// Publisher drains the outbox into the broker. It is safe to run multiple
// instances concurrently because Claim uses FOR UPDATE SKIP LOCKED.
type Publisher struct {
	store        *Store
	pool         *pgxpool.Pool
	client       PublisherClient
	log          *slog.Logger
	metrics      *metrics.Metrics
	pollInterval time.Duration
	batchSize    int
	maxAttempts  int
}

// PublisherConfig configures a Publisher.
type PublisherConfig struct {
	PollInterval time.Duration
	BatchSize    int
	MaxAttempts  int
}

// NewPublisher constructs a Publisher.
func NewPublisher(store *Store, pool *pgxpool.Pool, client PublisherClient, log *slog.Logger, m *metrics.Metrics, cfg PublisherConfig) *Publisher {
	return &Publisher{
		store:        store,
		pool:         pool,
		client:       client,
		log:          log,
		metrics:      m,
		pollInterval: cfg.PollInterval,
		batchSize:    cfg.BatchSize,
		maxAttempts:  cfg.MaxAttempts,
	}
}

// Run polls until the context is cancelled.
func (p *Publisher) Run(ctx context.Context) error {
	ticker := time.NewTicker(p.pollInterval)
	defer ticker.Stop()

	for {
		if err := p.DrainOnce(ctx); err != nil && ctx.Err() == nil {
			p.log.ErrorContext(ctx, "outbox drain failed", slog.Any("error", err))
		}
		p.observePending(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

// DrainOnce claims and publishes a single batch.
func (p *Publisher) DrainOnce(ctx context.Context) error {
	lease := p.pollInterval * 10
	if lease < 30*time.Second {
		lease = 30 * time.Second
	}
	records, err := p.store.Claim(ctx, p.pool, p.batchSize, lease)
	if err != nil {
		return err
	}
	for _, rec := range records {
		if err := p.publish(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

func (p *Publisher) publish(ctx context.Context, rec Record) error {
	err := p.client.Publish(ctx, rec.Topic, rec.AggregateID, rec.Payload, rec.Headers)
	if err == nil {
		if markErr := p.store.MarkPublished(ctx, p.pool, rec.ID); markErr != nil {
			// The broker already has the record; the mark failure means it will
			// be republished later. This is the documented at-least-once window.
			p.log.WarnContext(ctx, "outbox published but mark failed; duplicate publication expected",
				slog.String("outbox_id", rec.ID),
				slog.Any("error", markErr),
			)
			return markErr
		}
		p.metrics.OutboxPublished.Inc()
		p.log.DebugContext(ctx, "outbox record published",
			slog.String("outbox_id", rec.ID),
			slog.String("event_type", rec.EventType),
			slog.String("topic", rec.Topic),
		)
		return nil
	}

	p.metrics.OutboxFailures.Inc()
	backoff := p.backoff(rec.Attempts)
	p.log.ErrorContext(ctx, "outbox publish failed",
		slog.String("outbox_id", rec.ID),
		slog.String("event_type", rec.EventType),
		slog.Int("attempts", rec.Attempts),
		slog.Duration("retry_in", backoff),
		slog.Any("error", err),
	)
	if markErr := p.store.MarkFailed(ctx, p.pool, rec.ID, err.Error(), backoff); markErr != nil {
		return markErr
	}
	return nil
}

// backoff returns exponential backoff capped at one minute.
func (p *Publisher) backoff(attempts int) time.Duration {
	base := 250 * time.Millisecond
	shift := math.Min(float64(attempts), 8)
	d := time.Duration(float64(base) * math.Pow(2, shift))
	if d > time.Minute {
		return time.Minute
	}
	return d
}

func (p *Publisher) observePending(ctx context.Context) {
	n, err := p.store.PendingCount(ctx, p.pool)
	if err != nil {
		return
	}
	p.metrics.OutboxPending.Set(float64(n))
}

// Info reports the publisher configuration for startup logs.
func (p *Publisher) Info() string {
	return fmt.Sprintf("outbox publisher batch=%d poll=%s max_attempts=%d", p.batchSize, p.pollInterval, p.maxAttempts)
}
