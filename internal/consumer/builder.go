package consumer

import (
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/example/orbit/internal/idempotency"
	"github.com/example/orbit/internal/platform/broker"
	"github.com/example/orbit/internal/platform/metrics"
)

// RunnerConfig describes one consumer subscription.
type RunnerConfig struct {
	Service    string
	Group      string
	Topic      string
	Brokers    []string
	MaxBytes   int
	MaxRetries int
	Backoff    time.Duration
}

// BuildRunner wires a broker consumer, a reliability processor, and a runner.
// publisher must implement PublisherClient (the broker Producer does).
func BuildRunner(cfg RunnerConfig, handler Handler, pool *pgxpool.Pool, publisher PublisherClient, m *metrics.Metrics, log *slog.Logger) *Runner {
	if cfg.MaxBytes == 0 {
		cfg.MaxBytes = 10 << 20
	}
	dlq := NewDLQ(publisher, m, log)
	processor := NewProcessor(ProcessorConfig{
		Service:      cfg.Service,
		ConsumerName: cfg.Group,
		MaxRetries:   cfg.MaxRetries,
		Backoff:      cfg.Backoff,
	}, handler, pool, idempotency.New(), dlq, m, log)

	c := broker.NewConsumer(cfg.Brokers, cfg.Group, cfg.Topic, cfg.MaxBytes)
	return NewRunner(c, processor, cfg.Group, m, log)
}
