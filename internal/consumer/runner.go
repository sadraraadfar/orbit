package consumer

import (
	"context"
	"log/slog"
	"time"

	"github.com/example/orbit/internal/platform/broker"
	"github.com/example/orbit/internal/platform/metrics"
)

// Runner reads messages from one topic and feeds them to a Processor. Offsets
// are committed only after successful processing (or dead-lettering).
type Runner struct {
	consumer  *broker.Consumer
	processor *Processor
	group     string
	metrics   *metrics.Metrics
	log       *slog.Logger
}

// NewRunner constructs a Runner.
func NewRunner(c *broker.Consumer, p *Processor, group string, m *metrics.Metrics, log *slog.Logger) *Runner {
	return &Runner{consumer: c, processor: p, group: group, metrics: m, log: log}
}

// Run consumes until the context is cancelled.
func (r *Runner) Run(ctx context.Context) error {
	go r.reportLag(ctx)
	r.log.InfoContext(ctx, "consumer started",
		slog.String("topic", r.consumer.Topic()),
		slog.String("group", r.group),
	)

	for {
		msg, err := r.consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			r.log.ErrorContext(ctx, "fetch failed", slog.Any("error", err))
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}

		if err := r.processor.Process(ctx, msg); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// Infrastructure failure: do not commit so the message is redelivered.
			r.log.ErrorContext(ctx, "message processing aborted; offset not committed",
				slog.String("topic", msg.Topic),
				slog.Int64("offset", msg.Offset),
				slog.Any("error", err),
			)
			if !sleep(ctx, time.Second) {
				return nil
			}
			continue
		}

		if err := r.consumer.Commit(ctx, msg); err != nil {
			r.log.ErrorContext(ctx, "offset commit failed",
				slog.String("topic", msg.Topic),
				slog.Int64("offset", msg.Offset),
				slog.Any("error", err),
			)
		}
	}
}

func (r *Runner) reportLag(ctx context.Context) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			lag := r.consumer.Lag()
			if lag >= 0 {
				r.metrics.ConsumerLag.WithLabelValues(r.consumer.Topic(), r.group).Set(float64(lag))
			}
		}
	}
}

func sleep(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
