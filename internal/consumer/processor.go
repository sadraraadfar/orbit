package consumer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/idempotency"
	"github.com/example/orbit/internal/platform/broker"
	"github.com/example/orbit/internal/platform/correlation"
	"github.com/example/orbit/internal/platform/metrics"
	"github.com/example/orbit/internal/platform/postgres"
)

// Handler processes a single event inside a database transaction. The
// transaction already contains the processed-message marker, so any state the
// handler writes is committed atomically with the marker.
type Handler interface {
	Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, tx pgx.Tx, env events.Envelope) error

// Handle implements Handler.
func (f HandlerFunc) Handle(ctx context.Context, tx pgx.Tx, env events.Envelope) error {
	return f(ctx, tx, env)
}

// Processor applies idempotency, tracing, retry, and dead-lettering to handler
// invocations.
type Processor struct {
	service      string
	consumerName string
	handler      Handler
	pool         *pgxpool.Pool
	idem         *idempotency.Store
	dlq          *DLQ
	log          *slog.Logger
	metrics      *metrics.Metrics
	maxRetries   int
	backoff      time.Duration
	tracer       trace.Tracer
}

// ProcessorConfig configures a Processor.
type ProcessorConfig struct {
	Service      string
	ConsumerName string
	MaxRetries   int
	Backoff      time.Duration
}

// NewProcessor constructs a Processor.
func NewProcessor(cfg ProcessorConfig, handler Handler, pool *pgxpool.Pool, idem *idempotency.Store, dlq *DLQ, m *metrics.Metrics, log *slog.Logger) *Processor {
	return &Processor{
		service:      cfg.Service,
		consumerName: cfg.ConsumerName,
		handler:      handler,
		pool:         pool,
		idem:         idem,
		dlq:          dlq,
		log:          log,
		metrics:      m,
		maxRetries:   cfg.MaxRetries,
		backoff:      cfg.Backoff,
		tracer:       otel.Tracer("orbit/consumer"),
	}
}

type outcome struct {
	duplicate bool
	ignored   bool
}

// Process decodes and handles a message. It returns a non-nil error only when
// the message must be redelivered because an infrastructure dependency failed;
// poison messages are dead-lettered and acknowledged.
func (p *Processor) Process(ctx context.Context, msg broker.Message) error {
	env, err := events.Decode(msg.Value)
	if err != nil {
		p.metrics.EventsFailed.WithLabelValues("unknown", string(ClassInvalid)).Inc()
		return p.dlq.Publish(ctx, msg.Topic, msg.Key, msg.Value, ClassInvalid, err.Error(), p.consumerName, "unknown")
	}

	ctx = extractTrace(ctx, msg.Headers)
	ctx, span := p.tracer.Start(ctx, "consume "+env.EventType,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination", msg.Topic),
			attribute.String("orbit.event_type", env.EventType),
			attribute.String("orbit.aggregate_id", env.AggregateID),
			attribute.String("orbit.consumer", p.consumerName),
		),
	)
	defer span.End()
	ctx = correlation.With(ctx, env.CorrelationID)
	span.SetAttributes(attribute.String("orbit.correlation_id", env.CorrelationID))

	var lastErr error
	for attempt := 0; attempt <= p.maxRetries; attempt++ {
		out, handleErr := p.handleOnce(ctx, env, msg.Topic)
		if handleErr == nil {
			result := "ok"
			switch {
			case out.duplicate:
				result = "duplicate"
			case out.ignored:
				result = "ignored"
			}
			p.metrics.EventsProcessed.WithLabelValues(env.EventType, result).Inc()
			span.SetStatus(codes.Ok, "")
			return nil
		}

		lastErr = handleErr
		if Classify(handleErr) != ClassTransient || attempt == p.maxRetries {
			break
		}

		p.metrics.EventRetries.WithLabelValues(env.EventType).Inc()
		wait := p.backoff * time.Duration(1<<attempt)
		p.log.WarnContext(ctx, "transient handler failure; retrying",
			slog.String("consumer", p.consumerName),
			slog.String("event_type", env.EventType),
			slog.String("event_id", env.EventID),
			slog.Int("attempt", attempt+1),
			slog.Duration("retry_in", wait),
			slog.Any("error", handleErr),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(wait):
		}
	}

	class := Classify(lastErr)
	p.metrics.EventsFailed.WithLabelValues(env.EventType, string(class)).Inc()
	span.RecordError(lastErr)
	span.SetStatus(codes.Error, lastErr.Error())
	p.log.ErrorContext(ctx, "event handling failed; dead-lettering",
		slog.String("consumer", p.consumerName),
		slog.String("event_type", env.EventType),
		slog.String("event_id", env.EventID),
		slog.String("class", string(class)),
		slog.Any("error", lastErr),
	)
	return p.dlq.Publish(ctx, msg.Topic, msg.Key, msg.Value, class, lastErr.Error(), p.consumerName, env.EventType)
}

func (p *Processor) handleOnce(ctx context.Context, env events.Envelope, sourceTopic string) (outcome, error) {
	var out outcome
	err := postgres.WithTx(ctx, p.pool, func(tx pgx.Tx) error {
		fresh, err := p.idem.MarkIfNew(ctx, tx, p.consumerName, env.EventID, sourceTopic)
		if err != nil {
			return Transient(err)
		}
		if !fresh {
			out.duplicate = true
			return nil
		}
		if err := p.handler.Handle(ctx, tx, env); err != nil {
			if errors.Is(err, ErrIgnore) {
				out.ignored = true
				return nil // commit the marker so redelivery stays a no-op
			}
			return err
		}
		return nil
	})
	return out, err
}

// mapCarrier adapts Kafka headers to the OpenTelemetry text map carrier.
type mapCarrier map[string]string

func (m mapCarrier) Get(key string) string { return m[key] }
func (m mapCarrier) Set(key, value string) { m[key] = value }
func (m mapCarrier) Keys() []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func extractTrace(ctx context.Context, headers map[string]string) context.Context {
	if len(headers) == 0 {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, mapCarrier(headers))
}

var _ propagation.TextMapCarrier = mapCarrier{}
