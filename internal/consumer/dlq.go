package consumer

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/platform/metrics"
)

// Dead-letter headers. They make a dead-letter record self-describing so the
// orbitctl replay command can restore it to its source topic.
const (
	HeaderOriginalTopic = "x-orbit-original-topic"
	HeaderOriginalKey   = "x-orbit-original-key"
	HeaderFailureClass  = "x-orbit-failure-class"
	HeaderFailureReason = "x-orbit-failure-reason"
	HeaderConsumer      = "x-orbit-consumer"
)

// PublisherClient is the broker subset required to write dead letters.
type PublisherClient interface {
	Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error
}

// DLQ routes poison messages to a per-source dead-letter topic.
type DLQ struct {
	client  PublisherClient
	metrics *metrics.Metrics
	log     *slog.Logger
}

// NewDLQ constructs a DLQ publisher.
func NewDLQ(client PublisherClient, m *metrics.Metrics, log *slog.Logger) *DLQ {
	return &DLQ{client: client, metrics: m, log: log}
}

// Publish writes the original, undecoded message to <source>.dlq. The payload is
// preserved byte-for-byte so a replay is exact.
func (d *DLQ) Publish(ctx context.Context, sourceTopic, key string, value []byte, class Class, reason, consumerName, eventType string) error {
	if reason == "" {
		reason = "unspecified"
	}
	topic := events.DLQTopic(sourceTopic)
	headers := map[string]string{
		HeaderOriginalTopic: sourceTopic,
		HeaderOriginalKey:   key,
		HeaderFailureClass:  string(class),
		HeaderFailureReason: reason,
		HeaderConsumer:      consumerName,
	}
	if eventType != "" {
		headers["event_type"] = eventType
	}

	if err := d.client.Publish(ctx, topic, key, value, headers); err != nil {
		return fmt.Errorf("consumer: publish to DLQ %s: %w", topic, err)
	}
	d.metrics.DLQPublished.WithLabelValues(eventType).Inc()
	d.log.WarnContext(ctx, "event routed to dead-letter topic",
		slog.String("dlq_topic", topic),
		slog.String("original_topic", sourceTopic),
		slog.String("consumer", consumerName),
		slog.String("class", string(class)),
		slog.String("reason", reason),
	)
	return nil
}
