package app

import (
	"context"

	"github.com/example/orbit/internal/consumer"
	"github.com/example/orbit/internal/events"
	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/platform/broker"
)

// EnsureTopics creates the Orbit topics and dead-letter topics when missing.
// It is best-effort: brokers such as Redpanda may also auto-create topics.
func (a *App) EnsureTopics(ctx context.Context) error {
	return broker.EnsureTopics(ctx, a.Cfg.KafkaBrokers, events.AllTopics(a.Cfg.KafkaTopicPrefix), 3)
}

// Consume subscribes a handler to the event family (for example "inventory")
// and registers the runner as a background task. The consumer group is scoped
// to the service so multiple services can subscribe to the same topic.
func (a *App) Consume(family string, handler consumer.Handler) {
	group := a.Cfg.ServiceName + "." + family
	runner := consumer.BuildRunner(consumer.RunnerConfig{
		Service:    a.Cfg.ServiceName,
		Group:      group,
		Topic:      events.StreamTopic(a.Cfg.KafkaTopicPrefix, family),
		Brokers:    a.Cfg.KafkaBrokers,
		MaxRetries: a.Cfg.ConsumerMaxRetries,
		Backoff:    a.Cfg.ConsumerRetryBackoff,
	}, handler, a.Pool, a.Producer, a.Metrics, a.Log)
	a.AddTask("consumer."+family, runner.Run)
}

// PublishOutbox drains store into the broker as a background task.
func (a *App) PublishOutbox(store *outbox.Store) {
	publisher := outbox.NewPublisher(store, a.Pool, a.Producer, a.Log, a.Metrics, outbox.PublisherConfig{
		PollInterval: a.Cfg.OutboxPollInterval,
		BatchSize:    a.Cfg.OutboxBatchSize,
		MaxAttempts:  a.Cfg.OutboxMaxAttempts,
	})
	a.AddTask("outbox-publisher", publisher.Run)
}
