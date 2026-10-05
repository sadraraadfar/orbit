// Package broker wraps the Kafka client used by Orbit services.
//
// It deliberately keeps the surface small: a synchronous producer that records
// trace context and event metadata in headers, and a consumer with manual
// offset commits so that processing and offset advancement are explicit.
package broker

import (
	"context"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
)

const (
	headerEventType     = "event_type"
	headerEventVersion  = "event_version"
	headerCorrelationID = "correlation_id"
	headerContentType   = "content_type"
)

// Producer publishes envelopes synchronously. A successful Publish means the
// broker acknowledged the record; it does not imply exactly-once delivery.
type Producer struct {
	writer  *kafka.Writer
	brokers []string
}

// NewProducer creates a producer that writes with acks=all.
func NewProducer(brokers []string) *Producer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Balancer:     &kafka.Hash{}, // partition by aggregate key
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchTimeout: 10 * time.Millisecond,
		MaxAttempts:  5,
	}
	return &Producer{writer: w, brokers: brokers}
}

// Publish sends value to topic keyed by key, attaching event metadata and the
// W3C trace context so consumers can continue the trace.
func (p *Producer) Publish(ctx context.Context, topic, key string, value []byte, headers map[string]string) error {
	carrier := mapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)

	kafkaHeaders := make([]kafka.Header, 0, len(headers)+len(carrier))
	for k, v := range headers {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
	}
	for k, v := range carrier {
		kafkaHeaders = append(kafkaHeaders, kafka.Header{Key: k, Value: []byte(v)})
	}

	msg := kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   value,
		Headers: kafkaHeaders,
		Time:    time.Now().UTC(),
	}
	if err := p.writer.WriteMessages(ctx, msg); err != nil {
		return fmt.Errorf("broker: publish to %s: %w", topic, err)
	}
	return nil
}

// Close flushes and releases the underlying writer.
func (p *Producer) Close() error { return p.writer.Close() }

// Ping verifies that at least one broker is reachable.
func Ping(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("broker: no brokers configured")
	}
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("broker: dial %s: %w", brokers[0], err)
	}
	return conn.Close()
}

// mapCarrier adapts a string map to propagation.TextMapCarrier.
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

// Message is the broker-neutral view of a consumed record.
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       string
	Value     []byte
	Headers   map[string]string
}

// Consumer reads from a single topic using a consumer group. Offsets are
// committed manually by the caller after successful processing.
type Consumer struct {
	reader *kafka.Reader
	topic  string
}

// NewConsumer creates a group consumer for topic.
func NewConsumer(brokers []string, group, topic string, maxBytes int) *Consumer {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:         brokers,
		GroupID:         group,
		Topic:           topic,
		MinBytes:        1,
		MaxBytes:        maxBytes,
		MaxWait:         250 * time.Millisecond,
		StartOffset:     kafka.FirstOffset,
		CommitInterval:  0, // manual commits only
		ReadLagInterval: 5 * time.Second,
	})
	return &Consumer{reader: r, topic: topic}
}

// Lag returns the most recently observed consumer lag for the group.
func (c *Consumer) Lag() int64 { return c.reader.Stats().Lag }

// Fetch returns the next message, blocking until one is available or the
// context is cancelled.
func (c *Consumer) Fetch(ctx context.Context) (Message, error) {
	m, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Message{}, err
	}
	headers := make(map[string]string, len(m.Headers))
	for _, h := range m.Headers {
		headers[h.Key] = string(h.Value)
	}
	return Message{
		Topic:     m.Topic,
		Partition: m.Partition,
		Offset:    m.Offset,
		Key:       string(m.Key),
		Value:     m.Value,
		Headers:   headers,
	}, nil
}

// Commit advances the group offset for the supplied message.
func (c *Consumer) Commit(ctx context.Context, msg Message) error {
	return c.reader.CommitMessages(ctx, kafka.Message{
		Topic:     msg.Topic,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	})
}

// Topic returns the subscribed topic.
func (c *Consumer) Topic() string { return c.topic }

// Close releases the reader.
func (c *Consumer) Close() error { return c.reader.Close() }

// EnsureTopics creates the supplied topics when they do not yet exist. It is a
// development convenience; production topic management belongs to the platform.
func EnsureTopics(ctx context.Context, brokers []string, topics []string, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("broker: dial: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("broker: controller: %w", err)
	}
	ctrlConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("broker: dial controller: %w", err)
	}
	defer ctrlConn.Close()

	existingPartitions, err := ctrlConn.ReadPartitions()
	if err != nil {
		return fmt.Errorf("broker: read partitions: %w", err)
	}
	existing := make(map[string]struct{}, len(existingPartitions))
	for _, p := range existingPartitions {
		existing[p.Topic] = struct{}{}
	}

	configs := make([]kafka.TopicConfig, 0, len(topics))
	for _, t := range topics {
		if _, ok := existing[t]; ok {
			continue
		}
		configs = append(configs, kafka.TopicConfig{
			Topic:             t,
			NumPartitions:     partitions,
			ReplicationFactor: 1,
		})
	}
	if len(configs) == 0 {
		return nil
	}
	if err := ctrlConn.CreateTopics(configs...); err != nil {
		return fmt.Errorf("broker: create topics: %w", err)
	}
	return nil
}

// EventHeaders builds the standard header set for an outbox record.
func EventHeaders(eventType string, version int, correlationID string) map[string]string {
	return map[string]string{
		headerEventType:     eventType,
		headerEventVersion:  fmt.Sprintf("%d", version),
		headerCorrelationID: correlationID,
		headerContentType:   "application/json",
	}
}
