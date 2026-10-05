// Package config loads runtime configuration from the environment.
//
// Every service is configured exclusively through environment variables so the
// same binary can run locally, in CI, and in containers without code changes.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config holds the configuration shared by all Orbit services. Service
// specific settings are intentionally kept minimal; most behaviour is
// controlled by the domain packages.
type Config struct {
	ServiceName          string
	HTTPAddr             string
	DatabaseURL          string
	KafkaBrokers         []string
	KafkaTopicPrefix     string
	LogLevel             string
	LogFormat            string
	OTLPEndpoint         string
	Environment          string
	EnableFaultInjection bool

	OutboxPollInterval time.Duration
	OutboxBatchSize    int
	OutboxMaxAttempts  int

	ConsumerMaxRetries   int
	ConsumerRetryBackoff time.Duration
	ConsumerGroupPrefix  string
}

// Load reads configuration for the named service. The service name is used as
// the default value of ORBIT_SERVICE_NAME and must be non-empty.
func Load(service string) (Config, error) {
	if service == "" {
		return Config{}, fmt.Errorf("config: service name is required")
	}

	cfg := Config{
		ServiceName:          env("ORBIT_SERVICE_NAME", service),
		HTTPAddr:             env("ORBIT_HTTP_ADDR", ":8080"),
		DatabaseURL:          env("ORBIT_DATABASE_URL", ""),
		KafkaBrokers:         splitList(env("ORBIT_KAFKA_BROKERS", "localhost:9092")),
		KafkaTopicPrefix:     env("ORBIT_KAFKA_TOPIC_PREFIX", "orbit"),
		LogLevel:             env("ORBIT_LOG_LEVEL", "info"),
		LogFormat:            env("ORBIT_LOG_FORMAT", "json"),
		OTLPEndpoint:         env("ORBIT_OTLP_ENDPOINT", ""),
		Environment:          env("ORBIT_ENV", "development"),
		EnableFaultInjection: envBool("ORBIT_ENABLE_FAULT_INJECTION", false),
		OutboxPollInterval:   envDuration("ORBIT_OUTBOX_POLL_INTERVAL", 500*time.Millisecond),
		OutboxBatchSize:      envInt("ORBIT_OUTBOX_BATCH_SIZE", 100),
		OutboxMaxAttempts:    envInt("ORBIT_OUTBOX_MAX_ATTEMPTS", 12),
		ConsumerMaxRetries:   envInt("ORBIT_CONSUMER_MAX_RETRIES", 5),
		ConsumerRetryBackoff: envDuration("ORBIT_CONSUMER_RETRY_BACKOFF", 250*time.Millisecond),
		ConsumerGroupPrefix:  env("ORBIT_CONSUMER_GROUP_PREFIX", "orbit"),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("config: ORBIT_DATABASE_URL is required")
	}
	if len(cfg.KafkaBrokers) == 0 {
		return Config{}, fmt.Errorf("config: at least one Kafka broker is required")
	}
	if cfg.OutboxBatchSize <= 0 {
		return Config{}, fmt.Errorf("config: ORBIT_OUTBOX_BATCH_SIZE must be positive")
	}
	if cfg.ConsumerMaxRetries < 0 {
		return Config{}, fmt.Errorf("config: ORBIT_CONSUMER_MAX_RETRIES must not be negative")
	}

	return cfg, nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && strings.TrimSpace(v) != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return n
}

func envBool(key string, fallback bool) bool {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return b
}

func envDuration(key string, fallback time.Duration) time.Duration {
	v, ok := os.LookupEnv(key)
	if !ok || strings.TrimSpace(v) == "" {
		return fallback
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return fallback
	}
	return d
}

func splitList(v string) []string {
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if s := strings.TrimSpace(p); s != "" {
			out = append(out, s)
		}
	}
	return out
}
