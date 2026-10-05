package config_test

import (
	"testing"

	"github.com/example/orbit/internal/platform/config"
)

func TestLoadRequiresDatabaseURL(t *testing.T) {
	t.Setenv("ORBIT_DATABASE_URL", "")
	if _, err := config.Load("order"); err == nil {
		t.Fatal("expected error when ORBIT_DATABASE_URL is missing")
	}
}

func TestLoadAppliesDefaults(t *testing.T) {
	t.Setenv("ORBIT_DATABASE_URL", "postgres://localhost/test")
	t.Setenv("ORBIT_KAFKA_BROKERS", "a:9092, b:9092 ")

	cfg, err := config.Load("order")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ServiceName != "order" {
		t.Errorf("service name = %q", cfg.ServiceName)
	}
	if len(cfg.KafkaBrokers) != 2 || cfg.KafkaBrokers[0] != "a:9092" || cfg.KafkaBrokers[1] != "b:9092" {
		t.Errorf("brokers = %v", cfg.KafkaBrokers)
	}
	if cfg.OutboxBatchSize <= 0 {
		t.Errorf("outbox batch size = %d", cfg.OutboxBatchSize)
	}
	if cfg.ConsumerMaxRetries < 0 {
		t.Errorf("consumer max retries = %d", cfg.ConsumerMaxRetries)
	}
}

func TestLoadRejectsInvalidBatchSize(t *testing.T) {
	t.Setenv("ORBIT_DATABASE_URL", "postgres://localhost/test")
	t.Setenv("ORBIT_OUTBOX_BATCH_SIZE", "0")
	if _, err := config.Load("order"); err == nil {
		t.Fatal("expected error for zero batch size")
	}
}
