// Command payment-service runs the Payment service: it authorizes payments
// after inventory is reserved and refunds them when fulfillment fails.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/payment"
	"github.com/example/orbit/internal/platform/app"
	"github.com/example/orbit/internal/platform/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orbit payment:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	migFS, err := migrations.ServiceUnion(payment.Migrations, "migrations")
	if err != nil {
		return err
	}
	a, err := app.New(ctx, "payment", app.Options{Migrations: migFS, MigrationsRoot: ".", RunMigrations: true})
	if err != nil {
		return err
	}
	if err := a.EnsureTopics(ctx); err != nil {
		a.Log.Warn("could not ensure topics: " + err.Error())
	}

	store := outbox.NewStore(a.Cfg.KafkaTopicPrefix)
	repo := payment.NewRepository(store)
	handler := payment.NewHandler(repo, a.Log)

	for _, family := range []string{"inventory", "fulfillment"} {
		a.Consume(family, handler)
	}
	a.PublishOutbox(store)

	return a.Run(ctx)
}
