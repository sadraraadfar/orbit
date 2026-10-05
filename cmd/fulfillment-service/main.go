// Command fulfillment-service runs the Fulfillment service: it starts and
// completes (or fails) fulfillment after a payment is authorized.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/orbit/internal/fulfillment"
	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/platform/app"
	"github.com/example/orbit/internal/platform/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orbit fulfillment:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	migFS, err := migrations.ServiceUnion(fulfillment.Migrations, "migrations")
	if err != nil {
		return err
	}
	a, err := app.New(ctx, "fulfillment", app.Options{Migrations: migFS, MigrationsRoot: ".", RunMigrations: true})
	if err != nil {
		return err
	}
	if err := a.EnsureTopics(ctx); err != nil {
		a.Log.Warn("could not ensure topics: " + err.Error())
	}

	store := outbox.NewStore(a.Cfg.KafkaTopicPrefix)
	repo := fulfillment.NewRepository(store)
	handler := fulfillment.NewHandler(repo, a.Log)

	a.Consume("payment", handler)
	a.PublishOutbox(store)

	return a.Run(ctx)
}
