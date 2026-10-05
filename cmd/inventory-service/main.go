// Command inventory-service runs the Inventory service: it reserves stock in
// response to order.created.v1 and releases it as a compensation for payment or
// fulfillment failures.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/orbit/internal/inventory"
	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/platform/app"
	"github.com/example/orbit/internal/platform/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orbit inventory:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	migFS, err := migrations.ServiceUnion(inventory.Migrations, "migrations")
	if err != nil {
		return err
	}
	a, err := app.New(ctx, "inventory", app.Options{Migrations: migFS, MigrationsRoot: ".", RunMigrations: true})
	if err != nil {
		return err
	}
	if err := a.EnsureTopics(ctx); err != nil {
		a.Log.Warn("could not ensure topics: " + err.Error())
	}

	store := outbox.NewStore(a.Cfg.KafkaTopicPrefix)
	repo := inventory.NewRepository(store)
	handler := inventory.NewHandler(repo, a.Log)

	for _, family := range []string{"order", "payment", "fulfillment"} {
		a.Consume(family, handler)
	}
	a.PublishOutbox(store)

	return a.Run(ctx)
}
