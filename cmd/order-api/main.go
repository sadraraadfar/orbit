// Command order-api runs the Order service: the HTTP API, the outbox publisher,
// and the choreography consumers that advance the order aggregate.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/orbit/internal/order"
	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/platform/app"
	"github.com/example/orbit/internal/platform/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orbit order-api:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	migFS, err := migrations.ServiceUnion(order.Migrations, "migrations")
	if err != nil {
		return err
	}
	a, err := app.New(ctx, "order", app.Options{Migrations: migFS, MigrationsRoot: ".", RunMigrations: true})
	if err != nil {
		return err
	}

	if err := a.EnsureTopics(ctx); err != nil {
		a.Log.Warn("could not ensure topics: " + err.Error())
	}

	store := outbox.NewStore(a.Cfg.KafkaTopicPrefix)
	repo := order.NewRepository(store)
	svc := order.NewService(a.Pool, repo, a.Cfg.KafkaTopicPrefix, a.Cfg.EnableFaultInjection, a.Log)
	order.NewAPI(svc, a.Log).Register(a.Mux)

	handler := order.NewEventHandler(svc)
	for _, family := range []string{"inventory", "payment", "fulfillment"} {
		a.Consume(family, handler)
	}
	a.PublishOutbox(store)

	return a.Run(ctx)
}
