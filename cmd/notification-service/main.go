// Command notification-service runs the Notification service: it sends the
// customer notification scheduled when an order completes.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/example/orbit/internal/notification"
	"github.com/example/orbit/internal/outbox"
	"github.com/example/orbit/internal/platform/app"
	"github.com/example/orbit/internal/platform/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "orbit notification:", err)
		os.Exit(1)
	}
}

func run() error {
	ctx := context.Background()

	migFS, err := migrations.ServiceUnion(notification.Migrations, "migrations")
	if err != nil {
		return err
	}
	a, err := app.New(ctx, "notification", app.Options{Migrations: migFS, MigrationsRoot: ".", RunMigrations: true})
	if err != nil {
		return err
	}
	if err := a.EnsureTopics(ctx); err != nil {
		a.Log.Warn("could not ensure topics: " + err.Error())
	}

	store := outbox.NewStore(a.Cfg.KafkaTopicPrefix)
	repo := notification.NewRepository(store)
	handler := notification.NewHandler(repo, a.Log)

	a.Consume("notification", handler)
	a.PublishOutbox(store)

	return a.Run(ctx)
}
