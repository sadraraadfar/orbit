package notification

import "embed"

// Migrations holds the notification service's SQL migrations.
//
//go:embed migrations/*.sql
var Migrations embed.FS
