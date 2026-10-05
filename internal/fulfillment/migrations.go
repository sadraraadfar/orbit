package fulfillment

import "embed"

// Migrations holds the fulfillment service's SQL migrations.
//
//go:embed migrations/*.sql
var Migrations embed.FS
