package order

import "embed"

// Migrations holds the order service's SQL migrations.
//
//go:embed migrations/*.sql
var Migrations embed.FS
