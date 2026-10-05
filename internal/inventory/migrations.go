package inventory

import "embed"

// Migrations holds the inventory service's SQL migrations.
//
//go:embed migrations/*.sql
var Migrations embed.FS
