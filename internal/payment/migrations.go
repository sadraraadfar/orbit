package payment

import "embed"

// Migrations holds the payment service's SQL migrations.
//
//go:embed migrations/*.sql
var Migrations embed.FS
