// Package migrate runs embedded SQL migrations using golang-migrate.
//
// Migrations are embedded in each service binary so that a single command can
// bring a fresh database up to the expected schema. The golang-migrate pgx/v5
// driver is used so no external CLI is required.
package migrate

import (
	"errors"
	"fmt"
	"io/fs"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
)

// Up applies all outstanding migrations from root within fsys.
func Up(fsys fs.FS, root, databaseURL string) error {
	m, err := newMigrator(fsys, root, databaseURL)
	if err != nil {
		return err
	}
	defer closeMigrator(m)

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: up: %w", err)
	}
	return nil
}

// Down rolls back all applied migrations. It is primarily used by tests.
func Down(fsys fs.FS, root, databaseURL string) error {
	m, err := newMigrator(fsys, root, databaseURL)
	if err != nil {
		return err
	}
	defer closeMigrator(m)

	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("migrate: down: %w", err)
	}
	return nil
}

func newMigrator(fsys fs.FS, root, databaseURL string) (*migrate.Migrate, error) {
	src, err := iofs.New(fsys, root)
	if err != nil {
		return nil, fmt.Errorf("migrate: source: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, pgx5URL(databaseURL))
	if err != nil {
		return nil, fmt.Errorf("migrate: instance: %w", err)
	}
	return m, nil
}

func closeMigrator(m *migrate.Migrate) {
	srcErr, dbErr := m.Close()
	_ = srcErr
	_ = dbErr
}

// pgx5URL rewrites a standard postgres URL so golang-migrate selects the
// pgx/v5 driver instead of the default lib/pq driver.
func pgx5URL(databaseURL string) string {
	switch {
	case strings.HasPrefix(databaseURL, "postgres://"):
		return "pgx5://" + strings.TrimPrefix(databaseURL, "postgres://")
	case strings.HasPrefix(databaseURL, "postgresql://"):
		return "pgx5://" + strings.TrimPrefix(databaseURL, "postgresql://")
	default:
		return databaseURL
	}
}
