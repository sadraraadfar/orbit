package migrations_test

import (
	"io/fs"
	"testing"
	"testing/fstest"

	"github.com/example/orbit/internal/platform/migrations"
)

func TestUnionMergesPlatformAndServiceMigrations(t *testing.T) {
	service := fstest.MapFS{
		"000002_service.up.sql":   &fstest.MapFile{Data: []byte("SELECT 1;")},
		"000002_service.down.sql": &fstest.MapFile{Data: []byte("SELECT 2;")},
	}

	merged := migrations.Union(service)

	entries, err := fs.ReadDir(merged, ".")
	if err != nil {
		t.Fatal(err)
	}
	names := make(map[string]bool, len(entries))
	for _, e := range entries {
		names[e.Name()] = true
	}

	for _, want := range []string{
		"000001_platform.up.sql",
		"000001_platform.down.sql",
		"000002_service.up.sql",
		"000002_service.down.sql",
	} {
		if !names[want] {
			t.Errorf("merged filesystem missing %s (have %v)", want, names)
		}
	}

	data, err := fs.ReadFile(merged, "000002_service.up.sql")
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "SELECT 1;" {
		t.Errorf("service file content = %q", data)
	}
}

func TestServiceUnionRequiresDirectory(t *testing.T) {
	embedded := fstest.MapFS{
		"migrations/000002_service.up.sql": &fstest.MapFile{Data: []byte("SELECT 1;")},
	}
	mig, err := migrations.ServiceUnion(embedded, "migrations")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fs.ReadFile(mig, "000001_platform.up.sql"); err != nil {
		t.Fatalf("platform migration missing: %v", err)
	}
	if _, err := fs.ReadFile(mig, "000002_service.up.sql"); err != nil {
		t.Fatalf("service migration missing: %v", err)
	}
}
