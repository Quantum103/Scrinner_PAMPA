package migrations

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadSortsAndPairsMigrations(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"000002_add_index.up.sql":      "CREATE INDEX test_idx ON test_table(id);",
		"000002_add_index.down.sql":    "DROP INDEX test_idx;",
		"000001_create_table.up.sql":   "CREATE TABLE test_table(id BIGINT);",
		"000001_create_table.down.sql": "DROP TABLE test_table;",
		"README.md":                    "ignored",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	items, err := load(dir)
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if len(items) != 2 || items[0].version != 1 || items[1].version != 2 {
		t.Fatalf("unexpected migration ordering: %#v", items)
	}
}

func TestLoadRejectsMissingDownMigration(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "000001_initial.up.sql"), []byte("SELECT 1;"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := load(dir); err == nil {
		t.Fatal("load() expected an error for an incomplete migration")
	}
}

func TestLoadAllowsEmptyDirectory(t *testing.T) {
	items, err := load(t.TempDir())
	if err != nil {
		t.Fatalf("load() error = %v", err)
	}
	if len(items) != 0 {
		t.Fatalf("migration count = %d, want 0", len(items))
	}
}
