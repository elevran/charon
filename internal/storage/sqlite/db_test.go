package sqlite

import (
	"testing"
)

func TestOpenCreatesSchema(t *testing.T) {
	db, err := openDB(":memory:", Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer Close(db)

	// Verify both tables were created.
	for _, table := range []string{"responses", "write_intents"} {
		var count int
		err := db.QueryRow(
			"SELECT count(*) FROM sqlite_master WHERE type='table' AND name=?", table,
		).Scan(&count)
		if err != nil {
			t.Fatalf("query %s: %v", table, err)
		}
		if count != 1 {
			t.Errorf("table %q not found in schema", table)
		}
	}
}

func TestOpenIdempotent(t *testing.T) {
	// Running Open twice on the same path must not fail.
	dir := t.TempDir()
	path := dir + "/test.db"

	db1, err := openDB(path, Config{WALMode: true})
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	Close(db1)

	db2, err := openDB(path, Config{WALMode: true})
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	Close(db2)
}
