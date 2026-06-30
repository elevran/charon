package sqlite

import (
	"fmt"

	"github.com/jmoiron/sqlx"
	_ "modernc.org/sqlite"
)

// Config holds SQLite-specific tuning knobs.
type Config struct {
	WALMode       bool
	BusyTimeoutMs int // default 5000
}

// openDB opens (or creates) a SQLite database at path, applies pragmas, and
// ensures all tables exist. Returns a *sqlx.DB ready for use.
func openDB(path string, cfg Config) (*sqlx.DB, error) {
	db, err := sqlx.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite open: %w", err)
	}
	// SQLite single-writer: one connection prevents SQLITE_BUSY under concurrent writes.
	db.SetMaxOpenConns(1)

	if cfg.BusyTimeoutMs <= 0 {
		cfg.BusyTimeoutMs = 5000
	}
	journalMode := "WAL"
	if !cfg.WALMode {
		journalMode = "DELETE"
	}
	pragmas := []string{
		fmt.Sprintf("PRAGMA busy_timeout = %d", cfg.BusyTimeoutMs),
		fmt.Sprintf("PRAGMA journal_mode = %s", journalMode),
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	}
	for _, p := range pragmas {
		if _, err := db.Exec(p); err != nil {
			_ = db.Close()
			return nil, fmt.Errorf("sqlite pragma %q: %w", p, err)
		}
	}
	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite migrate: %w", err)
	}
	return db, nil
}

// Close closes the underlying database.
func Close(db *sqlx.DB) error {
	return db.Close()
}

func migrate(db *sqlx.DB) error {
	if _, err := db.Exec(schema); err != nil {
		return err
	}
	// Additive migration: add background column to pre-existing databases that
	// were created before this column was added to the schema.
	// We use PRAGMA table_info to check idempotently rather than relying on
	// "IF NOT EXISTS" (not supported by the modernc SQLite driver).
	return addColumnIfMissing(db, "responses", "background",
		`ALTER TABLE responses ADD COLUMN background INTEGER NOT NULL DEFAULT 0`)
}

// addColumnIfMissing runs ALTER TABLE ddl only if the named column is absent.
func addColumnIfMissing(db *sqlx.DB, table, column, ddl string) error {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return fmt.Errorf("pragma table_info: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, pk int
		var dflt interface{}
		if err := rows.Scan(&cid, &name, &typ, &notNull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil // column already exists
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.Exec(ddl)
	return err
}
