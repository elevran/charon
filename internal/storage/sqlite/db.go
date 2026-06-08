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

// Open opens (or creates) a SQLite database at path, applies pragmas, and
// ensures all tables exist. Returns a *sqlx.DB ready for use.
func Open(path string, cfg Config) (*sqlx.DB, error) {
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
	_, err := db.Exec(schema)
	return err
}
