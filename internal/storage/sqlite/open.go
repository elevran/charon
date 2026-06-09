package sqlite

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/filesystem"
)

// Open creates (or opens) the SQLite index store and a filesystem payload store
// rooted under cfg.DataDir. WAL journal mode and a 5s busy timeout are always
// applied — they are not user-configurable because the defaults are correct for
// all supported deployment modes.
func Open(cfg config.StorageConfig, log *slog.Logger) (storage.IndexStore, storage.PayloadStore, func() error, error) {
	noop := func() error { return nil }

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, nil, noop, fmt.Errorf("create data dir %q: %w", cfg.DataDir, err)
	}

	dbPath := filepath.Join(cfg.DataDir, "responses.db")
	db, err := openDB(dbPath, Config{
		WALMode:       true, // WAL is always on; better concurrency than DELETE journal
		BusyTimeoutMs: 5000,
	})
	if err != nil {
		return nil, nil, noop, fmt.Errorf("sqlite open %q: %w", dbPath, err)
	}
	cleanup := func() error { return Close(db) }

	payDir := filepath.Join(cfg.DataDir, "payloads")
	fsStore, err := filesystem.New(payDir)
	if err != nil {
		_ = Close(db)
		return nil, nil, noop, fmt.Errorf("filesystem store %q: %w", payDir, err)
	}

	idx, err := NewIndexStore(db)
	if err != nil {
		_ = Close(db)
		return nil, nil, noop, fmt.Errorf("prepare sqlite statements: %w", err)
	}

	if log != nil {
		log.Info("sqlite storage opened", "db", dbPath, "payloads", payDir)
	}

	return idx, fsStore, cleanup, nil
}
