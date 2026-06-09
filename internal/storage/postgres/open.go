package postgres

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/storage"
)

const defaultMaxConns = 10

// OpenIndex opens a PostgreSQL connection pool, runs schema migrations, and
// returns an IndexStore along with a cleanup function that closes the pool.
// The cleanup function is safe to call even if OpenIndex returns an error.
func OpenIndex(cfg config.StorageConfig, log *slog.Logger) (storage.IndexStore, func() error, error) {
	noop := func() error { return nil }

	poolCfg, err := pgxpool.ParseConfig(cfg.Postgres.DSN)
	if err != nil {
		return nil, noop, fmt.Errorf("postgres parse dsn: %w", err)
	}

	maxConns := cfg.Postgres.MaxConns
	if maxConns <= 0 {
		maxConns = defaultMaxConns
	}
	poolCfg.MaxConns = maxConns

	ctx := context.Background()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, noop, fmt.Errorf("postgres connect: %w", err)
	}
	cleanup := func() error {
		pool.Close()
		return nil
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, noop, fmt.Errorf("postgres ping: %w", err)
	}

	if err := migrate(ctx, pool); err != nil {
		pool.Close()
		return nil, noop, err
	}

	if log != nil {
		log.Info("postgres index store opened", "dsn_host", poolCfg.ConnConfig.Host)
	}

	return NewIndexStore(pool), cleanup, nil
}
