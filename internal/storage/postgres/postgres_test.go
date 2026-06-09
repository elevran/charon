//go:build integration

package postgres_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	pgstore "github.com/elevran/charon/internal/storage/postgres"
)

// TestPostgresIndexStore exercises the PostgreSQL IndexStore against a real
// database. Set POSTGRES_DSN to run:
//
//	POSTGRES_DSN="postgres://user:pass@localhost:5432/testdb" go test -tags integration ./internal/storage/postgres/...
func TestPostgresIndexStore(t *testing.T) {
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skip("POSTGRES_DSN not set; skipping integration test")
	}

	cfg := config.StorageConfig{
		Postgres: config.PostgresConfig{DSN: dsn, MaxConns: 2},
	}

	idx, cleanup, err := pgstore.OpenIndex(cfg, nil)
	require.NoError(t, err)
	defer func() { _ = cleanup() }()

	ctx := context.Background()

	meta := model.ResponseMeta{
		ID:             "pg-test-id-1",
		ChainRootID:    "pg-test-id-1",
		Position:       0,
		OwnerPrincipal: "owner@example.com",
		Model:          "gpt-4",
		Status:         model.StatusCompleted,
		CreatedAt:      time.Now().Unix(),
		PayloadKey:     "pg-test-id-1/0000_pg-test-id-1.json",
	}

	t.Run("PutAndGet", func(t *testing.T) {
		err := idx.Put(ctx, meta)
		require.NoError(t, err)

		got, err := idx.Get(ctx, meta.ID)
		require.NoError(t, err)
		require.Equal(t, meta.ID, got.ID)
		require.Equal(t, meta.OwnerPrincipal, got.OwnerPrincipal)
		require.Equal(t, meta.Status, got.Status)
	})

	t.Run("GetNotFound", func(t *testing.T) {
		_, err := idx.Get(ctx, "no-such-id")
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	t.Run("List", func(t *testing.T) {
		results, err := idx.List(ctx, storage.ListOptions{Owner: "owner@example.com"})
		require.NoError(t, err)
		require.NotEmpty(t, results)
	})

	t.Run("ListExpired", func(t *testing.T) {
		future := time.Now().Add(24 * time.Hour).Unix()
		expMeta := model.ResponseMeta{
			ID:             "pg-exp-id-1",
			ChainRootID:    "pg-exp-id-1",
			Position:       0,
			OwnerPrincipal: "owner@example.com",
			Model:          "gpt-4",
			Status:         model.StatusCompleted,
			CreatedAt:      time.Now().Unix(),
			ExpiresAt:      &future,
			PayloadKey:     "pg-exp-id-1/0000_pg-exp-id-1.json",
		}
		require.NoError(t, idx.Put(ctx, expMeta))

		// Nothing expires before now+2days
		past := time.Now().Add(-time.Hour).Unix()
		expired, err := idx.ListExpired(ctx, past)
		require.NoError(t, err)
		for _, r := range expired {
			require.NotEqual(t, expMeta.ID, r.ID)
		}

		// Expires before now+2days
		beyond := time.Now().Add(48 * time.Hour).Unix()
		expired, err = idx.ListExpired(ctx, beyond)
		require.NoError(t, err)
		found := false
		for _, r := range expired {
			if r.ID == expMeta.ID {
				found = true
			}
		}
		require.True(t, found)

		_ = idx.Delete(ctx, expMeta.ID)
	})

	t.Run("WriteIntentLifecycle", func(t *testing.T) {
		intent := model.WriteIntent{
			IntentID:      "pg-intent-1",
			ResponseID:    "pg-resp-1",
			ReservationID: "pg-res-1",
			PayloadKey:    "pg-key-1",
			Phase:         model.WriteIntentPending,
			CreatedAt:     time.Now().Unix(),
			UpdatedAt:     time.Now().Unix(),
		}
		err := idx.InsertWriteIntent(ctx, intent)
		require.NoError(t, err)

		// Duplicate insert returns ErrAlreadyExists
		err = idx.InsertWriteIntent(ctx, intent)
		require.ErrorIs(t, err, storage.ErrAlreadyExists)

		// Stale intents should include ours (threshold = 0 age)
		stale, err := idx.ListStaleWriteIntents(ctx, 0)
		require.NoError(t, err)
		found := false
		for _, s := range stale {
			if s.IntentID == intent.IntentID {
				found = true
			}
		}
		require.True(t, found)

		err = idx.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentCommitted)
		require.NoError(t, err)

		err = idx.DeleteWriteIntent(ctx, intent.IntentID)
		require.NoError(t, err)

		// Idempotent delete
		err = idx.DeleteWriteIntent(ctx, intent.IntentID)
		require.NoError(t, err)
	})

	t.Run("UpdateWriteIntentNotFound", func(t *testing.T) {
		err := idx.UpdateWriteIntent(ctx, "no-such-intent", model.WriteIntentCommitted)
		require.ErrorIs(t, err, storage.ErrNotFound)
	})

	// Cleanup test data
	t.Cleanup(func() {
		_ = idx.Delete(ctx, meta.ID)
	})
}
