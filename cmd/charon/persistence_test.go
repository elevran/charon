package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/model"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
)

// TestSQLiteBackendSurvivesRestart verifies that a stored response is
// retrievable after the stores are closed and re-opened from the same data_dir.
func TestSQLiteBackendSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	ctx := context.Background()
	storageCfg := config.StorageConfig{DataDir: dataDir}

	inpRaw := json.RawMessage(`{"type":"message","role":"user","content":"hello"}`)
	var inp responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(inpRaw, &inp)

	// "Run 1": open stores, write a response.
	{
		idx, pay, cleanup, err := sqlitestore.Open(storageCfg, log)
		require.NoError(t, err)

		svc := store.New(idx, pay, store.Config{}, log)
		req := model.StoreRequest{
			Input:  responses.ResponseInputParam{inp},
			Output: []json.RawMessage{},
			Status: "completed",
			Model:  "test-model",
		}
		require.NoError(t, svc.Store(ctx, "resp_persist1", req))
		require.NoError(t, cleanup())
	}

	// "Run 2": re-open from same data_dir, verify response is still present.
	{
		idx, pay, cleanup, err := sqlitestore.Open(storageCfg, log)
		require.NoError(t, err)
		defer cleanup() //nolint:errcheck

		svc := store.New(idx, pay, store.Config{}, log)
		meta, _, err := svc.Retrieve(ctx, "resp_persist1")
		require.NoError(t, err, "response must survive store close and reopen")
		assert.Equal(t, "resp_persist1", meta.ID)
		assert.Equal(t, "test-model", meta.Model)
	}
}
