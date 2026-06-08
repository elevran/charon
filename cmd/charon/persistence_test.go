package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage/filesystem"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
)

// TestSQLiteBackendSurvivesRestart verifies that a stored response is
// retrievable after the stores are closed and re-opened from the same data_dir.
func TestSQLiteBackendSurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	dbPath := filepath.Join(dataDir, "responses.db")
	payDir := filepath.Join(dataDir, "payloads")
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	ctx := context.Background()

	inpRaw := json.RawMessage(`{"type":"message","role":"user","content":"hello"}`)
	var inp responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(inpRaw, &inp)

	// "Run 1": open stores, write a response.
	{
		db, err := sqlitestore.Open(dbPath, sqlitestore.Config{WALMode: true})
		require.NoError(t, err)
		pay, err := filesystem.New(payDir)
		require.NoError(t, err)

		sqlIdx, err := sqlitestore.NewIndexStore(db)
		require.NoError(t, err)
		svc := store.New(sqlIdx, pay, store.Config{}, log)
		req := model.StoreRequest{
			Input:  responses.ResponseInputParam{inp},
			Output: []json.RawMessage{},
			Status: "completed",
			Model:  "test-model",
		}
		require.NoError(t, svc.Store(ctx, "resp_persist1", req))
		require.NoError(t, sqlitestore.Close(db))
	}

	// "Run 2": re-open from same data_dir, verify response is still present.
	{
		db, err := sqlitestore.Open(dbPath, sqlitestore.Config{WALMode: true})
		require.NoError(t, err)
		defer sqlitestore.Close(db)
		pay, err := filesystem.New(payDir)
		require.NoError(t, err)

		sqlIdx2, err := sqlitestore.NewIndexStore(db)
		require.NoError(t, err)
		svc := store.New(sqlIdx2, pay, store.Config{}, log)
		meta, _, err := svc.Retrieve(ctx, "resp_persist1")
		require.NoError(t, err, "response must survive store close and reopen")
		assert.Equal(t, "resp_persist1", meta.ID)
		assert.Equal(t, "test-model", meta.Model)
	}
}
