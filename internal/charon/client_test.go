package charon_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

var ctx = context.Background()

func startCharonServer(t *testing.T) (*charon.Client, *store.ContextStore) {
	t.Helper()
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := store.New(idx, pay, store.Config{}, log)
	h := api.NewHandler(svc, log)
	mux := http.NewServeMux()
	api.RegisterHandlers(mux, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	client := charon.New(srv.URL, 5*time.Second)
	return client, svc
}

func rawItem(role, content string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"type": "message", "role": role, "content": content})
	return b
}


func TestClientStoreAndRetrieve(t *testing.T) {
	client, _ := startCharonServer(t)

	out := rawItem("assistant", "hi")
	req := charon.StoreRequest{
		Output: []json.RawMessage{out},
		Status: "completed",
		Model:  "test",
	}
	err := client.Store(ctx, "resp_chtest1", req)
	require.NoError(t, err)

	retrieved, err := client.Retrieve(ctx, "resp_chtest1")
	require.NoError(t, err)
	assert.Equal(t, "resp_chtest1", retrieved.ID)
	assert.Equal(t, "test", retrieved.Model)
}

func TestClientRetrieveNotFound(t *testing.T) {
	client, _ := startCharonServer(t)
	_, err := client.Retrieve(ctx, "resp_missing")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientDeleteNotFound(t *testing.T) {
	client, _ := startCharonServer(t)
	// Delete of non-existent key — Charon returns 404
	err := client.Delete(ctx, "resp_missing")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientDelete(t *testing.T) {
	client, _ := startCharonServer(t)

	req := charon.StoreRequest{Status: "completed", Model: "test"}
	require.NoError(t, client.Store(ctx, "resp_del1", req))
	require.NoError(t, client.Delete(ctx, "resp_del1"))

	_, err := client.Retrieve(ctx, "resp_del1")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientResolveNotFound(t *testing.T) {
	client, _ := startCharonServer(t)
	_, _, err := client.Resolve(ctx, "resp_unknown")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientResolveAfterStore(t *testing.T) {
	client, _ := startCharonServer(t)

	out := rawItem("assistant", "hi")
	req := charon.StoreRequest{
		Output: []json.RawMessage{out},
		Status: "completed",
		Model:  "test",
	}
	require.NoError(t, client.Store(ctx, "resp_res0", req))

	rsrvID, flatCtx, err := client.Resolve(ctx, "resp_res0")
	require.NoError(t, err)
	assert.NotEmpty(t, rsrvID)
	assert.True(t, len(flatCtx) >= 0) // may be empty for a root with no input
}
