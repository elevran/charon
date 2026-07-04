package charon_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	"github.com/elevran/charon/internal/charon"
)

var ctx = context.Background()

func startCharonServer(t *testing.T) *charon.Client {
	t.Helper()
	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	svc, err := pebblebe.Open(context.Background(), "", opts, chainstore.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	h := api.NewHandler(svc, log)
	mux := http.NewServeMux()
	api.RegisterHandlers(mux, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return charon.New(srv.URL, 5*time.Second)
}

func responseBlob(id, model, status string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"id":     id,
		"model":  model,
		"status": status,
		"output": []interface{}{},
	})
	return b
}

func TestClientStoreAndRetrieve(t *testing.T) {
	client := startCharonServer(t)

	blob := responseBlob("resp_chtest1", "test", "completed")
	err := client.Store(ctx, "resp_chtest1", "", "", blob)
	require.NoError(t, err)

	retrieved, err := client.Retrieve(ctx, "resp_chtest1", "")
	require.NoError(t, err)
	assert.JSONEq(t, string(blob), string(retrieved.ResponseBlob))
}

func TestClientRetrieveNotFound(t *testing.T) {
	client := startCharonServer(t)
	_, err := client.Retrieve(ctx, "resp_missing", "")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientDeleteNotFound(t *testing.T) {
	client := startCharonServer(t)
	err := client.Delete(ctx, "resp_missing", "")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientDelete(t *testing.T) {
	client := startCharonServer(t)

	blob := responseBlob("resp_del1", "test", "completed")
	require.NoError(t, client.Store(ctx, "resp_del1", "", "", blob))
	require.NoError(t, client.Delete(ctx, "resp_del1", ""))

	_, err := client.Retrieve(ctx, "resp_del1", "")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientResolveNotFound(t *testing.T) {
	client := startCharonServer(t)
	_, _, err := client.Resolve(ctx, "resp_unknown", "", nil)
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

func TestClientResolveFirstTurn(t *testing.T) {
	client := startCharonServer(t)

	reqBlob := []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`)
	stagingID, turns, err := client.Resolve(ctx, "", "", reqBlob)
	require.NoError(t, err)
	assert.NotEmpty(t, stagingID, "first-turn staging returns a staging ID")
	assert.Empty(t, turns, "first-turn staging returns no prior turns")
}

func TestClientResolveAfterStore(t *testing.T) {
	client := startCharonServer(t)

	blob := responseBlob("resp_res0", "test", "completed")
	require.NoError(t, client.Store(ctx, "resp_res0", "", "", blob))

	stagingID, turns, err := client.Resolve(ctx, "resp_res0", "", nil)
	require.NoError(t, err)
	assert.NotEmpty(t, stagingID)
	assert.Len(t, turns, 1, "resolve of a root node returns 1 turn")
	assert.Equal(t, blob, turns[0].ResponseBlob)
}
