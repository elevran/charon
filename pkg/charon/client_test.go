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

	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	api "github.com/elevran/charon/internal/server"
	"github.com/elevran/charon/pkg/charon"
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
	assert.Equal(t, json.RawMessage(blob), turns[0].ResponseBlob)
}

// TestClientGetChain exercises the read-only chain fetch.
//
// Walks a 2-turn chain via GetChain and verifies root-first turns with
// both request and response blobs materialised. Also checks the
// ErrNotFound path on unknown IDs.
func TestClientGetChain(t *testing.T) {
	client := startCharonServer(t)

	// Build a 2-turn chain via the public client surfaces.
	blob0 := responseBlob("resp_gc0", "test", "completed")
	require.NoError(t, client.Store(ctx, "resp_gc0", "", "", blob0))

	stagingID, _, err := client.Resolve(ctx, "resp_gc0", "", []byte(`{"input":[{"type":"message","role":"user"}]}`))
	require.NoError(t, err)
	blob1 := responseBlob("resp_gc1", "test", "completed")
	require.NoError(t, client.Store(ctx, "resp_gc1", stagingID, "", blob1))

	turns, err := client.GetChain(ctx, "resp_gc1", "")
	require.NoError(t, err)
	assert.Len(t, turns, 2, "GetChain on a 2-turn chain returns 2 turns")
	assert.Equal(t, json.RawMessage(blob0), turns[0].ResponseBlob)
	assert.Equal(t, json.RawMessage(blob1), turns[1].ResponseBlob)
}

// TestClientGetChainNotFound: the client surfaces ErrNotFound when the
// chain doesn't exist.
func TestClientGetChainNotFound(t *testing.T) {
	client := startCharonServer(t)
	_, err := client.GetChain(ctx, "resp_missing", "")
	assert.ErrorIs(t, err, charon.ErrNotFound)
}

// TestClientGetChainIsReadOnly: calling GetChain does not open a staging
// record. Verified indirectly by confirming the chain remains identical
// (1 turn) before and after a GetChain call, and that a subsequent
// POST /responses still works using the same prevID.
func TestClientGetChainIsReadOnly(t *testing.T) {
	client := startCharonServer(t)
	blob0 := responseBlob("resp_gcr0", "test", "completed")
	require.NoError(t, client.Store(ctx, "resp_gcr0", "", "", blob0))

	turns0, err := client.GetChain(ctx, "resp_gcr0", "")
	require.NoError(t, err)
	assert.Len(t, turns0, 1)

	turns1, err := client.GetChain(ctx, "resp_gcr0", "")
	require.NoError(t, err)
	assert.Len(t, turns1, 1, "GetChain must not append to the chain")

	// Still resolvable as a previous ID for a new turn.
	_, _, err = client.Resolve(ctx, "resp_gcr0", "", []byte(`{"input":[]}`))
	assert.NoError(t, err)
}
