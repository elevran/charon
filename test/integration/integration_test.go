package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/openai/openai-go/responses"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/config"
	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/memory"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
	"github.com/elevran/charon/internal/worker"
)

// fixture bundles the test server with its backing stores for direct manipulation.
type fixture struct {
	srv      *httptest.Server
	index    storage.IndexStore
	payloads storage.PayloadStore
	svc      *store.ContextStore
	log      *slog.Logger
}

func (f *fixture) URL() string { return f.srv.URL }

// doJSON sends a JSON request and returns the response (body not yet read).
func (f *fixture) doJSON(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, f.URL()+path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func newFixture(t *testing.T, idx storage.IndexStore, pay storage.PayloadStore, cfg store.Config) *fixture {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := store.New(idx, pay, cfg, log)
	h := api.NewHandler(svc, log)

	reg := prometheus.NewRegistry()
	require.NoError(t, metrics.Register(reg, ""))

	mux := http.NewServeMux()
	api.RegisterHandlers(mux, h)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &fixture{srv: srv, index: idx, payloads: pay, svc: svc, log: log}
}

func newMemoryFixture(t *testing.T, cfg store.Config) *fixture {
	t.Helper()
	return newFixture(t, memory.NewIndexStore(), memory.NewPayloadStore(), cfg)
}

func newSQLiteFixture(t *testing.T, cfg store.Config) *fixture {
	t.Helper()
	if testing.Short() {
		t.Skip("sqlite fixture requires disk; skipped in -short mode")
	}
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	scfg := config.StorageConfig{Backend: "sqlite", DataDir: dir}
	idx, pay, cleanup, err := sqlitestore.Open(scfg, log)
	require.NoError(t, err)
	t.Cleanup(func() { _ = cleanup() })
	return newFixture(t, idx, pay, cfg)
}

// backends returns the set of fixture constructors to test against.
type backendFactory func(t *testing.T, cfg store.Config) *fixture

var backends = []struct {
	name    string
	factory backendFactory
}{
	{"memory", newMemoryFixture},
	{"sqlite", newSQLiteFixture},
}

// helpers to build a store request quickly.
func storeReq(prevID *string, inp, out string) model.StoreRequest {
	var inpParam responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(json.RawMessage(inp), &inpParam)
	return model.StoreRequest{
		PreviousResponseID: prevID,
		Input:              responses.ResponseInputParam{inpParam},
		Output:             []json.RawMessage{json.RawMessage(out)},
		Status:             "completed",
		Model:              "test",
	}
}

func ptr(s string) *string { return &s }

// --- legacy tests (kept for backward compat) ---

func startServer(t *testing.T) *httptest.Server {
	t.Helper()
	return newMemoryFixture(t, store.Config{}).srv
}

func TestMetricsEndpoint(t *testing.T) {
	srv := startServer(t)

	resp, err := http.Get(srv.URL + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.True(t, strings.Contains(string(body), "responses_write_intent_failures_total"))
}

func TestFullStoreCycle(t *testing.T) {
	srv := startServer(t)

	inp := json.RawMessage(`{"type":"message","role":"user","text":"hello"}`)
	out := json.RawMessage(`{"type":"message","role":"assistant","text":"hi"}`)
	var inpParam responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(inp, &inpParam)
	req := model.StoreRequest{
		Input:  responses.ResponseInputParam{inpParam},
		Output: []json.RawMessage{out},
		Status: "completed",
		Model:  "test",
	}
	body, _ := json.Marshal(req)

	storeResp, err := http.Post(srv.URL+"/responses/resp_smoke1", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer storeResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, storeResp.StatusCode)

	getResp, err := http.Get(srv.URL + "/responses/resp_smoke1")
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var retrieved model.RetrieveResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&retrieved))
	assert.Equal(t, "resp_smoke1", retrieved.ID)

	resolveResp, err := http.Get(srv.URL + "/responses/resp_smoke1/context")
	require.NoError(t, err)
	defer resolveResp.Body.Close()
	require.Equal(t, http.StatusOK, resolveResp.StatusCode)
	var resolved model.ResolveResponse
	require.NoError(t, json.NewDecoder(resolveResp.Body).Decode(&resolved))
	assert.NotEmpty(t, resolved.ReservationID)
	assert.Len(t, resolved.FlatContext, 2)
}

// --- new scenario tests ---

// TestNewChainStoreAndRetrieve: POST a new response, GET it back, verify fields.
func TestNewChainStoreAndRetrieve(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			req := storeReq(nil,
				`{"type":"message","role":"user","text":"hello"}`,
				`{"type":"message","role":"assistant","text":"hi"}`,
			)
			r := fx.doJSON(t, "POST", "/responses/resp_new1", req)
			defer r.Body.Close()
			require.Equal(t, http.StatusNoContent, r.StatusCode)

			r2 := fx.doJSON(t, "GET", "/responses/resp_new1", nil)
			defer r2.Body.Close()
			require.Equal(t, http.StatusOK, r2.StatusCode)

			var got model.RetrieveResponse
			require.NoError(t, json.NewDecoder(r2.Body).Decode(&got))
			assert.Equal(t, "resp_new1", got.ID)
			assert.Equal(t, "test", got.Model)
			assert.Equal(t, responses.ResponseStatus("completed"), got.Status)
			assert.Nil(t, got.PreviousResponseID)
			assert.Len(t, got.Output, 1)
		})
	}
}

// TestSingleContinuation: two-turn chain; resolve turn1 yields both turns.
func TestSingleContinuation(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			r0 := fx.doJSON(t, "POST", "/responses/resp_cont0",
				storeReq(nil, `{"type":"message","role":"user","text":"hi"}`, `{"type":"message","role":"assistant","text":"hello"}`))
			defer r0.Body.Close()
			require.Equal(t, http.StatusNoContent, r0.StatusCode)

			r1 := fx.doJSON(t, "POST", "/responses/resp_cont1",
				storeReq(ptr("resp_cont0"),
					`{"type":"message","role":"user","text":"how are you?"}`,
					`{"type":"message","role":"assistant","text":"fine"}`))
			defer r1.Body.Close()
			require.Equal(t, http.StatusNoContent, r1.StatusCode)

			// Resolve turn1: should include all 4 items (2 per turn).
			r2 := fx.doJSON(t, "GET", "/responses/resp_cont1/context", nil)
			defer r2.Body.Close()
			require.Equal(t, http.StatusOK, r2.StatusCode)

			var resolved model.ResolveResponse
			require.NoError(t, json.NewDecoder(r2.Body).Decode(&resolved))
			assert.NotEmpty(t, resolved.ReservationID)
			assert.Len(t, resolved.FlatContext, 4)
		})
	}
}

// TestMultiTurnChain builds a 5-turn chain and verifies flat context order.
func TestMultiTurnChain(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			const n = 5
			ids := make([]string, n)
			for i := range n {
				ids[i] = fmt.Sprintf("resp_mt%02d", i)
				var prevID *string
				if i > 0 {
					prevID = ptr(ids[i-1])
				}
				inp := fmt.Sprintf(`{"type":"message","role":"user","text":"turn%d"}`, i)
				out := fmt.Sprintf(`{"type":"message","role":"assistant","text":"reply%d"}`, i)
				r := fx.doJSON(t, "POST", "/responses/"+ids[i], storeReq(prevID, inp, out))
				defer r.Body.Close()
				require.Equal(t, http.StatusNoContent, r.StatusCode, "turn %d", i)
			}

			r := fx.doJSON(t, "GET", "/responses/"+ids[n-1]+"/context", nil)
			defer r.Body.Close()
			require.Equal(t, http.StatusOK, r.StatusCode)

			var resolved model.ResolveResponse
			require.NoError(t, json.NewDecoder(r.Body).Decode(&resolved))
			// Each turn has 1 input + 1 output item = n*2 total.
			assert.Len(t, resolved.FlatContext, n*2)

			// Verify chronological order: first item should be user role (turn 0 input).
			var first struct {
				Role string `json:"role"`
			}
			require.NoError(t, json.Unmarshal(resolved.FlatContext[0], &first))
			assert.Equal(t, "user", first.Role)
		})
	}
}

// TestChainBoundary: build a 4-turn chain and verify context assembly at intermediate
// and final positions is correct (replaces the old checkpoint-boundary test).
func TestCheckpointBoundary(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			store3 := func(id string, prevID *string, turn int) {
				inp := fmt.Sprintf(`{"type":"message","role":"user","text":"u%d"}`, turn)
				out := fmt.Sprintf(`{"type":"message","role":"assistant","text":"a%d"}`, turn)
				r := fx.doJSON(t, "POST", "/responses/"+id, storeReq(prevID, inp, out))
				defer r.Body.Close()
				require.Equal(t, http.StatusNoContent, r.StatusCode)
			}

			store3("resp_ck0", nil, 0)
			store3("resp_ck1", ptr("resp_ck0"), 1)
			store3("resp_ck2", ptr("resp_ck1"), 2)
			store3("resp_ck3", ptr("resp_ck2"), 3)

			// Resolve ck1: 2 turns × 2 items.
			r := fx.doJSON(t, "GET", "/responses/resp_ck1/context", nil)
			defer r.Body.Close()
			require.Equal(t, http.StatusOK, r.StatusCode)
			var p1 model.ResolveResponse
			require.NoError(t, json.NewDecoder(r.Body).Decode(&p1))
			assert.Len(t, p1.FlatContext, 4, "ck1 should have 4 items")

			// Resolve ck3: 4 turns × 2 items.
			r2 := fx.doJSON(t, "GET", "/responses/resp_ck3/context", nil)
			defer r2.Body.Close()
			require.Equal(t, http.StatusOK, r2.StatusCode)
			var p3 model.ResolveResponse
			require.NoError(t, json.NewDecoder(r2.Body).Decode(&p3))
			assert.Len(t, p3.FlatContext, 8, "ck3 should have 8 items")

			// Verify chronological order: first item should be user role (turn 0 input).
			var first struct {
				Role string `json:"role"`
			}
			require.NoError(t, json.Unmarshal(p3.FlatContext[0], &first))
			assert.Equal(t, "user", first.Role)
		})
	}
}

// TestWriteIntentRecovery: a stale stream_open intent is recovered as failed.
func TestWriteIntentRecovery(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			// Start a stream (creates a stream_open write intent).
			chunk := model.ChunkRequest{
				Type:  "chunk",
				Seq:   0,
				Items: []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant","text":"chunk0"}`)},
			}
			r := fx.doJSON(t, "PATCH", "/responses/resp_wir1", chunk)
			defer r.Body.Close()
			require.Equal(t, http.StatusNoContent, r.StatusCode)

			// Run the reconciler with stale=0 so the just-created intent is already stale.
			rec := worker.NewReconciler(fx.index, fx.payloads, fx.log, 0, time.Hour)
			rec.RunOnce(context.Background())

			// The stream was never committed; the response should not exist.
			r2 := fx.doJSON(t, "GET", "/responses/resp_wir1", nil)
			defer r2.Body.Close()
			assert.Equal(t, http.StatusNotFound, r2.StatusCode)
		})
	}
}

// TestTTLExpiry: store a response, set its TTL to the past, run cleaner, confirm it's gone.
func TestTTLExpiry(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			r := fx.doJSON(t, "POST", "/responses/resp_ttl1",
				storeReq(nil, `{"type":"message","role":"user","text":"bye"}`, `{"type":"message","role":"assistant","text":"farewell"}`))
			defer r.Body.Close()
			require.Equal(t, http.StatusNoContent, r.StatusCode)

			// Override ExpiresAt to 1 second in the past by re-putting the meta.
			meta, err := fx.index.Get(context.Background(), "resp_ttl1")
			require.NoError(t, err)
			past := time.Now().Add(-time.Second).Unix()
			meta.ExpiresAt = &past
			require.NoError(t, fx.index.Put(context.Background(), meta))

			// Run a single TTL sweep directly.
			sweepTTL(t, fx.index, fx.payloads, fx.log)

			r2 := fx.doJSON(t, "GET", "/responses/resp_ttl1", nil)
			defer r2.Body.Close()
			assert.Equal(t, http.StatusNotFound, r2.StatusCode)
		})
	}
}

// sweepTTL runs a single TTL sweep directly against the stores.
func sweepTTL(t *testing.T, index storage.IndexStore, payloads storage.PayloadStore, log *slog.Logger) {
	t.Helper()
	expired, err := index.ListExpired(context.Background(), time.Now().Unix())
	require.NoError(t, err)
	for _, meta := range expired {
		if meta.PayloadKey != "" {
			_ = payloads.Delete(context.Background(), meta.PayloadKey)
		}
		require.NoError(t, index.Delete(context.Background(), meta.ID))
	}
}

// TestChunkedStreamInOrder: PATCH chunks in seq order, commit, verify assembled output.
func TestChunkedStreamInOrder(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			// Send 3 chunks in order.
			for seq := range 3 {
				chunk := model.ChunkRequest{
					Type:  "chunk",
					Seq:   seq,
					Items: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"type":"message","seq":%d}`, seq))},
				}
				r := fx.doJSON(t, "PATCH", "/responses/resp_stream1", chunk)
				defer r.Body.Close()
				require.Equal(t, http.StatusNoContent, r.StatusCode)
			}

			// Commit.
			commit := model.ChunkRequest{
				Type:   "commit",
				Seq:    3,
				Input:  []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","text":"stream-in"}`)},
				Status: "completed",
				Model:  "test",
			}
			r := fx.doJSON(t, "PATCH", "/responses/resp_stream1", commit)
			defer r.Body.Close()
			require.Equal(t, http.StatusNoContent, r.StatusCode)

			// Retrieve and verify 3 output items.
			r2 := fx.doJSON(t, "GET", "/responses/resp_stream1", nil)
			defer r2.Body.Close()
			require.Equal(t, http.StatusOK, r2.StatusCode)
			var got model.RetrieveResponse
			require.NoError(t, json.NewDecoder(r2.Body).Decode(&got))
			assert.Len(t, got.Output, 3, "should have 3 output items")

			// Resolve: 1 input + 3 output = 4 items.
			r3 := fx.doJSON(t, "GET", "/responses/resp_stream1/context", nil)
			defer r3.Body.Close()
			require.Equal(t, http.StatusOK, r3.StatusCode)
			var resolved model.ResolveResponse
			require.NoError(t, json.NewDecoder(r3.Body).Decode(&resolved))
			assert.Len(t, resolved.FlatContext, 4)
		})
	}
}

// TestChunkedStreamOutOfOrder: PATCH chunks with reversed seq, commit, verify correct assembly.
func TestChunkedStreamOutOfOrder(t *testing.T) {
	for _, b := range backends {
		t.Run(b.name, func(t *testing.T) {
			fx := b.factory(t, store.Config{})

			// Send 3 chunks in reverse seq order.
			for _, seq := range []int{2, 0, 1} {
				chunk := model.ChunkRequest{
					Type:  "chunk",
					Seq:   seq,
					Items: []json.RawMessage{json.RawMessage(fmt.Sprintf(`{"type":"message","seq":%d}`, seq))},
				}
				r := fx.doJSON(t, "PATCH", "/responses/resp_ooo1", chunk)
				defer r.Body.Close()
				require.Equal(t, http.StatusNoContent, r.StatusCode)
			}

			// Commit.
			commit := model.ChunkRequest{
				Type:   "commit",
				Seq:    3,
				Input:  []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","text":"ooo"}`)},
				Status: "completed",
				Model:  "test",
			}
			r := fx.doJSON(t, "PATCH", "/responses/resp_ooo1", commit)
			defer r.Body.Close()
			require.Equal(t, http.StatusNoContent, r.StatusCode)

			// Retrieve and verify items are assembled in seq order (seq 0, 1, 2).
			r2 := fx.doJSON(t, "GET", "/responses/resp_ooo1", nil)
			defer r2.Body.Close()
			require.Equal(t, http.StatusOK, r2.StatusCode)
			var got model.RetrieveResponse
			require.NoError(t, json.NewDecoder(r2.Body).Decode(&got))
			require.Len(t, got.Output, 3)

			for i, raw := range got.Output {
				var item struct {
					Seq int `json:"seq"`
				}
				require.NoError(t, json.Unmarshal(raw, &item))
				assert.Equal(t, i, item.Seq, "output item %d should have seq=%d", i, i)
			}
		})
	}
}
