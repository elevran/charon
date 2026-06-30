package api_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

func newTestServer(t *testing.T) *httptest.Server {
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
	return srv
}

func doJSON(t *testing.T, srv *httptest.Server, method, path string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequest(method, srv.URL+path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func toInputParam(items ...json.RawMessage) responses.ResponseInputParam {
	params := make(responses.ResponseInputParam, len(items))
	for i, raw := range items {
		_ = json.Unmarshal(raw, &params[i])
	}
	return params
}

func TestResolveUnknownID(t *testing.T) {
	srv := newTestServer(t)

	resp := doJSON(t, srv, "GET", "/responses/resp_unknown/context", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	assert.Equal(t, "not found", body["error"])
}

func TestStoreNewChainAndRetrieve(t *testing.T) {
	srv := newTestServer(t)

	inp := json.RawMessage(`{"type":"message","role":"user"}`)
	out := json.RawMessage(`{"type":"message","role":"assistant"}`)
	req := model.StoreRequest{
		Input:  toInputParam(inp),
		Output: []json.RawMessage{out},
		Status: "completed",
		Model:  "test-model",
	}

	storeResp := doJSON(t, srv, "POST", "/responses/resp_api1", req)
	defer storeResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, storeResp.StatusCode)

	getResp := doJSON(t, srv, "GET", "/responses/resp_api1", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var retrieve model.RetrieveResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&retrieve))
	assert.Equal(t, "resp_api1", retrieve.ID)
	assert.Equal(t, "test-model", retrieve.Model)
}

func TestResolveContinuation(t *testing.T) {
	srv := newTestServer(t)

	inp := json.RawMessage(`{"type":"message","role":"user","text":"hi"}`)
	out := json.RawMessage(`{"type":"message","role":"assistant","text":"hello"}`)
	req := model.StoreRequest{
		Input:  toInputParam(inp),
		Output: []json.RawMessage{out},
		Status: "completed",
	}
	storeResp := doJSON(t, srv, "POST", "/responses/resp_cont0", req)
	defer storeResp.Body.Close()
	require.Equal(t, http.StatusNoContent, storeResp.StatusCode)

	resolveResp := doJSON(t, srv, "GET", "/responses/resp_cont0/context", nil)
	defer resolveResp.Body.Close()
	require.Equal(t, http.StatusOK, resolveResp.StatusCode)
	var resolved model.ResolveResponse
	require.NoError(t, json.NewDecoder(resolveResp.Body).Decode(&resolved))
	assert.NotEmpty(t, resolved.ReservationID)
	assert.Len(t, resolved.FlatContext, 2)
}

func TestDeleteThenRetrieve(t *testing.T) {
	srv := newTestServer(t)

	req := model.StoreRequest{
		Input:  toInputParam(json.RawMessage(`{"type":"message"}`)),
		Status: "completed",
	}
	storeResp := doJSON(t, srv, "POST", "/responses/resp_del1", req)
	defer storeResp.Body.Close()

	delResp := doJSON(t, srv, "DELETE", "/responses/resp_del1", nil)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	getResp := doJSON(t, srv, "GET", "/responses/resp_del1", nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestMalformedJSONBody(t *testing.T) {
	srv := newTestServer(t)

	req, _ := http.NewRequest("POST", srv.URL+"/responses/resp_bad", bytes.NewBufferString("{not json}"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestRecoveryMiddleware(t *testing.T) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	panicHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})
	handler := api.Chain(panicHandler, api.Recovery(log))
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/any")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}

func TestDeleteNotFound(t *testing.T) {
	srv := newTestServer(t)

	resp := doJSON(t, srv, "DELETE", "/responses/resp_nothere", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestListInputOutputItems(t *testing.T) {
	srv := newTestServer(t)

	// Use a commit (PATCH) to store a response whose input contains a compaction
	// item — compaction items reach the input via the streaming/commit path which
	// accepts raw JSON, not the OpenAI SDK types used by POST.
	out1 := json.RawMessage(`{"type":"message","role":"assistant","text":"hi"}`)
	out2 := json.RawMessage(`{"type":"compaction","encrypted_content":"secret"}`)
	out3 := json.RawMessage(`{"type":"message","role":"assistant","text":"bye"}`)
	chunk := model.ChunkRequest{Type: "chunk", Seq: 0, Items: []json.RawMessage{out1, out2, out3}}
	chunkResp := doJSON(t, srv, "PATCH", "/responses/resp_page1", chunk)
	defer chunkResp.Body.Close()
	require.Equal(t, http.StatusNoContent, chunkResp.StatusCode)

	// Input: one regular message + one compaction item (prior compressed context).
	commit := model.ChunkRequest{
		Type: "commit",
		Seq:  1,
		Input: []json.RawMessage{
			json.RawMessage(`{"type":"message","role":"user","text":"hello"}`),
			json.RawMessage(`{"type":"compaction","encrypted_content":"compressed"}`),
		},
		Status: "completed",
	}
	commitResp := doJSON(t, srv, "PATCH", "/responses/resp_page1", commit)
	defer commitResp.Body.Close()
	require.Equal(t, http.StatusNoContent, commitResp.StatusCode)

	t.Run("input_items returns all items including compaction", func(t *testing.T) {
		r := doJSON(t, srv, "GET", "/responses/resp_page1/input_items", nil)
		defer r.Body.Close()
		require.Equal(t, http.StatusOK, r.StatusCode)
		var page model.ItemsPage
		require.NoError(t, json.NewDecoder(r.Body).Decode(&page))
		// Both the message and the compaction item must appear — compaction items
		// in input represent prior compressed context and should not be hidden.
		assert.Len(t, page.Items, 2)
		assert.False(t, page.HasMore)
		assert.Nil(t, page.NextCursor)
	})

	t.Run("output_items hides compaction items", func(t *testing.T) {
		r := doJSON(t, srv, "GET", "/responses/resp_page1/output_items", nil)
		defer r.Body.Close()
		require.Equal(t, http.StatusOK, r.StatusCode)
		var page model.ItemsPage
		require.NoError(t, json.NewDecoder(r.Body).Decode(&page))
		assert.Len(t, page.Items, 2) // out1 and out3; out2 (compaction) excluded
		assert.False(t, page.HasMore)
	})

	t.Run("pagination with limit=1", func(t *testing.T) {
		r := doJSON(t, srv, "GET", "/responses/resp_page1/output_items?limit=1", nil)
		defer r.Body.Close()
		require.Equal(t, http.StatusOK, r.StatusCode)
		var page model.ItemsPage
		require.NoError(t, json.NewDecoder(r.Body).Decode(&page))
		assert.Len(t, page.Items, 1)
		assert.True(t, page.HasMore)
		require.NotNil(t, page.NextCursor)

		// Follow cursor
		r2 := doJSON(t, srv, "GET", "/responses/resp_page1/output_items?limit=1&after="+*page.NextCursor, nil)
		defer r2.Body.Close()
		require.Equal(t, http.StatusOK, r2.StatusCode)
		var page2 model.ItemsPage
		require.NoError(t, json.NewDecoder(r2.Body).Decode(&page2))
		assert.Len(t, page2.Items, 1)
		assert.False(t, page2.HasMore)
		assert.Nil(t, page2.NextCursor)
	})

	t.Run("invalid cursor returns 400", func(t *testing.T) {
		r := doJSON(t, srv, "GET", "/responses/resp_page1/output_items?after=!!notbase64!!", nil)
		defer r.Body.Close()
		assert.Equal(t, http.StatusBadRequest, r.StatusCode)
	})

	t.Run("unknown id returns 404", func(t *testing.T) {
		r := doJSON(t, srv, "GET", "/responses/resp_unknown/output_items", nil)
		defer r.Body.Close()
		assert.Equal(t, http.StatusNotFound, r.StatusCode)
	})
}

// TestStoreAndRetrieveBackgroundFlag verifies that background:true is persisted and returned.
func TestStoreAndRetrieveBackgroundFlag(t *testing.T) {
	srv := newTestServer(t)

	inp := json.RawMessage(`{"type":"message","role":"user"}`)
	out := json.RawMessage(`{"type":"message","role":"assistant"}`)
	req := model.StoreRequest{
		Input:      toInputParam(inp),
		Output:     []json.RawMessage{out},
		Status:     "completed",
		Model:      "test-model",
		Background: true,
	}

	storeResp := doJSON(t, srv, "POST", "/responses/resp_bg1", req)
	defer storeResp.Body.Close()
	require.Equal(t, http.StatusNoContent, storeResp.StatusCode)

	getResp := doJSON(t, srv, "GET", "/responses/resp_bg1", nil)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var retrieve model.RetrieveResponse
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&retrieve))
	assert.Equal(t, "resp_bg1", retrieve.ID)
	assert.True(t, retrieve.Background, "background flag must be persisted and returned as true")
}

// TestResolveMaxBytesExceeded verifies that GET /responses/{id}/context?max_bytes=<small>
// returns 422 with error "context_too_large" when the assembled context exceeds the limit.
func TestResolveMaxBytesExceeded(t *testing.T) {
	srv := newTestServer(t)

	req := model.StoreRequest{
		Input:  toInputParam(json.RawMessage(`{"type":"message","role":"user","text":"hi"}`)),
		Output: []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant","text":"hello"}`)},
		Status: "completed",
	}
	storeResp := doJSON(t, srv, "POST", "/responses/resp_maxbytes1", req)
	defer storeResp.Body.Close()
	require.Equal(t, http.StatusNoContent, storeResp.StatusCode)

	resolveResp := doJSON(t, srv, "GET", "/responses/resp_maxbytes1/context?max_bytes=1", nil)
	defer resolveResp.Body.Close()
	assert.Equal(t, http.StatusUnprocessableEntity, resolveResp.StatusCode)
	var body map[string]string
	require.NoError(t, json.NewDecoder(resolveResp.Body).Decode(&body))
	assert.Equal(t, "context_too_large", body["error"])
}
