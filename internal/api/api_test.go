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

	inp := json.RawMessage(`{"type":"message","role":"user","text":"hello"}`)
	out1 := json.RawMessage(`{"type":"message","role":"assistant","text":"hi"}`)
	out2 := json.RawMessage(`{"type":"compaction","encrypted_content":"secret"}`)
	out3 := json.RawMessage(`{"type":"message","role":"assistant","text":"bye"}`)
	req := model.StoreRequest{
		Input:  toInputParam(inp),
		Output: []json.RawMessage{out1, out2, out3},
		Status: "completed",
	}
	storeResp := doJSON(t, srv, "POST", "/responses/resp_page1", req)
	defer storeResp.Body.Close()
	require.Equal(t, http.StatusNoContent, storeResp.StatusCode)

	t.Run("input_items returns all input items", func(t *testing.T) {
		r := doJSON(t, srv, "GET", "/responses/resp_page1/input_items", nil)
		defer r.Body.Close()
		require.Equal(t, http.StatusOK, r.StatusCode)
		var page model.ItemsPage
		require.NoError(t, json.NewDecoder(r.Body).Decode(&page))
		assert.Len(t, page.Items, 1)
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
