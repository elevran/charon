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
