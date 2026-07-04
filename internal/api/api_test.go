package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
)

func newTestServer(t *testing.T) *httptest.Server {
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
	return srv
}

// storeRoot stores a root response directly — no staging ID (bypasses staging for test setup).
func storeRoot(t *testing.T, srv *httptest.Server, id string, blob []byte, tenantKey string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses/"+id, bytes.NewReader(blob))
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

// resolveAndStage sends POST /responses?prev={prevID} and returns the staging ID and decoded body.
func resolveAndStage(t *testing.T, srv *httptest.Server, prevID string, requestBlob []byte, tenantKey string) (string, map[string]json.RawMessage) {
	t.Helper()
	url := srv.URL + "/responses"
	if prevID != "" {
		url += "?prev=" + prevID
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(requestBlob))
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	var stagingID string
	_ = json.Unmarshal(body["staging_id"], &stagingID)
	return stagingID, body
}

// storeWithStaging sends POST /responses/{id}?req={stagingID}.
func storeWithStaging(t *testing.T, srv *httptest.Server, id, stagingID string, blob []byte, tenantKey string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses/"+id+"?req="+stagingID, bytes.NewReader(blob))
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func doGET(t *testing.T, srv *httptest.Server, path string, tenantKey string) *http.Response {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+path, nil)
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

// --- tests ---

func TestFirstTurnStaging(t *testing.T) {
	srv := newTestServer(t)
	reqBlob := []byte(`{"input":[{"type":"message","role":"user","content":"hello"}]}`)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses", bytes.NewReader(reqBlob))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var body map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	var stagingID string
	require.NoError(t, json.Unmarshal(body["staging_id"], &stagingID))
	assert.NotEmpty(t, stagingID, "first-turn staging returns a staging ID")
	var turns []json.RawMessage
	require.NoError(t, json.Unmarshal(body["turns"], &turns))
	assert.Empty(t, turns, "first-turn staging returns no prior turns")
}

func TestResolveUnknownPrevID(t *testing.T) {
	srv := newTestServer(t)
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses/resp_unknown/resolve", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestStoreRootAndRetrieve(t *testing.T) {
	srv := newTestServer(t)
	blob := []byte(`{"id":"resp_api1","model":"test-model","status":"completed","output":[]}`)
	storeRoot(t, srv, "resp_api1", blob, "")

	resp := doGET(t, srv, "/responses/resp_api1", "")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.JSONEq(t, string(blob), string(body))
	assert.NotEmpty(t, resp.Header.Get("X-Created-At"))
	assert.Equal(t, "0", resp.Header.Get("X-Depth"))
}

func TestResolveReturnsTurns(t *testing.T) {
	srv := newTestServer(t)

	blob0 := []byte(`{"id":"resp_r0","status":"completed","output":[{"type":"message"}]}`)
	storeRoot(t, srv, "resp_r0", blob0, "")

	reqBlob := []byte(`{"input":[{"type":"message","role":"user"}]}`)
	_, body := resolveAndStage(t, srv, "resp_r0", reqBlob, "")

	var turns []json.RawMessage
	_ = json.Unmarshal(body["turns"], &turns)
	assert.Len(t, turns, 1)
	assert.NotEmpty(t, body["staging_id"])
}

func TestStoreContinuationTurn(t *testing.T) {
	srv := newTestServer(t)

	blob0 := []byte(`{"id":"resp_chain0","status":"completed","output":[]}`)
	storeRoot(t, srv, "resp_chain0", blob0, "")

	stagingID, _ := resolveAndStage(t, srv, "resp_chain0", nil, "")
	blob1 := []byte(`{"id":"resp_chain1","status":"completed","output":[]}`)
	storeWithStaging(t, srv, "resp_chain1", stagingID, blob1, "")

	resp := doGET(t, srv, "/responses/resp_chain1", "")
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "1", resp.Header.Get("X-Depth"))
}

func TestDeleteThenRetrieve(t *testing.T) {
	srv := newTestServer(t)
	storeRoot(t, srv, "resp_del1", []byte(`{"id":"resp_del1","status":"completed","output":[]}`), "")

	delReq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/responses/resp_del1", nil)
	delResp, err := http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	getResp := doGET(t, srv, "/responses/resp_del1", "")
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestDeleteNotFound(t *testing.T) {
	srv := newTestServer(t)
	delReq, _ := http.NewRequest(http.MethodDelete, srv.URL+"/responses/resp_nothere", nil)
	resp, err := http.DefaultClient.Do(delReq)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestStoreWithUnknownStagingID(t *testing.T) {
	srv := newTestServer(t)
	blob := []byte(`{"id":"resp_x","status":"completed","output":[]}`)
	req, _ := http.NewRequest(http.MethodPost,
		srv.URL+"/responses/resp_x?req=00000000-0000-0000-0000-000000000001",
		bytes.NewReader(blob))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
}

func TestTenantIsolation(t *testing.T) {
	srv := newTestServer(t)
	storeRoot(t, srv, "resp_iso1", []byte(`{"id":"resp_iso1","status":"completed","output":[]}`), "alice")

	// bob cannot see alice's entry
	resp := doGET(t, srv, "/responses/resp_iso1", "bob")
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// alice can see her own entry
	resp2 := doGET(t, srv, "/responses/resp_iso1", "alice")
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}

func TestHealthzReadyz(t *testing.T) {
	srv := newTestServer(t)

	hResp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer hResp.Body.Close()
	assert.Equal(t, http.StatusOK, hResp.StatusCode)

	rResp, err := http.Get(srv.URL + "/readyz")
	require.NoError(t, err)
	defer rResp.Body.Close()
	assert.Equal(t, http.StatusOK, rResp.StatusCode)
}

func TestRecoveryMiddleware(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
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
