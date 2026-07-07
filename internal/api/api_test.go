package api_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
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

// storeRoot stores a root response using POST /responses (buffered path).
func storeRoot(t *testing.T, srv *httptest.Server, id string, blob []byte, tenantKey string) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"response_id":   id,
		"response_blob": json.RawMessage(blob),
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

// resolveAndStage sends POST /staging?prev={prevID} and returns the staging ID and decoded body.
func resolveAndStage(t *testing.T, srv *httptest.Server, prevID string, requestBlob []byte, tenantKey string) (string, map[string]json.RawMessage) {
	t.Helper()
	u := srv.URL + "/staging"
	if prevID != "" {
		u += "?prev=" + prevID
	}
	req, _ := http.NewRequest(http.MethodPost, u, bytes.NewReader(requestBlob))
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var body map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&body))
	var stagingID string
	_ = json.Unmarshal(body["staging_id"], &stagingID)
	return stagingID, body
}

// storeWithStaging commits a continuation turn via the streaming path:
// PUT /staging/{stagingID}/chunks/0 then PUT /staging/{stagingID}/complete.
func storeWithStaging(t *testing.T, srv *httptest.Server, id, stagingID string, blob []byte, tenantKey string) {
	t.Helper()
	chunkURL := fmt.Sprintf("%s/staging/%s/chunks/0?response_id=%s", srv.URL, stagingID, id)
	req, _ := http.NewRequest(http.MethodPut, chunkURL, bytes.NewReader(blob))
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.True(t, resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK)

	commitURL := fmt.Sprintf("%s/staging/%s/complete?response_id=%s&total=%d",
		srv.URL, stagingID, id, len(blob))
	req, _ = http.NewRequest(http.MethodPut, commitURL, nil)
	if tenantKey != "" {
		req.Header.Set("X-Tenant-Key", tenantKey)
	}
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
}

// putChunk sends PUT /staging/<stagingID>/chunks/<k>[?response_id=...]
// (streaming ingest). The chunk number is in the URL path so the request
// is genuinely idempotent at the wire level. response_id is bound on
// first use and locked thereafter.
type putChunkOpts struct {
	responseID string
}

func putChunk(t *testing.T, srv *httptest.Server, stagingID string, k uint32, chunk []byte, opts *putChunkOpts) {
	t.Helper()
	u := fmt.Sprintf("%s/staging/%s/chunks/%d", srv.URL, stagingID, k)
	if opts != nil && opts.responseID != "" {
		u += "?response_id=" + opts.responseID
	}
	req, _ := http.NewRequest(http.MethodPut, u, bytes.NewReader(chunk))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.True(t,
		resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK,
		"chunk PUT must return 202 (new) or 200 (replay); got %d", resp.StatusCode)
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
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/staging", bytes.NewReader(reqBlob))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
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
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/staging?prev=resp_unknown", nil)
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

// TestBufferedStoreAndRetrieve exercises POST /responses directly:
// verifies 201, Location header, response_id in body, and round-trip GET.
func TestBufferedStoreAndRetrieve(t *testing.T) {
	srv := newTestServer(t)
	blob := []byte(`{"id":"resp_buf1","model":"test","status":"completed","output":[]}`)
	body, _ := json.Marshal(map[string]interface{}{
		"response_id":   "resp_buf1",
		"response_blob": json.RawMessage(blob),
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "/responses/resp_buf1", resp.Header.Get("Location"))

	var respBody map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))
	var gotID string
	require.NoError(t, json.Unmarshal(respBody["response_id"], &gotID))
	assert.Equal(t, "resp_buf1", gotID)

	got := doGET(t, srv, "/responses/resp_buf1", "")
	defer got.Body.Close()
	require.Equal(t, http.StatusOK, got.StatusCode)
	gotBlob, _ := io.ReadAll(got.Body)
	assert.JSONEq(t, string(blob), string(gotBlob))
	assert.Equal(t, "0", got.Header.Get("X-Depth"))
}

// TestBufferedStoreServerAssignsID verifies that omitting response_id causes
// the server to generate one, which is returned in the body and reachable via GET.
func TestBufferedStoreServerAssignsID(t *testing.T) {
	srv := newTestServer(t)
	blob := []byte(`{"status":"completed","output":[]}`)
	body, _ := json.Marshal(map[string]interface{}{
		"response_blob": json.RawMessage(blob),
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var respBody map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))
	var assignedID string
	require.NoError(t, json.Unmarshal(respBody["response_id"], &assignedID))
	require.NotEmpty(t, assignedID, "server must assign a non-empty response_id")
	assert.Equal(t, "/responses/"+assignedID, resp.Header.Get("Location"))

	got := doGET(t, srv, "/responses/"+assignedID, "")
	defer got.Body.Close()
	assert.Equal(t, http.StatusOK, got.StatusCode)
}

// TestBufferedStoreChain verifies that prev_id in the buffered path links turns
// correctly: depth increments and the body is round-tripped.
func TestBufferedStoreChain(t *testing.T) {
	srv := newTestServer(t)

	blob0 := []byte(`{"id":"resp_bc0","status":"completed","output":[]}`)
	storeRoot(t, srv, "resp_bc0", blob0, "")

	blob1 := []byte(`{"id":"resp_bc1","status":"completed","output":[]}`)
	body, _ := json.Marshal(map[string]interface{}{
		"prev_id":       "resp_bc0",
		"response_id":   "resp_bc1",
		"response_blob": json.RawMessage(blob1),
	})
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	var respBody map[string]json.RawMessage
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&respBody))
	var turns []json.RawMessage
	require.NoError(t, json.Unmarshal(respBody["turns"], &turns))
	assert.Len(t, turns, 1, "buffered store with prev_id returns 1 prior turn")

	got := doGET(t, srv, "/responses/resp_bc1", "")
	defer got.Body.Close()
	require.Equal(t, http.StatusOK, got.StatusCode)
	gotBlob, _ := io.ReadAll(got.Body)
	assert.JSONEq(t, string(blob1), string(gotBlob))
	assert.Equal(t, "1", got.Header.Get("X-Depth"))
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
	bogus := "00000000-0000-0000-0000-000000000001"
	// Complete against a staging ID that was never opened returns 422.
	u := fmt.Sprintf("%s/staging/%s/complete?response_id=resp_x&total=1", srv.URL, bogus)
	req, _ := http.NewRequest(http.MethodPut, u, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
}

// TestPUTChunkAndPOSTCommit exercises the streaming ingest path end-to-end:
// PUT /responses/<stagingID>?offset=N × 3, then POST /responses/<id>?req=<sid>
// to commit, then GET /responses/<id> to read back the reassembled response.
func TestPUTChunkAndPOSTCommit(t *testing.T) {
	srv := newTestServer(t)
	stagingID, _ := resolveAndStage(t, srv, "", nil, "")

	chunk0 := []byte(`[{"type":"message","content":"part0"}]`)
	chunk1 := []byte(`[{"type":"message","content":"part1"}]`)
	chunk2 := []byte(`[{"type":"message","content":"part2"}]`)

	// Write three chunks via the new chunk-path API.
	putChunk(t, srv, stagingID, 0, chunk0, nil)
	putChunk(t, srv, stagingID, 1, chunk1, nil)
	putChunk(t, srv, stagingID, 2, chunk2, nil)

	// Commit via PUT /staging/<sid>/complete?response_id=...&total=...
	combined := append(append(chunk0, chunk1...), chunk2...)
	commitURL := fmt.Sprintf("%s/staging/%s/complete?response_id=resp_stream&total=%d",
		srv.URL, stagingID, len(combined))
	req, _ := http.NewRequest(http.MethodPut, commitURL, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "/responses/resp_stream", resp.Header.Get("Location"))

	// Retrieve: reassembled blob is the JSON concatenation of three arrays.
	got, hdr := retrieve(t, srv, "resp_stream")
	assert.NotEmpty(t, hdr.Get("X-Created-At"))
	assert.Equal(t, string(combined), string(got))
}

// TestPUTChunkAppendOnly uses the chunk-path API: each chunk PUT goes
// to /chunks/<k>. The commit is a separate PUT to /complete.
func TestPUTChunkAppendOnly(t *testing.T) {
	srv := newTestServer(t)
	stagingID, _ := resolveAndStage(t, srv, "", nil, "")

	chunk0 := []byte(`[{"type":"message","content":"part0"}]`)
	chunk1 := []byte(`[{"type":"message","content":"part1"}]`)
	chunk2 := []byte(`[{"type":"message","content":"part2"}]`)
	putChunk(t, srv, stagingID, 0, chunk0, nil)
	putChunk(t, srv, stagingID, 1, chunk1, nil)
	putChunk(t, srv, stagingID, 2, chunk2, nil)

	// Commit.
	combined := append(append(chunk0, chunk1...), chunk2...)
	commitURL := fmt.Sprintf("%s/staging/%s/complete?response_id=resp_stream&total=%d",
		srv.URL, stagingID, len(combined))
	req, _ := http.NewRequest(http.MethodPut, commitURL, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)

	got, _ := retrieve(t, srv, "resp_stream")
	assert.Equal(t, string(combined), string(got))
}

// TestCompleteRequiresResponseID: /complete without a bound response_id
// and without ?response_id=... returns 400. Without one or the other, the
// data would be unreachable via /responses/{id} after /staging/{id} flips
// to 410, so the chainstore rejects the call up front.
func TestCompleteRequiresResponseID(t *testing.T) {
	srv := newTestServer(t)
	stagingID, _ := resolveAndStage(t, srv, "", nil, "")

	u := fmt.Sprintf("%s/staging/%s/complete?total=2", srv.URL, stagingID)
	req, _ := http.NewRequest(http.MethodPut, u, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestCompleteRequiresTotal rejects /complete without the required total
// query param.
func TestCompleteRequiresTotal(t *testing.T) {
	srv := newTestServer(t)
	stagingID, _ := resolveAndStage(t, srv, "", nil, "")

	u := fmt.Sprintf("%s/staging/%s/complete?response_id=r1", srv.URL, stagingID)
	req, _ := http.NewRequest(http.MethodPut, u, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

// TestPUTChunkEarlyBinding: response_id can be sent on any chunk PUT.
// The first PUT carrying it binds the staging record. Subsequent PUTs
// with a different response_id get 409 Conflict.
func TestPUTChunkEarlyBinding(t *testing.T) {
	srv := newTestServer(t)
	stagingID, _ := resolveAndStage(t, srv, "", nil, "")

	// First PUT: append-only, no response_id.
	putChunk(t, srv, stagingID, 0, []byte("part0"), nil)

	// Second PUT: bind response_id, no complete.
	putChunk(t, srv, stagingID, 1, []byte("part1"), &putChunkOpts{responseID: "r_early"})

	// Third PUT: same response_id, idempotent re-bind.
	putChunk(t, srv, stagingID, 2, []byte("part2"), &putChunkOpts{responseID: "r_early"})

	// Fourth PUT: conflicting response_id → 409 Conflict.
	u := fmt.Sprintf("%s/staging/%s/chunks/3?response_id=r_other", srv.URL, stagingID)
	req, _ := http.NewRequest(http.MethodPut, u, bytes.NewReader([]byte("part3")))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusConflict, resp.StatusCode)

	// Final PUT (separate /complete): uses the bound r_early; succeeds.
	body := []byte("part0part1part2")
	commitURL := fmt.Sprintf("%s/staging/%s/complete?response_id=r_early&total=%d",
		srv.URL, stagingID, len(body))
	req, _ = http.NewRequest(http.MethodPut, commitURL, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	assert.Equal(t, http.StatusCreated, resp.StatusCode)
	assert.Equal(t, "/responses/r_early", resp.Header.Get("Location"))
}

// TestPUTChunkRequiresStagingID rejects PUT calls without a path
// stagingID so misconfigured clients fail fast.
func TestPUTChunkRequiresStagingID(t *testing.T) {
	srv := newTestServer(t)
	req, _ := http.NewRequest(http.MethodPut,
		srv.URL+"/responses/", bytes.NewReader([]byte("[]"))) // empty path
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	// Either 404 (route miss) or 400 (handler validation) is acceptable.
	assert.Contains(t, []int{http.StatusBadRequest, http.StatusNotFound}, resp.StatusCode)
}

// TestPUTChunkAtUnknownStagingID rejects PUT calls against a stagingID
// that doesn't exist.
func TestPUTChunkAtUnknownStagingID(t *testing.T) {
	srv := newTestServer(t)
	bogus := "00000000-0000-0000-0000-000000000099"
	req, _ := http.NewRequest(http.MethodPut,
		srv.URL+"/staging/"+bogus+"/chunks/0",
		bytes.NewReader([]byte("[]")))
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

// TestHandleStagingStatus_TerminalStates verifies that GET /responses/staging/<id>
// returns the correct status for each terminal outcome:
//   - 303 See Other (with Location) once the staging record is committed
//   - 410 Gone once the staging record is aborted
func TestHandleStagingStatus_TerminalStates(t *testing.T) {
	t.Run("completed_redirects", func(t *testing.T) {
		srv := newTestServer(t)
		stagingID, _ := resolveAndStage(t, srv, "", []byte("req"), "")

		putChunk(t, srv, stagingID, 0, []byte("payload"), nil)
		commitURL := fmt.Sprintf("%s/staging/%s/complete?response_id=r_done&total=7",
			srv.URL, stagingID)
		commitReq, _ := http.NewRequest(http.MethodPut, commitURL, nil)
		commitResp, err := http.DefaultClient.Do(commitReq)
		require.NoError(t, err)
		commitResp.Body.Close()
		require.Equal(t, http.StatusCreated, commitResp.StatusCode)

		// GET /staging/{id} must redirect to the canonical resource, not return 410.
		client := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse // do not follow redirect
		}}
		statusResp, err := client.Get(srv.URL + "/staging/" + stagingID)
		require.NoError(t, err)
		statusResp.Body.Close()
		assert.Equal(t, http.StatusSeeOther, statusResp.StatusCode)
		assert.Equal(t, "/responses/r_done", statusResp.Header.Get("Location"))
	})

	t.Run("aborted_is_gone", func(t *testing.T) {
		srv := newTestServer(t)
		stagingID, _ := resolveAndStage(t, srv, "", []byte("req"), "")

		abortReq, _ := http.NewRequest(http.MethodPut,
			fmt.Sprintf("%s/staging/%s/abort", srv.URL, stagingID), nil)
		abortResp, err := http.DefaultClient.Do(abortReq)
		require.NoError(t, err)
		abortResp.Body.Close()
		require.Equal(t, http.StatusNoContent, abortResp.StatusCode)

		statusResp, err := http.Get(srv.URL + "/staging/" + stagingID)
		require.NoError(t, err)
		statusResp.Body.Close()
		assert.Equal(t, http.StatusGone, statusResp.StatusCode)
	})
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
