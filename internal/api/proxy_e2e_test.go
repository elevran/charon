package api_test

// Proxy E2E tests verify the Charon internal API operations as the proxy performs them:
//
//  1. Store a root response       POST /responses/{id} (no staging ID)
//  2. Resolve for continuation    POST /responses?prev={id} → staging_id + turns
//  3. Store continuation          POST /responses/{id}?req={staging_id}
//  4. Retrieve / Delete           GET/DELETE /responses/{id}
//
// The flat context is assembled by the proxy from turns; these tests verify
// turn counts and blob round-trips.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- helpers re-used across e2e scenarios ---

type resolveResult struct {
	StagingID string            `json:"staging_id"`
	Turns     []json.RawMessage `json:"turns"`
}

func resolve(t *testing.T, srv *httptest.Server, prevID string, requestBlob []byte) resolveResult {
	t.Helper()
	url := srv.URL + "/responses"
	if prevID != "" {
		url += "?prev=" + prevID
	}
	req, _ := http.NewRequest(http.MethodPost, url, bytes.NewReader(requestBlob))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r resolveResult
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	return r
}

func retrieve(t *testing.T, srv *httptest.Server, id string) ([]byte, http.Header) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/responses/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return body, resp.Header
}

func del(t *testing.T, srv *httptest.Server, id string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodDelete, srv.URL+"/responses/"+id, nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	return resp.StatusCode
}

func blob(id, status string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"id":     id,
		"status": status,
		"output": []interface{}{map[string]string{"type": "message", "role": "assistant"}},
	})
	return b
}

// --- E2E scenarios ---

func TestE2EStoreAndRetrieve(t *testing.T) {
	srv := newTestServer(t)

	b := blob("resp_e2e0", "completed")
	storeRoot(t, srv, "resp_e2e0", b, "")

	got, hdr := retrieve(t, srv, "resp_e2e0")
	assert.JSONEq(t, string(b), string(got))
	assert.NotEmpty(t, hdr.Get("X-Created-At"))
	assert.Equal(t, "0", hdr.Get("X-Depth"))
}

func TestE2EThreeTurnChain(t *testing.T) {
	srv := newTestServer(t)

	// Turn 0: root
	b0 := blob("resp_e2e_t0", "completed")
	storeRoot(t, srv, "resp_e2e_t0", b0, "")

	// Turn 1: continuation
	r1 := resolve(t, srv, "resp_e2e_t0", nil)
	assert.Len(t, r1.Turns, 1, "t1 resolve sees 1 prior turn")
	b1 := blob("resp_e2e_t1", "completed")
	storeWithStaging(t, srv, "resp_e2e_t1", r1.StagingID, b1, "")

	// Turn 2: continuation
	r2 := resolve(t, srv, "resp_e2e_t1", nil)
	assert.Len(t, r2.Turns, 2, "t2 resolve sees 2 prior turns")
	b2 := blob("resp_e2e_t2", "completed")
	storeWithStaging(t, srv, "resp_e2e_t2", r2.StagingID, b2, "")

	// Verify all nodes stored correctly
	got0, _ := retrieve(t, srv, "resp_e2e_t0")
	assert.JSONEq(t, string(b0), string(got0))
	got2, hdr2 := retrieve(t, srv, "resp_e2e_t2")
	assert.JSONEq(t, string(b2), string(got2))
	assert.Equal(t, "2", hdr2.Get("X-Depth"))
}

func TestE2EDeleteAndRetrieve(t *testing.T) {
	srv := newTestServer(t)

	b := blob("resp_e2e_del", "completed")
	storeRoot(t, srv, "resp_e2e_del", b, "")

	assert.Equal(t, http.StatusNoContent, del(t, srv, "resp_e2e_del"))

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/responses/resp_e2e_del", nil)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestE2EDeleteSubtreeEvictsDescendants(t *testing.T) {
	srv := newTestServer(t)

	// Build chain t0 → t1 → t2
	storeRoot(t, srv, "resp_bc_t0", blob("resp_bc_t0", "completed"), "")

	r1 := resolve(t, srv, "resp_bc_t0", nil)
	storeWithStaging(t, srv, "resp_bc_t1", r1.StagingID, blob("resp_bc_t1", "completed"), "")

	r2 := resolve(t, srv, "resp_bc_t1", nil)
	storeWithStaging(t, srv, "resp_bc_t2", r2.StagingID, blob("resp_bc_t2", "completed"), "")

	// Delete t1 (mid-chain): HTTP DELETE always removes the full subtree.
	// t2, as a descendant of t1, is also removed.
	assert.Equal(t, http.StatusNoContent, del(t, srv, "resp_bc_t1"))

	// t1 is gone
	req1, _ := http.NewRequest(http.MethodGet, srv.URL+"/responses/resp_bc_t1", nil)
	resp1, err := http.DefaultClient.Do(req1)
	require.NoError(t, err)
	defer resp1.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp1.StatusCode)

	// t2 (descendant of t1) is also gone — deleted as part of the subtree
	req2, _ := http.NewRequest(http.MethodGet, srv.URL+"/responses/resp_bc_t2", nil)
	resp2, err := http.DefaultClient.Do(req2)
	require.NoError(t, err)
	defer resp2.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp2.StatusCode)

	// t0 (ancestor of t1) is unaffected
	got0, _ := retrieve(t, srv, "resp_bc_t0")
	assert.JSONEq(t, string(blob("resp_bc_t0", "completed")), string(got0))
}
