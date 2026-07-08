package integration_test

// Integration tests exercise the Charon internal API end-to-end against a real
// pebble backend, verifying the full store→resolve→retrieve→delete cycle.

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
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	"github.com/elevran/charon/internal/metrics"
)

// fixture bundles the Charon API test server.
type fixture struct {
	srv *httptest.Server
}

func (f *fixture) URL() string { return f.srv.URL }

func newFixture(t *testing.T) *fixture {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	reg := prometheus.NewRegistry()
	require.NoError(t, metrics.Register(reg, ""))

	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	// Pass reg so chainstore metrics (chainstore_entries_total, etc.) are exported.
	svc, err := pebblebe.Open(context.Background(), "", opts, chainstore.Config{Registerer: reg})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	h := api.NewHandler(svc, log)

	mux := http.NewServeMux()
	api.RegisterHandlers(mux, h)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return &fixture{srv: srv}
}

func responseBlob(id, status string) []byte {
	b, _ := json.Marshal(map[string]interface{}{
		"id":     id,
		"status": status,
		"output": []interface{}{map[string]string{"type": "message", "role": "assistant"}},
	})
	return b
}

// storeRoot stores a root response via POST /responses (buffered path).
func storeRoot(t *testing.T, fx *fixture, id string, blob []byte) {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"response_id":   id,
		"response_blob": json.RawMessage(blob),
	})
	resp, err := http.Post(fx.URL()+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

// openStaging calls POST /staging?prev={prevID} and returns the staging ID.
func openStaging(t *testing.T, fx *fixture, prevID string) string {
	t.Helper()
	u := fx.URL() + "/staging"
	if prevID != "" {
		u += "?prev=" + prevID
	}
	resp, err := http.Post(u, "application/octet-stream", bytes.NewReader(nil))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
	var r struct {
		StagingID string `json:"staging_id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	return r.StagingID
}

// commitStaging writes blob as a single chunk then completes the staging record.
func commitStaging(t *testing.T, fx *fixture, stagingID, responseID string, blob []byte) {
	t.Helper()
	// PUT chunk 0
	chunkURL := fmt.Sprintf("%s/staging/%s/chunks/0?response_id=%s", fx.URL(), stagingID, responseID)
	req, _ := http.NewRequest(http.MethodPut, chunkURL, bytes.NewReader(blob))
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.True(t, resp.StatusCode == http.StatusAccepted || resp.StatusCode == http.StatusOK)

	// PUT complete
	commitURL := fmt.Sprintf("%s/staging/%s/complete?response_id=%s&total=%d",
		fx.URL(), stagingID, responseID, len(blob))
	req, _ = http.NewRequest(http.MethodPut, commitURL, nil)
	resp, err = http.DefaultClient.Do(req)
	require.NoError(t, err)
	resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)
}

// --- Tests ---

func TestMetricsEndpoint(t *testing.T) {
	fx := newFixture(t)
	resp, err := http.Get(fx.URL() + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	// The fixture registers chainstore metrics; any of the chainstore_* names must appear.
	assert.Contains(t, string(body), "chainstore_",
		"metrics response must contain registered application metrics")
}

func TestFullStoreCycle(t *testing.T) {
	fx := newFixture(t)
	blob := responseBlob("resp_smoke1", "completed")
	storeRoot(t, fx, "resp_smoke1", blob)

	getResp, err := http.Get(fx.URL() + "/responses/resp_smoke1")
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	body, _ := io.ReadAll(getResp.Body)
	assert.JSONEq(t, string(blob), string(body))
	assert.Equal(t, "0", getResp.Header.Get("X-Depth"))
}

func TestChainStoreAndResolve(t *testing.T) {
	fx := newFixture(t)

	// Store root
	b0 := responseBlob("resp_chain0", "completed")
	storeRoot(t, fx, "resp_chain0", b0)

	// Open staging for next turn — opens a staging record for the next turn,
	// returns 201 Created with Location: /staging/<id>.
	stagingID := openStaging(t, fx, "resp_chain0")
	assert.NotEmpty(t, stagingID)

	// Commit continuation via streaming path
	b1 := responseBlob("resp_chain1", "completed")
	commitStaging(t, fx, stagingID, "resp_chain1", b1)

	// Verify depth
	get1, err := http.Get(fx.URL() + "/responses/resp_chain1")
	require.NoError(t, err)
	defer get1.Body.Close()
	assert.Equal(t, "1", get1.Header.Get("X-Depth"))
}

func TestMultiTurnChain(t *testing.T) {
	fx := newFixture(t)

	const n = 5
	ids := make([]string, n)
	for i := range ids {
		ids[i] = fmt.Sprintf("resp_mt%02d", i)
	}

	// Store root
	b := responseBlob(ids[0], "completed")
	storeRoot(t, fx, ids[0], b)

	// Store continuation turns via staging path
	for i := 1; i < n; i++ {
		stagingID := openStaging(t, fx, ids[i-1])
		blob := responseBlob(ids[i], "completed")
		commitStaging(t, fx, stagingID, ids[i], blob)
	}

	// Verify depth of last turn
	get, err := http.Get(fx.URL() + "/responses/" + ids[n-1])
	require.NoError(t, err)
	defer get.Body.Close()
	assert.Equal(t, fmt.Sprintf("%d", n-1), get.Header.Get("X-Depth"))
}

func TestDeleteThenGet(t *testing.T) {
	fx := newFixture(t)
	blob := responseBlob("resp_del_intg", "completed")
	storeRoot(t, fx, "resp_del_intg", blob)

	delReq, _ := http.NewRequest(http.MethodDelete, fx.URL()+"/responses/resp_del_intg", nil)
	delResp, _ := http.DefaultClient.Do(delReq)
	delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	getResp, _ := http.Get(fx.URL() + "/responses/resp_del_intg")
	getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestTenantIsolation(t *testing.T) {
	fx := newFixture(t)
	blob := responseBlob("resp_iso_intg", "completed")

	body, _ := json.Marshal(map[string]interface{}{
		"response_id":   "resp_iso_intg",
		"response_blob": json.RawMessage(blob),
	})
	req, _ := http.NewRequest(http.MethodPost, fx.URL()+"/responses", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Tenant-Key", "alice")
	r, _ := http.DefaultClient.Do(req)
	r.Body.Close()

	// bob cannot see alice's entry
	getReq, _ := http.NewRequest(http.MethodGet, fx.URL()+"/responses/resp_iso_intg", nil)
	getReq.Header.Set("X-Tenant-Key", "bob")
	resp, _ := http.DefaultClient.Do(getReq)
	resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)

	// alice can see her own entry
	getReq2, _ := http.NewRequest(http.MethodGet, fx.URL()+"/responses/resp_iso_intg", nil)
	getReq2.Header.Set("X-Tenant-Key", "alice")
	resp2, _ := http.DefaultClient.Do(getReq2)
	resp2.Body.Close()
	assert.Equal(t, http.StatusOK, resp2.StatusCode)
}
