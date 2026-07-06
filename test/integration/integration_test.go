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
	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	svc, err := pebblebe.Open(context.Background(), "", opts, chainstore.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })
	h := api.NewHandler(svc, log)

	reg := prometheus.NewRegistry()
	require.NoError(t, metrics.Register(reg, ""))

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

// --- Tests ---

func TestMetricsEndpoint(t *testing.T) {
	fx := newFixture(t)
	resp, err := http.Get(fx.URL() + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, _ := io.ReadAll(resp.Body)
	// The fixture registers application metrics; any of the responses_* names must appear.
	assert.Contains(t, string(body), "responses_",
		"metrics response must contain registered application metrics")
}

func TestFullStoreCycle(t *testing.T) {
	fx := newFixture(t)
	blob := responseBlob("resp_smoke1", "completed")

	storeResp, err := http.Post(fx.URL()+"/responses/resp_smoke1",
		"application/octet-stream", bytes.NewReader(blob))
	require.NoError(t, err)
	defer storeResp.Body.Close()
	assert.Equal(t, http.StatusOK, storeResp.StatusCode)

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
	r0, err := http.Post(fx.URL()+"/responses/resp_chain0",
		"application/octet-stream", bytes.NewReader(b0))
	require.NoError(t, err)
	defer r0.Body.Close()
	require.Equal(t, http.StatusOK, r0.StatusCode)

	// Resolve continuation — opens a staging record for the next turn, so the
	// response is 201 Created with Location: /responses/staging/<id>.
	resolveResp, err := http.Post(fx.URL()+"/responses?prev=resp_chain0",
		"application/octet-stream", bytes.NewReader(nil))
	require.NoError(t, err)
	defer resolveResp.Body.Close()
	require.Equal(t, http.StatusCreated, resolveResp.StatusCode)

	var resolved struct {
		StagingID string            `json:"staging_id"`
		Turns     []json.RawMessage `json:"turns"`
	}
	require.NoError(t, json.NewDecoder(resolveResp.Body).Decode(&resolved))
	assert.NotEmpty(t, resolved.StagingID)
	assert.Len(t, resolved.Turns, 1)

	// Store continuation
	b1 := responseBlob("resp_chain1", "completed")
	req, _ := http.NewRequest(http.MethodPost,
		fx.URL()+"/responses/resp_chain1?req="+resolved.StagingID,
		bytes.NewReader(b1))
	r1, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer r1.Body.Close()
	assert.Equal(t, http.StatusOK, r1.StatusCode)

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
	r, _ := http.Post(fx.URL()+"/responses/"+ids[0],
		"application/octet-stream", bytes.NewReader(b))
	r.Body.Close()
	require.Equal(t, http.StatusOK, r.StatusCode)

	// Store continuation turns
	for i := 1; i < n; i++ {
		resolveResp, _ := http.Post(fx.URL()+"/responses?prev="+ids[i-1],
			"application/octet-stream", bytes.NewReader(nil))
		var resolved struct {
			StagingID string `json:"staging_id"`
		}
		json.NewDecoder(resolveResp.Body).Decode(&resolved)
		resolveResp.Body.Close()

		blob := responseBlob(ids[i], "completed")
		req, _ := http.NewRequest(http.MethodPost,
			fx.URL()+"/responses/"+ids[i]+"?req="+resolved.StagingID,
			bytes.NewReader(blob))
		storeResp, _ := http.DefaultClient.Do(req)
		storeResp.Body.Close()
		require.Equal(t, http.StatusOK, storeResp.StatusCode, "turn %d", i)
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

	r, _ := http.Post(fx.URL()+"/responses/resp_del_intg",
		"application/octet-stream", bytes.NewReader(blob))
	r.Body.Close()

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

	req, _ := http.NewRequest(http.MethodPost, fx.URL()+"/responses/resp_iso_intg",
		bytes.NewReader(blob))
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
