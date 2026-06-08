package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/openai/openai-go/responses"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

func startServer(t *testing.T) *httptest.Server {
	t.Helper()
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := store.New(idx, pay, store.Config{}, log)
	h := api.NewHandler(svc, log)

	reg := prometheus.NewRegistry()
	require.NoError(t, metrics.Register(reg, ""))

	mux := http.NewServeMux()
	api.RegisterHandlers(mux, h)
	mux.Handle("GET /metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
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
