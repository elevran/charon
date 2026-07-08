package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// modelsResponse matches the minimal OpenAI /v1/models response shape.
type modelsResponse struct {
	Object string `json:"object"`
	Data   []struct {
		ID     string `json:"id"`
		Object string `json:"object"`
	} `json:"data"`
}

// TestPassthroughGetModels verifies that GET /v1/models is forwarded to the
// inference backend and the response is passed through verbatim.
func TestPassthroughGetModels(t *testing.T) {
	// Build a custom inference server that handles /v1/models in addition to
	// the standard POST /responses expected by the proxy stack.
	infMux := http.NewServeMux()

	// Minimal /v1/models response.
	infMux.HandleFunc("GET /v1/models", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(modelsResponse{
			Object: "list",
			Data: []struct {
				ID     string `json:"id"`
				Object string `json:"object"`
			}{
				{ID: "mock-model", Object: "model"},
			},
		})
	})

	// POST /responses is required so the proxy's infClient.Complete calls work
	// (e.g. during other tests in this package that share a server). A 501
	// is fine here because this test only calls /v1/models.
	infMux.HandleFunc("POST /responses", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "not implemented in passthrough test server", http.StatusNotImplemented)
	})

	infSrv := httptest.NewServer(infMux)
	t.Cleanup(infSrv.Close)

	s := newTestStack(t, withInferenceURL(infSrv.URL))

	resp := doRequest(t, s.proxyURL, "GET", "/v1/models", nil)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var got modelsResponse
	require.NoError(t, json.Unmarshal(body, &got))
	assert.Equal(t, "list", got.Object)
	require.Len(t, got.Data, 1)
	assert.Equal(t, "mock-model", got.Data[0].ID)
}
