package proxy_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apihandler "github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/proxy"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

// stack holds the full test infrastructure.
type stack struct {
	charonSrv *httptest.Server
	mockInf   *inference.MockServer
	proxySrv  *httptest.Server
}

func startStack(t *testing.T) *stack {
	t.Helper()
	// Charon internal API
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := store.New(idx, pay, store.Config{}, log)
	charonH := apihandler.NewHandler(svc, log)
	charonMux := http.NewServeMux()
	apihandler.RegisterHandlers(charonMux, charonH)
	charonSrv := httptest.NewServer(apihandler.WrapH2c(charonMux))
	t.Cleanup(charonSrv.Close)

	// Mock inference
	mockInf := inference.NewMockServer()
	t.Cleanup(mockInf.Close)

	// Proxy
	charonClient := charon.New(charonSrv.URL, 5*time.Second)
	infClient := inference.New(mockInf.URL, "", 5*time.Second)
	proxyH := proxy.NewHandler(charonClient, infClient, log, 0)
	proxyMux := http.NewServeMux()
	proxy.RegisterHandlers(proxyMux, proxyH)
	proxySrv := httptest.NewServer(proxyMux)
	t.Cleanup(proxySrv.Close)

	return &stack{charonSrv: charonSrv, mockInf: mockInf, proxySrv: proxySrv}
}

func doRequest(t *testing.T, baseURL, method, path string, body any) *http.Response {
	t.Helper()
	var r io.Reader
	if body != nil {
		b, _ := json.Marshal(body)
		r = bytes.NewReader(b)
	}
	req, _ := http.NewRequestWithContext(context.Background(), method, baseURL+path, r)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func decodeBody[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&v))
	return v
}

// --- Tests ---

func TestCreateNewChain(t *testing.T) {
	s := startStack(t)
	body := map[string]interface{}{"model": "test", "input": "hello"}
	resp := doRequest(t, s.proxySrv.URL, "POST", "/responses", body)
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r proxy.ResponseResource
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	assert.True(t, len(r.ID) > 0, "id must be non-empty")
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

func TestCreateMissingModel(t *testing.T) {
	s := startStack(t)
	body := map[string]interface{}{"input": "hello"}
	resp := doRequest(t, s.proxySrv.URL, "POST", "/responses", body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCreatePreviousNotFound(t *testing.T) {
	s := startStack(t)
	prevID := "resp_unknown"
	body := map[string]interface{}{
		"model":                "test",
		"input":                "hi",
		"previous_response_id": prevID,
	}
	resp := doRequest(t, s.proxySrv.URL, "POST", "/responses", body)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var errBody map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errBody))
	assert.Equal(t, "previous_response_not_found", errBody["code"])
}

func TestCreateContinuation(t *testing.T) {
	s := startStack(t)

	// Turn 0
	r0 := doRequest(t, s.proxySrv.URL, "POST", "/responses",
		map[string]interface{}{"model": "test", "input": "hello"})
	resource0 := decodeBody[proxy.ResponseResource](t, r0)
	require.Equal(t, "completed", resource0.Status)

	// Turn 1 continuing from turn 0
	r1 := doRequest(t, s.proxySrv.URL, "POST", "/responses",
		map[string]interface{}{
			"model":                "test",
			"input":                "follow up",
			"previous_response_id": resource0.ID,
		})
	resource1 := decodeBody[proxy.ResponseResource](t, r1)
	require.Equal(t, http.StatusOK, r1.StatusCode)
	assert.Equal(t, "completed", resource1.Status)
	assert.NotEmpty(t, resource1.ID)
}

func TestRetrieve(t *testing.T) {
	s := startStack(t)

	r0 := doRequest(t, s.proxySrv.URL, "POST", "/responses",
		map[string]interface{}{"model": "test", "input": "hello"})
	resource0 := decodeBody[proxy.ResponseResource](t, r0)

	r1 := doRequest(t, s.proxySrv.URL, "GET", "/responses/"+resource0.ID, nil)
	resource1 := decodeBody[proxy.ResponseResource](t, r1)
	assert.Equal(t, http.StatusOK, r1.StatusCode)
	assert.Equal(t, resource0.ID, resource1.ID)
}

func TestRetrieveNotFound(t *testing.T) {
	s := startStack(t)
	resp := doRequest(t, s.proxySrv.URL, "GET", "/responses/resp_missing", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDelete(t *testing.T) {
	s := startStack(t)

	r0 := doRequest(t, s.proxySrv.URL, "POST", "/responses",
		map[string]interface{}{"model": "test", "input": "hello"})
	resource0 := decodeBody[proxy.ResponseResource](t, r0)

	delResp := doRequest(t, s.proxySrv.URL, "DELETE", "/responses/"+resource0.ID, nil)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	getResp := doRequest(t, s.proxySrv.URL, "GET", "/responses/"+resource0.ID, nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestCompactMissingModel(t *testing.T) {
	s := startStack(t)
	resp := doRequest(t, s.proxySrv.URL, "POST", "/responses/compact",
		map[string]interface{}{"input": []interface{}{}})
	defer resp.Body.Close()
	assert.True(t, resp.StatusCode == 400 || resp.StatusCode == 422,
		"expected 400 or 422, got %d", resp.StatusCode)
}

func TestStoreEquality(t *testing.T) {
	s := startStack(t)

	// store:false — should NOT be retrievable from Charon
	storeFalse := false
	body := map[string]interface{}{
		"model": "test",
		"input": "hello",
		"store": storeFalse,
	}
	r0 := doRequest(t, s.proxySrv.URL, "POST", "/responses", body)
	resource0 := decodeBody[proxy.ResponseResource](t, r0)
	require.Equal(t, http.StatusOK, r0.StatusCode)
	assert.False(t, resource0.Store)

	// Should not be in Charon
	getResp := doRequest(t, s.proxySrv.URL, "GET", "/responses/"+resource0.ID, nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}
