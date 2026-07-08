package disruptive_test

// Disruptive end-to-end tests exercise failure paths that span the full
// proxy → Charon → storage stack:
//
//  1. Mid-stream inference failure: the inference backend closes the SSE
//     stream before emitting response.completed.  The proxy must propagate
//     the truncation to the client and must NOT persist any partial record.
//
//  2. Store failure after non-streaming inference: the proxy returns 500 so
//     the client knows the response was not persisted and can retry.  GET on
//     the response ID returns 404.
//
//  3. Store failure after streaming inference: the proxy omits
//     response.completed so the client knows storage failed.  GET on the
//     response ID returns 404.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
	pebblebe "github.com/elevran/charon/internal/chainstore/pebble"
	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/proxy"

	apihandler "github.com/elevran/charon/internal/api"
)

// fullStack holds the servers needed for end-to-end tests.
type fullStack struct {
	charonSrv *httptest.Server
	proxySrv  *httptest.Server
}

// startNormalStack starts a stack backed by an in-memory pebble store.
func startNormalStack(t *testing.T, infSrv *httptest.Server) *fullStack {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	svc, err := pebblebe.Open(context.Background(), "", opts, chainstore.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	charonH := apihandler.NewHandler(svc, log)
	charonMux := http.NewServeMux()
	apihandler.RegisterHandlers(charonMux, charonH)
	charonSrv := httptest.NewServer(charonMux)
	t.Cleanup(charonSrv.Close)

	charonClient := charon.New(charonSrv.URL, 5*time.Second)
	infClient := inference.New(infSrv.URL, "", 5*time.Second)
	proxyH := proxy.NewHandler(charonClient, infClient, log)
	proxyMux := http.NewServeMux()
	proxy.RegisterHandlers(proxyMux, proxyH)
	proxySrv := httptest.NewServer(proxyMux)
	t.Cleanup(proxySrv.Close)

	return &fullStack{charonSrv: charonSrv, proxySrv: proxySrv}
}

// failingCharonMux returns an http.Handler that returns 507 for all PUT
// /staging/{id}/chunks/* and PUT /staging/{id}/complete requests (the two
// calls that make up a Store) and delegates everything else to the real handler.
func failingCharonMux(realMux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut && strings.HasPrefix(r.URL.Path, "/staging/") {
			http.Error(w, "injected store failure", http.StatusInsufficientStorage)
			return
		}
		realMux.ServeHTTP(w, r)
	})
}

// startFailingStoreStack starts a stack whose Charon server rejects POST .../store.
func startFailingStoreStack(t *testing.T, infSrv *httptest.Server) *fullStack {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	svc, err := pebblebe.Open(context.Background(), "", opts, chainstore.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = svc.Close() })

	charonH := apihandler.NewHandler(svc, log)
	realMux := http.NewServeMux()
	apihandler.RegisterHandlers(realMux, charonH)
	charonSrv := httptest.NewServer(failingCharonMux(realMux))
	t.Cleanup(charonSrv.Close)

	charonClient := charon.New(charonSrv.URL, 5*time.Second)
	infClient := inference.New(infSrv.URL, "", 5*time.Second)
	proxyH := proxy.NewHandler(charonClient, infClient, log)
	proxyMux := http.NewServeMux()
	proxy.RegisterHandlers(proxyMux, proxyH)
	proxySrv := httptest.NewServer(proxyMux)
	t.Cleanup(proxySrv.Close)

	return &fullStack{charonSrv: charonSrv, proxySrv: proxySrv}
}

// sseResponseID extracts the response ID from the first response.created event
// in an SSE stream and returns all event types seen.
func sseResponseID(t *testing.T, body io.Reader) (string, []string) {
	t.Helper()
	var types []string
	var id string
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		var evt struct {
			Type     string `json:"type"`
			Response *struct {
				ID string `json:"id"`
			} `json:"response"`
		}
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			continue
		}
		if evt.Type != "" {
			types = append(types, evt.Type)
		}
		if evt.Type == "response.created" && evt.Response != nil && id == "" {
			id = evt.Response.ID
		}
	}
	return id, types
}

// --- 1. Mid-stream inference failure ---

// TestMidStreamInferenceFailureLeavesNoTrace verifies that when the inference
// backend closes the SSE stream after response.created (before response.completed):
//   - the proxy stream closes without a response.completed event
//   - no response record is persisted (GET on the canonical ID returns 404)
func TestMidStreamInferenceFailureLeavesNoTrace(t *testing.T) {
	partialSrv := inference.NewPartialMockServer()
	t.Cleanup(partialSrv.Close)

	stack := startNormalStack(t, partialSrv.Server)

	body, _ := json.Marshal(map[string]any{
		"model":  "mock",
		"stream": true,
		"input":  []map[string]string{{"type": "message", "role": "user", "content": "hi"}},
	})
	resp, err := http.Post(stack.proxySrv.URL+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "proxy must accept the streaming request")

	canonicalID, eventTypes := sseResponseID(t, resp.Body)

	assert.NotEmpty(t, canonicalID, "proxy must emit response.created with a canonical ID")
	assert.Contains(t, eventTypes, "response.created")
	assert.NotContains(t, eventTypes, "response.completed",
		"proxy must NOT emit response.completed when the inference stream was truncated")

	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + canonicalID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be stored when inference stream was truncated")
}

// --- 2. Store failure after non-streaming inference ---

// TestNonStreamingStoreFailureIsFatal verifies that when Charon's store step
// fails after a successful non-streaming inference call:
//   - the proxy returns 500 (store failure is fatal — response cannot be retrieved)
//   - GET on the response ID returns 404 (response was not persisted)
func TestNonStreamingStoreFailureIsFatal(t *testing.T) {
	mockInf := inference.NewMockServer()
	t.Cleanup(mockInf.Close)

	stack := startFailingStoreStack(t, mockInf.Server)

	body, _ := json.Marshal(map[string]any{
		"model": "mock",
		"input": []map[string]string{{"type": "message", "role": "user", "content": "hi"}},
	})
	resp, err := http.Post(stack.proxySrv.URL+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)

	// Store failure is fatal: proxy returns 500 so the client knows the response
	// was not persisted and can retry, rather than believing it was stored.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"proxy must return 500 when Charon store fails")
	defer resp.Body.Close()

	// The mock inference server assigns IDs deterministically: "resp_" + 32-hex
	// zero-padded counter, starting at 1.  For a fresh mock with one request the
	// canonical ID is always "resp_00000000000000000000000000000001".
	firstMockID := fmt.Sprintf("resp_%032x", mockInf.Calls())
	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + firstMockID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be persisted when store fails")
}

// --- 3. Store failure after streaming inference ---

// TestStreamingStoreFailureIsFatal verifies that when Charon's store step
// fails after a successful streaming inference call:
//   - the proxy omits response.completed (client must not believe the response was persisted)
//   - GET on the response ID returns 404 (response was not persisted)
func TestStreamingStoreFailureIsFatal(t *testing.T) {
	mockInf := inference.NewMockServer()
	t.Cleanup(mockInf.Close)

	stack := startFailingStoreStack(t, mockInf.Server)

	body, _ := json.Marshal(map[string]any{
		"model":  "mock",
		"stream": true,
		"input":  []map[string]string{{"type": "message", "role": "user", "content": "hi"}},
	})
	resp, err := http.Post(stack.proxySrv.URL+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	canonicalID, eventTypes := sseResponseID(t, resp.Body)

	require.NotEmpty(t, canonicalID, "proxy must emit response.created with a canonical ID")
	assert.Contains(t, eventTypes, "response.created")
	assert.NotContains(t, eventTypes, "response.completed",
		"proxy must NOT emit response.completed when Charon store fails")

	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + canonicalID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be accessible via GET after a store failure")
}
