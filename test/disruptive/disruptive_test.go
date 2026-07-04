package disruptive_test

// Disruptive end-to-end tests exercise failure paths that span the full
// proxy → Charon → storage stack:
//
//  1. Mid-stream inference failure: the inference backend closes the SSE
//     stream before emitting response.completed.  The proxy must propagate
//     the truncation to the client and must NOT persist any partial record.
//
//  2. Store failure after non-streaming inference: the Charon server rejects
//     the POST /responses/{id} call with a 5xx.  The proxy must still
//     return the inference result to the client (non-fatal) while making the
//     response unretrievable via GET (not persisted).
//
//  3. Store failure after streaming inference: same contract as (2) but for
//     the SSE streaming path — client receives a complete response.completed
//     event but the response is not in Charon's index.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
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
	proxyH := proxy.NewHandler(charonClient, infClient, log, 0)
	proxyMux := http.NewServeMux()
	proxy.RegisterHandlers(proxyMux, proxyH)
	proxySrv := httptest.NewServer(proxyMux)
	t.Cleanup(proxySrv.Close)

	return &fullStack{charonSrv: charonSrv, proxySrv: proxySrv}
}

// failingCharonMux returns an http.Handler that returns 507 for all POST /responses/{id}
// (store) requests and delegates all other requests to the real Charon handler.
// Store calls are POST /responses/{id} — distinguished from resolve (POST /responses)
// by the presence of an additional path segment.
func failingCharonMux(realMux http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/responses/") {
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
	proxyH := proxy.NewHandler(charonClient, infClient, log, 0)
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
	defer resp.Body.Close()

	// Store failure is fatal: proxy returns 500 so the client knows the response
	// was not persisted and can retry, rather than believing it was stored.
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"proxy must return 500 when Charon store fails")
}

// --- 3. Store failure after streaming inference ---

// TestStreamingStoreFailureIsNonFatal verifies that when Charon's store step
// fails after a successful streaming inference call:
//   - the client receives the complete SSE stream including response.completed
//   - GET on the response ID returns 404 (response was not persisted)
func TestStreamingStoreFailureIsNonFatal(t *testing.T) {
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
	assert.Contains(t, eventTypes, "response.completed",
		"client must receive response.completed even when Charon store fails")

	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + canonicalID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be accessible via GET after a store failure")
}
