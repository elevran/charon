package disruptive_test

// Disruptive end-to-end tests exercise failure paths that span the full
// proxy → Charon → storage stack:
//
//  1. Mid-stream inference failure: the inference backend closes the SSE
//     stream before emitting response.completed.  The proxy must propagate
//     the truncation to the client and must NOT persist any partial record.
//
//  2. Store failure after non-streaming inference: Charon's store step fails
//     after the inference result has already been produced.  The proxy must
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
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apihandler "github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/proxy"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
	"github.com/elevran/charon/internal/testutil"
)

// fullStack holds the servers needed for end-to-end tests.
type fullStack struct {
	charonSrv *httptest.Server
	proxySrv  *httptest.Server
}

// startStack starts a Charon API server backed by the given stores, wires it
// to the given inference server, and returns the running stack.
func startStack(t *testing.T, idx storage.IndexStore, pay storage.PayloadStore, infSrv *httptest.Server) *fullStack {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	svc := store.New(idx, pay, store.Config{}, log)
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

// startNormalStack starts a stack with memory stores and the given inference server.
func startNormalStack(t *testing.T, infSrv *httptest.Server) *fullStack {
	t.Helper()
	return startStack(t, memory.NewIndexStore(), memory.NewPayloadStore(), infSrv)
}

// startFailingStoreStack starts a stack whose index rejects every index.Put,
// simulating a store failure after the payload write succeeds.
func startFailingStoreStack(t *testing.T, infSrv *httptest.Server) *fullStack {
	t.Helper()
	realIdx := memory.NewIndexStore()
	hookIdx := &testutil.HookIndexStore{
		IndexStore: realIdx,
		OnPut:      func(context.Context, model.ResponseMeta) error { return errStoreFailure },
	}
	return startStack(t, hookIdx, memory.NewPayloadStore(), infSrv)
}

// sseEventTypes reads an SSE response body and returns the "type" field of each
// data event in the order they were received.
func sseEventTypes(t *testing.T, body io.Reader) []string {
	t.Helper()
	var types []string
	scanner := bufio.NewScanner(body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		var evt struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal([]byte(strings.TrimPrefix(line, "data: ")), &evt); err != nil {
			continue
		}
		if evt.Type != "" {
			types = append(types, evt.Type)
		}
	}
	return types
}

// sseResponseID extracts the response ID from the first response.created event
// in an SSE event list, returning empty string if not found.
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

var errStoreFailure = errors.New("injected store failure")

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

	// The response must not be in Charon's index.
	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + canonicalID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be stored when inference stream was truncated")
}

// --- 2. Store failure after non-streaming inference ---

// TestNonStreamingStoreFailureIsNonFatal verifies that when Charon's store step
// fails after a successful non-streaming inference call:
//   - the proxy still returns 200 with the inference result
//   - the client receives the response ID in the body
//   - GET on that ID returns 404 (response was not persisted)
func TestNonStreamingStoreFailureIsNonFatal(t *testing.T) {
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

	// The proxy must return 200 — inference succeeded, storage failure is non-fatal.
	require.Equal(t, http.StatusOK, resp.StatusCode,
		"proxy must return 200 even when Charon store fails")

	var resource struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&resource))
	require.NotEmpty(t, resource.ID, "proxy must return the inference response ID")

	// Response must not be retrievable — the store failed.
	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + resource.ID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be accessible via GET after a store failure")
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

	// Response must not be retrievable — the store failed.
	getResp, err := http.Get(stack.charonSrv.URL + "/responses/" + canonicalID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be accessible via GET after a store failure")
}
