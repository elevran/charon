package main

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
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/cmd/proxy/inference"
)

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

// --- 1. Mid-stream inference failure ---

// TestMidStreamInferenceFailureLeavesNoTrace verifies that when the inference
// backend closes the SSE stream after response.created (before response.completed):
//   - the proxy stream closes without a response.completed event
//   - no response record is persisted (GET on the canonical ID returns 404)
func TestMidStreamInferenceFailureLeavesNoTrace(t *testing.T) {
	partialSrv := inference.NewPartialMockServer()
	t.Cleanup(partialSrv.Close)

	s := newTestStack(t, withInferenceURL(partialSrv.URL))

	body, _ := json.Marshal(map[string]any{
		"model":  "mock",
		"stream": true,
		"input":  []map[string]string{{"type": "message", "role": "user", "content": "hi"}},
	})
	resp, err := http.Post(s.proxyURL+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode, "proxy must accept the streaming request")

	sse := readSSE(t, resp)
	canonicalID := sse.createdID()

	assert.NotEmpty(t, canonicalID, "proxy must emit response.created with a canonical ID")
	assert.Contains(t, sse.EventTypes, "response.created")
	assert.NotContains(t, sse.EventTypes, "response.completed",
		"proxy must NOT emit response.completed when the inference stream was truncated")

	getResp := doRequest(t, s.charonSrv.URL, "GET", "/responses/"+canonicalID, nil)
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

	s := newTestStack(t,
		withInferenceURL(mockInf.URL),
		withCharonMiddleware(failingCharonMux),
	)

	body, _ := json.Marshal(map[string]any{
		"model": "mock",
		"input": []map[string]string{{"type": "message", "role": "user", "content": "hi"}},
	})
	resp, err := http.Post(s.proxyURL+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)

	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode,
		"proxy must return 500 when Charon store fails")
	defer resp.Body.Close()

	// The mock inference server assigns IDs deterministically: "resp_" + 32-hex
	// zero-padded counter, starting at 1. For a fresh mock with one request the
	// canonical ID is always "resp_00000000000000000000000000000001".
	firstMockID := fmt.Sprintf("resp_%032x", mockInf.Calls())
	getResp := doRequest(t, s.charonSrv.URL, "GET", "/responses/"+firstMockID, nil)
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

	s := newTestStack(t,
		withInferenceURL(mockInf.URL),
		withCharonMiddleware(failingCharonMux),
	)

	body, _ := json.Marshal(map[string]any{
		"model":  "mock",
		"stream": true,
		"input":  []map[string]string{{"type": "message", "role": "user", "content": "hi"}},
	})
	resp, err := http.Post(s.proxyURL+"/responses", "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	sse := readSSE(t, resp)
	canonicalID := sse.createdID()

	require.NotEmpty(t, canonicalID, "proxy must emit response.created with a canonical ID")
	assert.Contains(t, sse.EventTypes, "response.created")
	assert.NotContains(t, sse.EventTypes, "response.completed",
		"proxy must NOT emit response.completed when Charon store fails")

	getResp := doRequest(t, s.charonSrv.URL, "GET", "/responses/"+canonicalID, nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode,
		"response must not be accessible via GET after a store failure")
}
