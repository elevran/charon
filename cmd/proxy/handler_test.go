package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateNewChain(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{"model": "test", "input": "hello"})
	defer resp.Body.Close()

	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r ResponseResource
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	assert.True(t, len(r.ID) > 0, "id must be non-empty")
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

func TestCreateMissingModel(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{"input": "hello"})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestCreatePreviousNotFound(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":                "test",
		"input":                "hi",
		"previous_response_id": "resp_unknown",
	})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	var errBody map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errBody))
	assert.Equal(t, "previous_response_not_found", errBody["code"])
}

func TestCreateContinuation(t *testing.T) {
	s := newTestStack(t)

	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{"model": "test", "input": "hello"})
	resource0 := decodeJSON[ResponseResource](t, r0)
	require.Equal(t, "completed", resource0.Status)

	r1 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":                "test",
		"input":                "follow up",
		"previous_response_id": resource0.ID,
	})
	resource1 := decodeJSON[ResponseResource](t, r1)
	require.Equal(t, http.StatusOK, r1.StatusCode)
	assert.Equal(t, "completed", resource1.Status)
	assert.NotEmpty(t, resource1.ID)
}

func TestRetrieve(t *testing.T) {
	s := newTestStack(t)

	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{"model": "test", "input": "hello"})
	resource0 := decodeJSON[ResponseResource](t, r0)

	r1 := doRequest(t, s.proxyURL, "GET", "/responses/"+resource0.ID, nil)
	resource1 := decodeJSON[ResponseResource](t, r1)
	assert.Equal(t, http.StatusOK, r1.StatusCode)
	assert.Equal(t, resource0.ID, resource1.ID)
}

func TestRetrieveNotFound(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "GET", "/responses/resp_missing", nil)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}

func TestDelete(t *testing.T) {
	s := newTestStack(t)

	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{"model": "test", "input": "hello"})
	resource0 := decodeJSON[ResponseResource](t, r0)

	delResp := doRequest(t, s.proxyURL, "DELETE", "/responses/"+resource0.ID, nil)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	getResp := doRequest(t, s.proxyURL, "GET", "/responses/"+resource0.ID, nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

func TestBackgroundFlagRoundtrip(t *testing.T) {
	s := newTestStack(t)

	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":      "test",
		"input":      "hello",
		"background": true,
	})
	resource0 := decodeJSON[ResponseResource](t, r0)
	require.Equal(t, http.StatusOK, r0.StatusCode)
	assert.True(t, resource0.Background, "immediate POST response must echo background:true")

	r1 := doRequest(t, s.proxyURL, "GET", "/responses/"+resource0.ID, nil)
	resource1 := decodeJSON[ResponseResource](t, r1)
	require.Equal(t, http.StatusOK, r1.StatusCode)
	assert.True(t, resource1.Background, "GET /responses/{id} must return background:true")
}

func TestStoreEquality(t *testing.T) {
	s := newTestStack(t)

	r0 := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello",
		"store": false,
	})
	resource0 := decodeJSON[ResponseResource](t, r0)
	require.Equal(t, http.StatusOK, r0.StatusCode)
	assert.False(t, resource0.Store)

	getResp := doRequest(t, s.proxyURL, "GET", "/responses/"+resource0.ID, nil)
	defer getResp.Body.Close()
	assert.Equal(t, http.StatusNotFound, getResp.StatusCode)
}

// TestStoreTrueContinuation verifies that a store:true turn can be
// continued with another store:true turn that chains to it via
// previous_response_id. The proxy hits Charon's POST /staging path on
// the continuation, which commits the request blob alongside the
// previous turn. The store:false -> store:true chaining pattern is
// pinned separately in backend_routing_test.go.
func TestStoreTrueContinuation(t *testing.T) {
	s := newTestStack(t)

	storeTrue := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "anchor",
	})
	anchor := decodeJSON[ResponseResource](t, storeTrue)
	require.Equal(t, http.StatusOK, storeTrue.StatusCode)

	// Continue from a stored turn; this exercises the proxy→Charon
	// POST /staging path which commits the request blob.
	cont := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":                "test",
		"input":                "follow",
		"previous_response_id": anchor.ID,
	})
	follow := decodeJSON[ResponseResource](t, cont)
	require.Equal(t, http.StatusOK, cont.StatusCode)
	assert.Equal(t, "completed", follow.Status)
	assert.NotEqual(t, anchor.ID, follow.ID)
}

// TestBufferedCapConfigurable verifies that a tiny MaxChunkBytes forces the
// buffered path to split a response blob into multiple AppendChunk calls.
func TestBufferedCapConfigurable(t *testing.T) {
	rec := &routingRecorder{}
	// 64-byte cap forces chunking: a typical storedResponse blob is >64 bytes.
	s := newTestStack(t, withMaxChunkBytes(64), withCharonMiddleware(rec.middleware()))

	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello world, this is a test request that should produce a response blob exceeding 64 bytes",
	})
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)

	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "/chunks/"), 2,
		"tiny cap must force ≥2 AppendChunk calls for a non-trivial response blob")
	assert.Equal(t, 1, hitsContaining(hits, "/complete"), "exactly 1 Complete call")
}

// TestCreateStoreFalseProduces200NoCommit ensures that the proxy
// doesn't 5xx on a store:false turn — the response is returned to the
// client and no Charon state is committed.
func TestCreateStoreFalseProduces200NoCommit(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "hello",
		"store": false,
	})
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	var r ResponseResource
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	assert.False(t, r.Store)
}
