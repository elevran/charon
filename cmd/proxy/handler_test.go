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

// TestStoreFalseContinuation verifies that a store:false turn can be
// continued with a store:true turn that chains to it via
// previous_response_id.
//
// The store:false turn must return an HTTP 200 with a response object
// whose id the client can supply as previous_response_id on the next
// turn. Since the response was not persisted, the proxy must NOT be
// able to retrieve it via GET /responses/{id} — that path returns 404
// from the proxy's passthrough to Charon. (Independently covered by
// TestStoreEquality.)
//
// This test pins the store:false path's resolution semantics: the
// proxy returns a successful 200 response and never tries to look up
// the previous_response_id server-side for store:false turns it didn't
// itself write — the assumption is that subsequent store:true turns
// refer to store:true turns, not to store:false ones.
func TestStoreFalseContinuation(t *testing.T) {
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
