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
