package api_test

// Proxy end-to-end tests simulate the three operations a proxy performs:
//
//  1. Store a completed LLM response          POST /responses/{id}
//  2. Resolve previous context for next turn  GET  /responses/{id}/context
//  3. Retrieve or delete a stored response    GET/DELETE /responses/{id}
//
// Tests use a real TCP httptest.Server so the full HTTP stack (routing,
// middleware, serialisation) is exercised.  In-memory stores keep tests fast
// and hermetic.

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

// newE2EServer wires the full stack and starts a real TCP listener.
func newE2EServer(t *testing.T) (*httptest.Server, *http.Client) {
	t.Helper()
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := store.New(idx, pay, store.Config{CheckpointInterval: 10}, log)
	h := api.NewHandler(svc, log)

	mux := http.NewServeMux()
	api.RegisterHandlers(mux, h)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, srv.Client()
}

// --- proxy helpers (mirror what the real proxy does) ---

func storeResponse(t *testing.T, client *http.Client, base, responseID string, req model.StoreRequest) {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)

	resp, err := client.Post(base+"/responses/"+responseID, "application/json", bytes.NewReader(body))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "POST /responses/%s", responseID)
}

func resolveContext(t *testing.T, client *http.Client, base, previousID string) model.ResolveResponse {
	t.Helper()
	resp, err := client.Get(base + "/responses/" + previousID + "/context")
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /responses/%s/context", previousID)

	var resolved model.ResolveResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&resolved))
	require.NotEmpty(t, resolved.ResponseID, "server must mint a new response_id")
	return resolved
}

func retrieveResponse(t *testing.T, client *http.Client, base, responseID string) model.RetrieveResponse {
	t.Helper()
	resp, err := client.Get(base + "/responses/" + responseID)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode, "GET /responses/%s", responseID)

	var retrieved model.RetrieveResponse
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&retrieved))
	return retrieved
}

func deleteResponse(t *testing.T, client *http.Client, base, responseID string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodDelete, base+"/responses/"+responseID, nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusNoContent, resp.StatusCode, "DELETE /responses/%s", responseID)
}

func item(role, text string) json.RawMessage {
	b, _ := json.Marshal(map[string]string{"type": "message", "role": role, "text": text})
	return b
}

func inputItem(role, text string) responses.ResponseInputItemUnionParam {
	var p responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(item(role, text), &p)
	return p
}

// --- E2E scenarios ---

// TestProxyNewChain verifies the proxy can store the first turn of a new chain.
func TestProxyNewChain(t *testing.T) {
	srv, client := newE2EServer(t)

	req := model.StoreRequest{
		Input:  responses.ResponseInputParam{inputItem("user", "hello")},
		Output: []json.RawMessage{item("assistant", "hi")},
		Status: "completed",
		Model:  "test-model",
	}
	storeResponse(t, client, srv.URL, "resp_chain0_t0", req)

	retrieved := retrieveResponse(t, client, srv.URL, "resp_chain0_t0")
	assert.Equal(t, "resp_chain0_t0", retrieved.ID)
	assert.Equal(t, responses.ResponseStatusCompleted, retrieved.Status)
	assert.Equal(t, "test-model", retrieved.Model)
	assert.Len(t, retrieved.Input, 1)
	assert.Len(t, retrieved.Output, 1)
}

// TestProxyMultiTurnChain validates the full proxy loop across three turns:
//
//   proxy loop per turn:
//     1. GET /responses/{prev_id}/context  → mint new id + flat_context
//     2. POST /responses/{new_id}          → store result
//
// After each turn the flat_context must grow by exactly 2 items (1 input + 1 output).
func TestProxyMultiTurnChain(t *testing.T) {
	srv, client := newE2EServer(t)

	// Turn 0: new chain — no resolve, proxy stores directly.
	t0req := model.StoreRequest{
		Input:  responses.ResponseInputParam{inputItem("user", "turn 0")},
		Output: []json.RawMessage{item("assistant", "turn 0 reply")},
		Status: "completed",
		Model:  "test-model",
	}
	const t0ID = "resp_multi_t0"
	storeResponse(t, client, srv.URL, t0ID, t0req)

	// Turn 1: proxy resolves from t0, stores with the minted id.
	resolved1 := resolveContext(t, client, srv.URL, t0ID)
	assert.Len(t, resolved1.FlatContext, 2, "turn 1 context: t0 input + t0 output")

	t1ID := resolved1.ResponseID
	prevID := t0ID
	t1req := model.StoreRequest{
		PreviousResponseID: &prevID,
		Input:              responses.ResponseInputParam{inputItem("user", "turn 1")},
		Output:             []json.RawMessage{item("assistant", "turn 1 reply")},
		Status:             "completed",
		Model:              "test-model",
	}
	storeResponse(t, client, srv.URL, t1ID, t1req)

	// Turn 2: proxy resolves from t1.
	resolved2 := resolveContext(t, client, srv.URL, t1ID)
	assert.Len(t, resolved2.FlatContext, 4, "turn 2 context: t0 + t1 (2 items each)")

	t2ID := resolved2.ResponseID
	prev1ID := t1ID
	t2req := model.StoreRequest{
		PreviousResponseID: &prev1ID,
		Input:              responses.ResponseInputParam{inputItem("user", "turn 2")},
		Output:             []json.RawMessage{item("assistant", "turn 2 reply")},
		Status:             "completed",
		Model:              "test-model",
	}
	storeResponse(t, client, srv.URL, t2ID, t2req)

	// Resolve from t2 should include all three turns.
	resolved3 := resolveContext(t, client, srv.URL, t2ID)
	assert.Len(t, resolved3.FlatContext, 6, "turn 3 context: t0 + t1 + t2 (2 items each)")
}

// TestProxyFailedInferenceAndBrokenChain covers two failure modes:
//
//  1. A failed LLM inference is stored with status="failed".
//     The proxy can still retrieve it; subsequent turns treat it as a dead end.
//
//  2. A mid-chain response is deleted (e.g. point-deleted by an operator).
//     Any resolve that walks through the gap returns 500 with X-Charon-Error:
//     chain_corrupted.
func TestProxyFailedInferenceAndBrokenChain(t *testing.T) {
	srv, client := newE2EServer(t)

	// Build a 3-turn chain: t0 → t1 → t2.
	const t0ID = "resp_broken_t0"
	storeResponse(t, client, srv.URL, t0ID, model.StoreRequest{
		Input:  responses.ResponseInputParam{inputItem("user", "turn 0")},
		Output: []json.RawMessage{item("assistant", "turn 0 reply")},
		Status: "completed",
	})

	res1 := resolveContext(t, client, srv.URL, t0ID)
	t1ID := res1.ResponseID
	prev0 := t0ID
	storeResponse(t, client, srv.URL, t1ID, model.StoreRequest{
		PreviousResponseID: &prev0,
		Input:              responses.ResponseInputParam{inputItem("user", "turn 1")},
		Output:             []json.RawMessage{item("assistant", "turn 1 reply")},
		Status:             "completed",
	})

	res2 := resolveContext(t, client, srv.URL, t1ID)
	t2ID := res2.ResponseID
	prev1 := t1ID
	storeResponse(t, client, srv.URL, t2ID, model.StoreRequest{
		PreviousResponseID: &prev1,
		Input:              responses.ResponseInputParam{inputItem("user", "turn 2")},
		Output:             []json.RawMessage{item("assistant", "turn 2 reply")},
		Status:             "completed",
	})

	// Scenario A: failed inference stored after t2.
	res3 := resolveContext(t, client, srv.URL, t2ID)
	t3FailedID := res3.ResponseID
	prev2 := t2ID
	storeResponse(t, client, srv.URL, t3FailedID, model.StoreRequest{
		PreviousResponseID: &prev2,
		Input:              responses.ResponseInputParam{inputItem("user", "turn 3")},
		Output:             nil,
		Status:             "failed",
	})

	retrieved := retrieveResponse(t, client, srv.URL, t3FailedID)
	assert.Equal(t, responses.ResponseStatusFailed, retrieved.Status, "failed response must be retrievable with correct status")

	// Scenario B: delete t1 (mid-chain), then attempt resolve from t2.
	deleteResponse(t, client, srv.URL, t1ID)

	// t2's chain walks: t2 → t1 (deleted) → chain corrupted.
	req, err := http.NewRequest(http.MethodGet, srv.URL+"/responses/"+t2ID+"/context", nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusConflict, resp.StatusCode, "broken chain must return 409")

	var errBody map[string]string
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&errBody))
	assert.Equal(t, "chain corrupted", errBody["error"])
}
