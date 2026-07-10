package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestChunkedRoundtripReassemblesIntoChain closes the test gap between the
// chunked-staging wire pattern (asserted by backend_routing_test.go) and the
// downstream chain-serve path: a response saved via PUT /staging/{sid}/chunks/{k}
// → PUT /staging/{sid}/complete must be reassembled by Charon into a single
// entry, and that entry must be served back as part of the chained context
// for any subsequent turn that references the anchor via previous_response_id.
//
// The proxy builds the chained context in turnsToFlatCtx and prepends it to
// the current turn's input before forwarding to the inference backend (see
// buildInferenceMap in assemble.go). So the strongest end-to-end assertion is
// that the inference call for the continuation sees the anchor's response
// output (msg_ok) somewhere in its input array — proving the chunks were
// reassembled into a chain entry that the proxy can serve.
//
// Pairs with:
//   - cmd/proxy/chunk_test.go: unit-level chunk → chunked byte round-trip.
//   - internal/chainstore/streaming_test.go: server-side chunk reassembly.
//   - cmd/proxy/backend_routing_test.go: wire-pattern pinning (no byte check).
func TestChunkedRoundtripReassemblesIntoChain(t *testing.T) {
	s, rec := newRoutingStack(t, withMaxChunkBytes(64))

	// Step 1: anchor turn. With maxChunkBytes=64 the stored response blob
	// (mock returns ~140-byte output) splits across multiple AppendChunk calls.
	anchorResp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "anchor turn input",
	})
	anchor := decodeJSON[ResponseResource](t, anchorResp)
	require.Equal(t, http.StatusOK, anchorResp.StatusCode)

	// Step 2: wire-pattern check — confirm the anchor really was chunked,
	// not just a single PUT that happened to fit.
	hits := rec.snapshot()
	assert.GreaterOrEqual(t, hitsContaining(hits, "/chunks/"), 2,
		"tiny cap must force ≥2 AppendChunk calls for a non-trivial stored blob")
	assert.Equal(t, 1, hitsContaining(hits, "/complete"),
		"exactly 1 Complete call on success")
	assert.Equal(t, 0, hitsContaining(hits, "/abort"),
		"no abort on success")

	// Step 3: continuation. The proxy's HandleCreate calls hydrateContext,
	// which fetches the chain rooted at anchor.ID and produces a flatCtx.
	// buildInferenceMap prepends flatCtx to the new input items before the
	// inference call.
	contResp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":                "test",
		"input":                "follow up input",
		"previous_response_id": anchor.ID,
	})
	follow := decodeJSON[ResponseResource](t, contResp)
	require.Equal(t, http.StatusOK, contResp.StatusCode)
	require.Equal(t, "completed", follow.Status)

	// Step 4: capture the inference backend's request bodies via the mock.
	// Bodies arrive in order: anchor turn (call 1), continuation turn (call 2).
	bodies := s.mockInf.RequestBodies()
	require.GreaterOrEqual(t, len(bodies), 2,
		"both anchor and continuation should have hit the inference backend")

	// Step 5: assert the continuation's inference call carried the anchor's
	// output item in its input array. The MockServer returns a single item
	// with id="msg_ok" and role="assistant" — distinguishing it from the
	// user-role input items the proxy assembles around it.
	var infReq struct {
		Input json.RawMessage `json:"input"`
	}
	require.NoError(t, json.Unmarshal(bodies[1], &infReq))

	var inputItems []map[string]interface{}
	require.NoError(t, json.Unmarshal(infReq.Input, &inputItems),
		"continuation input must be a JSON array (stringified for stateless inference)")

	// Locate the anchor's response output in the assembled input array.
	found := false
	for _, item := range inputItems {
		if id, _ := item["id"].(string); id == "msg_ok" {
			role, _ := item["role"].(string)
			assert.Equal(t, "assistant", role,
				"anchor's msg_ok must appear as an assistant item in the chained context")
			found = true
			break
		}
	}
	assert.True(t, found,
		"continuation's inference call must include the anchor's response output "+
			"(msg_ok) in its input — proves the chunked save was reassembled into "+
			"a chain entry the proxy can serve back")

	// Sanity check: both turn inputs should also be present, in the right order.
	// Flat context order (per turnsToFlatCtx): turn input, turn output, turn input, turn output, ...
	// Combined order (per buildInferenceMap): flatCtx, then new input items.
	// So expected: [anchor_input, msg_ok(anchor), follow_input].
	assert.GreaterOrEqual(t, len(inputItems), 3,
		"chained input must contain anchor input + anchor output + continuation input")
}
