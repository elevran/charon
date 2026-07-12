package main

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inputItem is the subset of fields we assert on for an item in the chained
// context. The full shape is open-ended (user messages, assistant messages,
// tool calls, ...) so we only decode what we check.
type inputItem struct {
	ID   string          `json:"id,omitempty"`
	Role string          `json:"role,omitempty"`
	Type string          `json:"type,omitempty"`
	Text json.RawMessage `json:"content,omitempty"` // string for user, []item for assistant
}

// TestChunkedRoundtripReassemblesIntoChain closes the test gap between the
// chunked-staging wire pattern (asserted by backend_routing_test.go, in
// particular TestBufferedProxyMultipleChunks) and the downstream chain-serve
// path: a response saved via PUT /staging/{sid}/chunks/{k} →
// PUT /staging/{sid}/complete must be reassembled by Charon into a single
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
	// maxChunkBytes=64 forces ≥2 AppendChunk calls for a non-trivial stored
	// blob. The wire pattern itself is pinned by TestBufferedProxyMultipleChunks
	// — this test focuses on the chain-serve property.
	s := newTestStack(t, withMaxChunkBytes(64))

	// Anchor turn — store:true, multi-chunk save under the hood.
	anchorHTTP := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": "anchor turn input",
	})
	require.Equal(t, http.StatusOK, anchorHTTP.StatusCode,
		"anchor turn must succeed before we can chain off it")
	anchor := decodeJSON[ResponseResource](t, anchorHTTP)

	// Continuation — references the anchor via previous_response_id.
	contHTTP := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model":                "test",
		"input":                "follow up input",
		"previous_response_id": anchor.ID,
	})
	require.Equal(t, http.StatusOK, contHTTP.StatusCode,
		"continuation must succeed")
	follow := decodeJSON[ResponseResource](t, contHTTP)
	assert.Equal(t, "completed", follow.Status)

	// Capture the inference backend's request bodies in arrival order:
	// call 1 = anchor, call 2 = continuation.
	bodies := s.mockInf.RequestBodies()
	require.GreaterOrEqual(t, len(bodies), 2,
		"both anchor and continuation should have hit the inference backend")

	// Decode the continuation's inference request and walk its input array.
	var infReq struct {
		Input json.RawMessage `json:"input"`
	}
	require.NoError(t, json.Unmarshal(bodies[1], &infReq))

	var inputItems []inputItem
	require.NoError(t, json.Unmarshal(infReq.Input, &inputItems),
		"continuation input must be a JSON array (stringified for stateless inference)")

	// Strongest assertion: the anchor's response output (msg_ok, role=assistant)
	// appears in the chained context. Proves chunks → chain → served-context.
	var foundAssistant *inputItem
	for i := range inputItems {
		if inputItems[i].ID == "msg_ok" {
			foundAssistant = &inputItems[i]
			break
		}
	}
	require.NotNil(t, foundAssistant,
		"continuation's inference call must include the anchor's response output "+
			"(msg_ok) in its input — proves the chunked save was reassembled "+
			"into a chain entry the proxy can serve back")
	assert.Equal(t, "assistant", foundAssistant.Role,
		"anchor's msg_ok must appear as an assistant item in the chained context")

	// Sanity: at least three items — anchor input, anchor output, continuation input.
	assert.GreaterOrEqual(t, len(inputItems), 3,
		"chained input must contain anchor input + anchor output + continuation input")
}
