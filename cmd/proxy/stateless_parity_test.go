package main

// TestStatelessStatefulParity demonstrates that the inference backend always
// receives the complete conversation context (a stateless view), even though
// clients use the stateful previous_response_id API.
//
// Proxy flow per turn:
//
//	client:    POST /responses {input: "...", previous_response_id: "<prev>"}
//	proxy:     GET  /charon/responses/<prev>/context  → flat_context
//	inference: POST /responses {input: [flat_context... + new_input...], store: false}
//
// From the inference backend's view every call is stateless: it always sees the
// full conversation history assembled by the proxy. This test captures inference
// requests and asserts the assembled context grows correctly across three turns.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/cmd/proxy/inference"
)

// capturedInfReq records a single request body received by the inference server.
type capturedInfReq struct {
	Model  string            `json:"model"`
	Input  []json.RawMessage `json:"input"`
	Store  bool              `json:"store"`
	Stream bool              `json:"stream"`
}

// capturingInfServer is a test inference server that records each request.
// It returns the same deterministic response as MockServer.
type capturingInfServer struct {
	*httptest.Server
	mu      sync.Mutex
	reqs    []capturedInfReq
	counter atomic.Int64
}

func newCapturingInfServer(t *testing.T) *capturingInfServer {
	t.Helper()
	c := &capturingInfServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /responses", c.handle)
	c.Server = httptest.NewServer(mux)
	t.Cleanup(c.Close)
	return c
}

func (c *capturingInfServer) handle(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()

	var req capturedInfReq
	_ = json.Unmarshal(body, &req)
	c.mu.Lock()
	c.reqs = append(c.reqs, req)
	c.mu.Unlock()

	n := c.counter.Add(1)
	id := fmt.Sprintf("resp_%032x", n)
	outputItem := json.RawMessage(`{"type":"message","id":"msg_ok","role":"assistant","status":"completed","content":[{"type":"output_text","text":"OK."}]}`)
	resp := inference.Response{
		ID:     id,
		Status: "completed",
		Model:  "mock",
		Output: []json.RawMessage{outputItem},
		Usage:  &inference.UsageInfo{InputTokens: 10, OutputTokens: 2, TotalTokens: 12},
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func (c *capturingInfServer) captured() []capturedInfReq {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]capturedInfReq, len(c.reqs))
	copy(out, c.reqs)
	return out
}

// postResponse sends POST /responses to the proxy and returns the decoded resource.
func postResponse(t *testing.T, client *http.Client, baseURL string, body map[string]any) ResponseResource {
	t.Helper()
	b, _ := json.Marshal(body)
	resp, err := client.Post(baseURL+"/responses", "application/json", bytes.NewReader(b))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusOK, resp.StatusCode)
	var r ResponseResource
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&r))
	return r
}

// msgRole extracts the "role" field from a raw JSON item.
func msgRole(t *testing.T, raw json.RawMessage) string {
	t.Helper()
	var m struct {
		Role string `json:"role"`
	}
	require.NoError(t, json.Unmarshal(raw, &m))
	return m.Role
}

// assertItemsJSONEqual checks two slices of raw JSON items are semantically equal.
func assertItemsJSONEqual(t *testing.T, want, got []json.RawMessage) {
	t.Helper()
	require.Len(t, got, len(want), "item count mismatch")
	for i := range want {
		var w, g any
		require.NoError(t, json.Unmarshal(want[i], &w), "unmarshal want[%d]", i)
		require.NoError(t, json.Unmarshal(got[i], &g), "unmarshal got[%d]", i)
		assert.Equal(t, w, g, "item %d differs", i)
	}
}

// TestStatelessStatefulParity is the main demonstration.
//
// The client makes three turns using the stateful previous_response_id API.
// After each turn we inspect the inference server's captured input to show
// that the proxy assembled the full conversation context — identical to what a
// stateless client would have to construct manually.
func TestStatelessStatefulParity(t *testing.T) {
	infSrv := newCapturingInfServer(t)
	s := newTestStack(t, withInferenceURL(infSrv.URL))
	client := http.DefaultClient
	if s.proxySrv != nil {
		client = s.proxySrv.Client()
	}
	post := func(body map[string]any) ResponseResource {
		return postResponse(t, client, s.proxyURL, body)
	}

	// ----------------------------------------------------------------
	// Stateful client: each turn sends only the latest message.
	// ----------------------------------------------------------------

	// Turn 0 — new conversation, no history.
	r0 := post(map[string]any{"model": "test", "input": "hello"})
	require.Equal(t, "completed", r0.Status)

	// Turn 1 — client references turn 0, sends only the new message.
	r1 := post(map[string]any{
		"model":                "test",
		"input":                "how are you?",
		"previous_response_id": r0.ID,
	})
	require.Equal(t, "completed", r1.Status)

	// Turn 2 — client references turn 1, sends only the new message.
	r2 := post(map[string]any{
		"model":                "test",
		"input":                "goodbye",
		"previous_response_id": r1.ID,
	})
	require.Equal(t, "completed", r2.Status)

	// ----------------------------------------------------------------
	// Assertion: inference backend received stateless, complete context.
	// ----------------------------------------------------------------

	captured := infSrv.captured()
	require.Len(t, captured, 3, "expected one inference call per turn")

	// All inference requests must have store=false and stream=false:
	// the proxy owns persistence, the inference backend is stateless.
	for i, req := range captured {
		assert.False(t, req.Store, "turn %d: inference request must have store=false", i)
		assert.False(t, req.Stream, "turn %d: inference request must have stream=false", i)
	}

	// Turn 0: inference sees exactly the one user message the client sent.
	require.Len(t, captured[0].Input, 1, "turn 0: inference sees 1 item")
	assert.Equal(t, "user", msgRole(t, captured[0].Input[0]))

	// Turn 1: inference sees [t0_user, t0_assistant, t1_user] — 3 items.
	require.Len(t, captured[1].Input, 3, "turn 1: inference sees 3 items (prior 2 + 1 new)")
	assert.Equal(t, "user", msgRole(t, captured[1].Input[0]), "t0 input")
	assert.Equal(t, "assistant", msgRole(t, captured[1].Input[1]), "t0 output")
	assert.Equal(t, "user", msgRole(t, captured[1].Input[2]), "t1 input")

	// Turn 2: inference sees [t0_user, t0_asst, t1_user, t1_asst, t2_user] — 5 items.
	require.Len(t, captured[2].Input, 5, "turn 2: inference sees 5 items (prior 4 + 1 new)")
	assert.Equal(t, "user", msgRole(t, captured[2].Input[0]), "t0 input")
	assert.Equal(t, "assistant", msgRole(t, captured[2].Input[1]), "t0 output")
	assert.Equal(t, "user", msgRole(t, captured[2].Input[2]), "t1 input")
	assert.Equal(t, "assistant", msgRole(t, captured[2].Input[3]), "t1 output")
	assert.Equal(t, "user", msgRole(t, captured[2].Input[4]), "t2 input")

	// ----------------------------------------------------------------
	// Stateless equivalence check.
	// ----------------------------------------------------------------

	assertItemsJSONEqual(t,
		captured[0].Input[:1],
		captured[1].Input[:1],
	)

	assertItemsJSONEqual(t,
		captured[1].Input[:3],
		captured[2].Input[:3],
	)
}
