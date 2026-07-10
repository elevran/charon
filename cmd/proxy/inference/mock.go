package inference

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
)

// MockServer is a deterministic Responses API inference server for tests.
// Every POST /responses returns a valid Response with:
//   - id: "resp_" + 32 hex zero-padded counter (unique per request)
//   - status: "completed"
//   - output: [{type:"message",role:"assistant",status:"completed",
//     content:[{type:"output_text",text:"OK."}]}]
//
// Streaming emits the standard SSE event sequence for the same response.
type MockServer struct {
	*httptest.Server
	counter atomic.Int64

	mu     sync.Mutex
	bodies [][]byte // captured request bodies, in arrival order
}

// NewMockServer starts a mock inference server. The caller must call Close().
func NewMockServer() *MockServer {
	m := &MockServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /responses", m.handle)
	m.Server = httptest.NewServer(mux)
	return m
}

// Calls returns the total number of inference requests handled.
func (m *MockServer) Calls() int64 { return m.counter.Load() }

// RequestBodies returns a snapshot of every request body the mock has
// received, in arrival order. Each returned byte slice is a deep copy of
// the stored body, so callers may mutate the slices without affecting
// future snapshots or each other. Tests use this to assert what the
// proxy actually forwarded to the inference backend.
func (m *MockServer) RequestBodies() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, len(m.bodies))
	for i, b := range m.bodies {
		out[i] = append([]byte(nil), b...)
	}
	return out
}

// BaseURL returns the mock server's base URL (no trailing slash), satisfying
// the inference.Backend interface.
func (m *MockServer) BaseURL() string { return m.URL }

func (m *MockServer) nextID() string {
	n := m.counter.Add(1)
	return fmt.Sprintf("resp_%032x", n)
}

func (m *MockServer) handle(w http.ResponseWriter, r *http.Request) {
	// recordAndReplaceBody MUST run before any JSON decode of r.Body — it
	// drains r.Body and re-installs a fresh reader so subsequent decoding
	// observes the same bytes we captured. If a future change decodes
	// r.Body directly before this call, stream=false would be observed
	// for every request, regardless of what the client sent.
	recordAndReplaceBody(r, m)

	var req struct {
		Stream bool `json:"stream"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)

	id := m.nextID()
	outputItem := json.RawMessage(`{"type":"message","id":"msg_ok","role":"assistant","status":"completed","content":[{"type":"output_text","text":"OK."}]}`)
	usage := &UsageInfo{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}

	if req.Stream {
		m.writeStream(w, id, outputItem, usage)
		return
	}
	m.writeComplete(w, id, outputItem, usage)
}

// recordAndReplaceBody reads r.Body fully, stores a copy in m.bodies, and
// re-installs a fresh reader on r.Body so subsequent decoders in the handler
// see the same bytes. On read error the body may be partial or empty —
// either way we still replace r.Body so the handler decodes a deterministic
// (possibly empty) payload rather than re-draining an unknown state.
func recordAndReplaceBody(r *http.Request, m *MockServer) {
	body, _ := io.ReadAll(r.Body)
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))

	cp := make([]byte, len(body))
	copy(cp, body)
	m.mu.Lock()
	m.bodies = append(m.bodies, cp)
	m.mu.Unlock()
}

func (m *MockServer) writeComplete(w http.ResponseWriter, id string, item json.RawMessage, usage *UsageInfo) {
	w.Header().Set("Content-Type", "application/json")
	resp := Response{
		ID:     id,
		Status: "completed",
		Model:  "mock",
		Output: []json.RawMessage{item},
		Usage:  usage,
	}
	_ = json.NewEncoder(w).Encode(resp)
}

// NewPartialMockServer creates a mock that closes the inference stream after
// emitting response.created, simulating a mid-stream backend failure (no
// response.completed is ever delivered).
func NewPartialMockServer() *MockServer {
	m := &MockServer{}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /responses", func(w http.ResponseWriter, _ *http.Request) {
		id := m.nextID()
		m.writePartialStream(w, id)
	})
	m.Server = httptest.NewServer(mux)
	return m
}

func (m *MockServer) writePartialStream(w http.ResponseWriter, id string) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	f := w.(http.Flusher)

	inProgressResp := Response{ID: id, Status: "in_progress", Model: "mock", Output: []json.RawMessage{}}
	evt := SSEEvent{Type: "response.created", SequenceNumber: 0, Response: &inProgressResp}
	b, _ := json.Marshal(evt)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
	f.Flush()
	// Return without response.completed — handler exit closes the response body,
	// which signals the inference client's SSE reader goroutine to stop.
}

func (m *MockServer) writeStream(w http.ResponseWriter, id string, item json.RawMessage, usage *UsageInfo) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	f := w.(http.Flusher)

	inProgressResp := Response{ID: id, Status: "in_progress", Model: "mock", Output: []json.RawMessage{}}
	completedResp := Response{ID: id, Status: "completed", Model: "mock", Output: []json.RawMessage{item}, Usage: usage}

	idx := 0
	writeEvent := func(evt SSEEvent) {
		evt.SequenceNumber = idx
		idx++
		b, _ := json.Marshal(evt)
		_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
		f.Flush()
	}

	writeEvent(SSEEvent{Type: "response.created", Response: &inProgressResp})
	outIdx := 0
	writeEvent(SSEEvent{Type: "response.output_item.added", OutputIndex: &outIdx, Item: json.RawMessage(`{"type":"message","id":"msg_ok","role":"assistant","status":"in_progress","content":[]}`)})
	writeEvent(SSEEvent{Type: "response.output_item.done", OutputIndex: &outIdx, Item: item})
	writeEvent(SSEEvent{Type: "response.completed", Response: &completedResp})
}
