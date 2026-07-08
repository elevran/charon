package inference

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
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

// BaseURL returns the mock server's base URL (no trailing slash), satisfying
// the inference.Backend interface.
func (m *MockServer) BaseURL() string { return m.URL }

func (m *MockServer) nextID() string {
	n := m.counter.Add(1)
	return fmt.Sprintf("resp_%032x", n)
}

func (m *MockServer) handle(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Stream bool `json:"stream"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	_ = r.Body.Close()

	id := m.nextID()
	outputItem := json.RawMessage(`{"type":"message","id":"msg_ok","role":"assistant","status":"completed","content":[{"type":"output_text","text":"OK."}]}`)
	usage := &UsageInfo{InputTokens: 10, OutputTokens: 2, TotalTokens: 12}

	if req.Stream {
		m.writeStream(w, id, outputItem, usage)
		return
	}
	m.writeComplete(w, id, outputItem, usage)
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
