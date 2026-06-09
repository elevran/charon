package compliance_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	apihandler "github.com/elevran/charon/internal/api"
	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/proxy"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
	"github.com/elevran/charon/internal/worker"

	"net/http/httptest"
)

// ---------------------------------------------------------------------------
// Stack setup
// ---------------------------------------------------------------------------

type testStack struct {
	charonSrv *httptest.Server
	mockInf   *inference.MockServer
	proxySrv  *httptest.Server
}

func startStack(t testing.TB) *testStack {
	return startStackWithBuffer(t, 0)
}

// startStackWithBuffer creates a test stack with a specific proxy store buffer size.
// bufferBytes: 0 = use default (64K), -1 = no buffering (flush every item), N>0 = N byte threshold.
func startStackWithBuffer(t testing.TB, bufferBytes int) *testStack {
	t.Helper()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	svc := store.New(idx, pay, store.Config{}, log)
	charonH := apihandler.NewHandler(svc, log)
	charonMux := http.NewServeMux()
	apihandler.RegisterHandlers(charonMux, charonH)
	charonSrv := httptest.NewServer(apihandler.WrapH2c(charonMux))
	t.Cleanup(charonSrv.Close)

	mockInf := inference.NewMockServer()
	t.Cleanup(mockInf.Close)

	charonClient := charon.New(charonSrv.URL, 5*time.Second)
	infClient := inference.New(mockInf.URL, "", 5*time.Second)
	proxyH := proxy.NewHandler(charonClient, infClient, log, bufferBytes)
	proxyMux := http.NewServeMux()
	proxy.RegisterHandlers(proxyMux, proxyH)
	proxySrv := httptest.NewServer(proxyMux)
	t.Cleanup(proxySrv.Close)

	return &testStack{charonSrv: charonSrv, mockInf: mockInf, proxySrv: proxySrv}
}

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

func postJSON(t *testing.T, baseURL, path string, body interface{}) *http.Response {
	t.Helper()
	b, _ := json.Marshal(body)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", baseURL+path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	return resp
}

func decodeJSON[T any](t *testing.T, resp *http.Response) T {
	t.Helper()
	defer resp.Body.Close()
	var v T
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&v))
	return v
}

type sseResult struct {
	Events        []map[string]json.RawMessage
	FinalResponse *proxy.ResponseResource
	ErrorCode     string
}

func readSSE(t *testing.T, resp *http.Response) sseResult {
	t.Helper()
	defer resp.Body.Close()
	var r sseResult
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if !strings.HasPrefix(line, "data: ") {
			continue
		}
		data := strings.TrimPrefix(line, "data: ")
		var evt map[string]json.RawMessage
		if err := json.Unmarshal([]byte(data), &evt); err != nil {
			continue
		}
		r.Events = append(r.Events, evt)

		var typeStr string
		_ = json.Unmarshal(evt["type"], &typeStr)
		if typeStr == "response.completed" {
			var container struct {
				Response proxy.ResponseResource `json:"response"`
			}
			_ = json.Unmarshal([]byte(data), &container)
			res := container.Response
			r.FinalResponse = &res
		}
	}
	return r
}

// ---------------------------------------------------------------------------
// WebSocket helpers
// ---------------------------------------------------------------------------

type wsSession struct {
	conn *websocket.Conn
	t    *testing.T
}

func dialWS(t *testing.T, serverURL string) *wsSession {
	t.Helper()
	u, _ := url.Parse(serverURL)
	u.Scheme = "ws"
	u.Path = "/responses"
	conn, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return &wsSession{conn: conn, t: t}
}

func (w *wsSession) send(msg interface{}) {
	b, _ := json.Marshal(msg)
	require.NoError(w.t, w.conn.WriteMessage(websocket.TextMessage, b))
}

func (w *wsSession) readUntil(timeout time.Duration) (resp proxy.ResponseResource, errCode string) {
	w.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, msgBytes, err := w.conn.ReadMessage()
		if err != nil {
			return
		}
		var evt struct {
			Type     string                  `json:"type"`
			Response *proxy.ResponseResource `json:"response,omitempty"`
			Error    *struct {
				Code string `json:"code"`
			} `json:"error,omitempty"`
		}
		if json.Unmarshal(msgBytes, &evt) != nil {
			continue
		}
		switch evt.Type {
		case "response.completed", "response.failed":
			if evt.Response != nil {
				resp = *evt.Response
			}
			return
		case "error":
			if evt.Error != nil {
				errCode = evt.Error.Code
			}
			return
		}
	}
}

// ---------------------------------------------------------------------------
// Compliance tests
// ---------------------------------------------------------------------------

// 1. basic-response
func TestBasicResponse(t *testing.T) {
	s := startStack(t)
	resp := postJSON(t, s.proxySrv.URL, "/responses", map[string]interface{}{
		"model": "test",
		"input": []map[string]interface{}{{"type": "message", "role": "user", "content": "Say hello in exactly 3 words."}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := decodeJSON[proxy.ResponseResource](t, resp)
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

// 2. streaming-response
func TestStreamingResponse(t *testing.T) {
	s := startStack(t)
	body := map[string]interface{}{
		"model":  "test",
		"input":  []map[string]interface{}{{"type": "message", "role": "user", "content": "Count from 1 to 5."}},
		"stream": true,
	}
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxySrv.URL+"/responses", func() io.Reader {
		b, _ := json.Marshal(body)
		return bytes.NewReader(b)
	}())
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	sse := readSSE(t, resp)
	require.NotEmpty(t, sse.Events)
	require.NotNil(t, sse.FinalResponse)
	assert.Equal(t, "completed", sse.FinalResponse.Status)
	assert.NotEmpty(t, sse.FinalResponse.Output)

	var types []string
	for _, e := range sse.Events {
		var tp string
		_ = json.Unmarshal(e["type"], &tp)
		types = append(types, tp)
	}
	assert.Contains(t, types, "response.created")
	assert.Contains(t, types, "response.completed")
}

// 3. system-prompt
func TestSystemPrompt(t *testing.T) {
	s := startStack(t)
	resp := postJSON(t, s.proxySrv.URL, "/responses", map[string]interface{}{
		"model": "test",
		"input": []map[string]interface{}{
			{"type": "message", "role": "system", "content": "You are a pirate. Speak like one."},
			{"type": "message", "role": "user", "content": "Say hello."},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := decodeJSON[proxy.ResponseResource](t, resp)
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

// 4. multi-turn
func TestMultiTurn(t *testing.T) {
	s := startStack(t)
	resp := postJSON(t, s.proxySrv.URL, "/responses", map[string]interface{}{
		"model": "test",
		"input": []map[string]interface{}{
			{"type": "message", "role": "user", "content": "My name is Alice."},
			{"type": "message", "role": "assistant", "content": "Hello Alice!"},
			{"type": "message", "role": "user", "content": "What is my name?"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := decodeJSON[proxy.ResponseResource](t, resp)
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

// 5. websocket-response
func TestWSResponse(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)
	ws.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": []map[string]interface{}{{"type": "message", "role": "user", "content": "Count from 1 to 3."}},
	})
	resp, errCode := ws.readUntil(5 * time.Second)
	assert.Empty(t, errCode)
	assert.Equal(t, "completed", resp.Status)
	assert.NotEmpty(t, resp.Output)
}

// 6. websocket-sequential-responses
func TestWSSequentialResponses(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	ws.send(map[string]interface{}{"type": "response.create", "model": "test", "input": "Reply with exactly: first"})
	r1, e1 := ws.readUntil(5 * time.Second)
	require.Empty(t, e1)
	assert.Equal(t, "completed", r1.Status)
	assert.NotEmpty(t, r1.Output)

	ws.send(map[string]interface{}{"type": "response.create", "model": "test", "input": "Reply with exactly: second"})
	r2, e2 := ws.readUntil(5 * time.Second)
	require.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
	assert.NotEmpty(t, r2.Output)
}

// 7. websocket-continuation (store:false, connection-local cache)
func TestWSContinuation(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false
	ws.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "Remember the code word: cobalt. Reply with OK.",
		"store": storeFalse,
	})
	r1, e1 := ws.readUntil(5 * time.Second)
	require.Empty(t, e1)
	require.Equal(t, "completed", r1.Status)
	require.NotEmpty(t, r1.ID)

	ws.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "What is the code word? Reply with only the code word.",
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	})
	r2, e2 := ws.readUntil(5 * time.Second)
	assert.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
	assert.NotEmpty(t, r2.Output)
}

// 8. websocket-reconnect-store-false-recovery
func TestWSReconnectStoreFalseRecovery(t *testing.T) {
	s := startStack(t)

	storeFalse := false
	ws1 := dialWS(t, s.proxySrv.URL)
	ws1.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "seed turn",
		"store": storeFalse,
	})
	r1, _ := ws1.readUntil(5 * time.Second)
	require.Equal(t, "completed", r1.Status)
	ws1.conn.Close()

	// New connection — the store:false response is not in Charon or the new cache.
	ws2 := dialWS(t, s.proxySrv.URL)
	ws2.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "continue",
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	})
	_, errCode := ws2.readUntil(5 * time.Second)
	assert.Equal(t, "previous_response_not_found", errCode)

	// Recovery: fresh turn without previous_response_id.
	ws2.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "fresh start",
		"store": storeFalse,
	})
	rRecov, errRecov := ws2.readUntil(5 * time.Second)
	assert.Empty(t, errRecov)
	assert.Equal(t, "completed", rRecov.Status)
}

// 9. websocket-previous-response-not-found
func TestWSPreviousResponseNotFound(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false
	ws.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "This should fail.",
		"previous_response_id": "resp_openresponses_missing_12345",
		"store":                storeFalse,
	})
	_, errCode := ws.readUntil(5 * time.Second)
	assert.Equal(t, "previous_response_not_found", errCode)
}

// 10. websocket-failed-continuation-evicts-cache
func TestWSFailedContinuationEvictsCache(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false

	// Turn 1: store:false seed.
	ws.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "Remember the code word: ember. Reply with OK.",
		"store": storeFalse,
	})
	r1, e1 := ws.readUntil(5 * time.Second)
	require.Empty(t, e1)
	require.Equal(t, "completed", r1.Status)

	// Turn 2: orphaned function_call_output → failure.
	ws.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": []map[string]interface{}{{
			"type":    "function_call_output",
			"call_id": "call_orphaned_no_match",
			"output":  "some result",
		}},
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	})
	r2, errCode2 := ws.readUntil(5 * time.Second)
	isFailed := r2.Status == "failed" || errCode2 != ""
	assert.True(t, isFailed, "turn 2 must be a failure; status=%q errCode=%q", r2.Status, errCode2)

	// Turn 3: same previous_response_id must now be not_found (evicted).
	ws.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "Continue from here.",
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	})
	_, errCode3 := ws.readUntil(5 * time.Second)
	assert.Equal(t, "previous_response_not_found", errCode3)
}

// 11. response-output-phase-schema (local struct test, no HTTP required)
func TestResponseOutputPhaseSchema(t *testing.T) {
	commentary := json.RawMessage(`{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"Thinking..."}],"phase":"commentary"}`)
	finalAnswer := json.RawMessage(`{"type":"message","id":"msg_2","role":"assistant","status":"completed","content":[{"type":"output_text","text":"The answer is 42."}],"phase":"final_answer"}`)

	resource := proxy.ResponseResource{
		ID:        "resp_phase_test",
		Object:    "response",
		CreatedAt: time.Now().Unix(),
		Status:    "completed",
		Model:     "test",
		Output:    []json.RawMessage{commentary, finalAnswer},
		Tools:     []json.RawMessage{},
		Metadata:  map[string]string{},
	}

	b, err := json.Marshal(resource)
	require.NoError(t, err)

	var decoded proxy.ResponseResource
	require.NoError(t, json.Unmarshal(b, &decoded))
	require.Len(t, decoded.Output, 2)

	// Verify phase field is preserved verbatim.
	var item0 map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded.Output[0], &item0))
	assert.Equal(t, "commentary", item0["phase"])

	var item1 map[string]interface{}
	require.NoError(t, json.Unmarshal(decoded.Output[1], &item1))
	assert.Equal(t, "final_answer", item1["phase"])
}

// 12. compact-missing-model
func TestCompactMissingModel(t *testing.T) {
	s := startStack(t)
	resp := postJSON(t, s.proxySrv.URL, "/responses/compact", map[string]interface{}{
		"input": []map[string]interface{}{
			{"type": "message", "role": "user", "content": "Compact this conversation."},
		},
	})
	defer resp.Body.Close()
	assert.True(t, resp.StatusCode == 400 || resp.StatusCode == 422,
		"expected 400 or 422, got %d", resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Streaming store tests — buffer=default (64K) and buffer=-1 (no buffering)
// ---------------------------------------------------------------------------

// TestStreamingResponse_NoBuffer verifies SSE streaming with immediate per-item
// Charon flushes (storeBufferBytes=-1) produces the same result as buffered mode.
func TestStreamingResponse_NoBuffer(t *testing.T) {
	s := startStackWithBuffer(t, -1)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxySrv.URL+"/responses",
		strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, resp.StatusCode)

	sse := readSSE(t, resp)
	require.NotNil(t, sse.FinalResponse)
	assert.Equal(t, "completed", sse.FinalResponse.Status)
	assert.NotEmpty(t, sse.FinalResponse.Output)

	// Verify the stored response is retrievable (chunked store committed correctly).
	getResp, err := http.Get(s.proxySrv.URL + "/responses/" + sse.FinalResponse.ID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)
	var retrieved proxy.ResponseResource
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&retrieved))
	assert.Equal(t, sse.FinalResponse.ID, retrieved.ID)
	assert.NotEmpty(t, retrieved.Output)
}

// TestStreamingResponse_SmallBuffer verifies a buffer smaller than the output
// triggers at least one mid-stream flush to Charon.
func TestStreamingResponse_SmallBuffer(t *testing.T) {
	// 1-byte buffer forces a flush after every item.
	s := startStackWithBuffer(t, 1)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxySrv.URL+"/responses",
		strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	sse := readSSE(t, resp)
	require.NotNil(t, sse.FinalResponse)
	assert.Equal(t, "completed", sse.FinalResponse.Status)
}

// TestStreamStore_StripsSSEFraming verifies stored output items contain only
// item-type fields (e.g. "type":"message") and NOT SSE envelope fields
// ("sequence_number", "output_index").
func TestStreamStore_StripsSSEFraming(t *testing.T) {
	s := startStackWithBuffer(t, -1)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxySrv.URL+"/responses",
		strings.NewReader(`{"model":"test","input":"hello","stream":true}`))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	sse := readSSE(t, resp)
	require.NotNil(t, sse.FinalResponse)

	// Retrieve and check each stored output item.
	getResp, err := http.Get(s.proxySrv.URL + "/responses/" + sse.FinalResponse.ID)
	require.NoError(t, err)
	defer getResp.Body.Close()
	var retrieved proxy.ResponseResource
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&retrieved))
	require.NotEmpty(t, retrieved.Output)

	for _, item := range retrieved.Output {
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(item, &fields))
		assert.NotContains(t, fields, "sequence_number", "SSE envelope field must not be stored")
		assert.NotContains(t, fields, "output_index", "SSE envelope field must not be stored")
		assert.Contains(t, fields, "type", "stored item must have a type field")
	}
}

// TestWSContinuation_NoBuffer verifies WebSocket continuation works with
// no-buffering mode.
func TestWSContinuation_NoBuffer(t *testing.T) {
	s := startStackWithBuffer(t, -1)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false
	ws.send(map[string]interface{}{
		"type": "response.create", "model": "test",
		"input": "Remember the code word: cobalt. Reply with OK.",
		"store": storeFalse,
	})
	r1, e1 := ws.readUntil(5 * time.Second)
	require.Empty(t, e1)
	require.Equal(t, "completed", r1.Status)

	ws.send(map[string]interface{}{
		"type": "response.create", "model": "test",
		"input":                "What is the code word?",
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	})
	r2, e2 := ws.readUntil(5 * time.Second)
	assert.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
}

// TestStreamingRecovery_StreamOpenIntent verifies that a stale stream_open
// write-intent is marked failed by the recovery worker.
func TestStreamingRecovery_StreamOpenIntent(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	old := time.Now().Add(-10 * time.Minute).Unix()
	intent := model.WriteIntent{
		IntentID:   "wi_stream1",
		ResponseID: "resp_stream1",
		PayloadKey: "",
		Phase:      model.WriteIntentStreamOpen,
		CreatedAt:  old,
		UpdatedAt:  old,
	}
	require.NoError(t, idx.InsertWriteIntent(context.Background(), intent))

	rec := worker.NewReconciler(idx, pay, log, 5*time.Minute, time.Hour)
	rec.RunOnce(context.Background())

	stale, _ := idx.ListStaleWriteIntents(context.Background(), 5*time.Minute)
	for _, s := range stale {
		assert.NotEqual(t, "wi_stream1", s.IntentID, "stream_open intent must be marked failed by recovery")
	}
}
