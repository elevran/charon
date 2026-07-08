package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// 1. basic-response
func TestBasicResponse(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": []map[string]interface{}{{"type": "message", "role": "user", "content": "Say hello in exactly 3 words."}},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := decodeJSON[ResponseResource](t, resp)
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

// 2. streaming-response
func TestStreamingResponse(t *testing.T) {
	s := newTestStack(t)
	body := map[string]interface{}{
		"model":  "test",
		"input":  []map[string]interface{}{{"type": "message", "role": "user", "content": "Count from 1 to 5."}},
		"stream": true,
	}
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses", func() io.Reader {
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
	assert.Contains(t, sse.EventTypes, "response.created")
	assert.Contains(t, sse.EventTypes, "response.completed")
}

// 3. system-prompt
func TestSystemPrompt(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": []map[string]interface{}{
			{"type": "message", "role": "system", "content": "You are a pirate. Speak like one."},
			{"type": "message", "role": "user", "content": "Say hello."},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := decodeJSON[ResponseResource](t, resp)
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

// 4. multi-turn
func TestMultiTurn(t *testing.T) {
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses", map[string]interface{}{
		"model": "test",
		"input": []map[string]interface{}{
			{"type": "message", "role": "user", "content": "My name is Alice."},
			{"type": "message", "role": "assistant", "content": "Hello Alice!"},
			{"type": "message", "role": "user", "content": "What is my name?"},
		},
	})
	require.Equal(t, http.StatusOK, resp.StatusCode)
	r := decodeJSON[ResponseResource](t, resp)
	assert.Equal(t, "completed", r.Status)
	assert.NotEmpty(t, r.Output)
}

// 5. websocket-response
func TestWSResponse(t *testing.T) {
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)
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
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)

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
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)

	ws.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "Remember the code word: cobalt. Reply with OK.",
		"store": false,
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
		"store":                false,
	})
	r2, e2 := ws.readUntil(5 * time.Second)
	assert.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
	assert.NotEmpty(t, r2.Output)
}

// 8. websocket-reconnect-store-false-recovery
func TestWSReconnectStoreFalseRecovery(t *testing.T) {
	s := newTestStack(t)

	ws1 := dialWS(t, s.proxyURL)
	ws1.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "seed turn",
		"store": false,
	})
	r1, _ := ws1.readUntil(5 * time.Second)
	require.Equal(t, "completed", r1.Status)
	ws1.conn.Close()

	// New connection — the store:false response is not in Charon or the new cache.
	ws2 := dialWS(t, s.proxyURL)
	ws2.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "continue",
		"previous_response_id": r1.ID,
		"store":                false,
	})
	_, errCode := ws2.readUntil(5 * time.Second)
	assert.Equal(t, "previous_response_not_found", errCode)

	// Recovery: fresh turn without previous_response_id.
	ws2.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "fresh start",
		"store": false,
	})
	rRecov, errRecov := ws2.readUntil(5 * time.Second)
	assert.Empty(t, errRecov)
	assert.Equal(t, "completed", rRecov.Status)
}

// 9. websocket-previous-response-not-found
func TestWSPreviousResponseNotFound(t *testing.T) {
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)

	ws.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "This should fail.",
		"previous_response_id": "resp_openresponses_missing_12345",
		"store":                false,
	})
	_, errCode := ws.readUntil(5 * time.Second)
	assert.Equal(t, "previous_response_not_found", errCode)
}

// 10. websocket-failed-continuation-evicts-cache
func TestWSFailedContinuationEvictsCache(t *testing.T) {
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)

	// Turn 1: store:false seed.
	ws.send(map[string]interface{}{
		"type":  "response.create",
		"model": "test",
		"input": "Remember the code word: ember. Reply with OK.",
		"store": false,
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
		"store":                false,
	})
	r2, errCode2 := ws.readUntil(5 * time.Second)
	assert.True(t, r2.Status == "failed" || errCode2 != "",
		"turn 2 must be a failure; status=%q errCode=%q", r2.Status, errCode2)

	// Turn 3: same previous_response_id must now be not_found (evicted).
	ws.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "Continue from here.",
		"previous_response_id": r1.ID,
		"store":                false,
	})
	_, errCode3 := ws.readUntil(5 * time.Second)
	assert.Equal(t, "previous_response_not_found", errCode3)
}

// 11. response-output-phase-schema (local struct test, no HTTP required)
func TestResponseOutputPhaseSchema(t *testing.T) {
	commentary := json.RawMessage(`{"type":"message","id":"msg_1","role":"assistant","status":"completed","content":[{"type":"output_text","text":"Thinking..."}],"phase":"commentary"}`)
	finalAnswer := json.RawMessage(`{"type":"message","id":"msg_2","role":"assistant","status":"completed","content":[{"type":"output_text","text":"The answer is 42."}],"phase":"final_answer"}`)

	resource := ResponseResource{
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

	var decoded ResponseResource
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
	s := newTestStack(t)
	resp := doRequest(t, s.proxyURL, "POST", "/responses/compact", map[string]interface{}{
		"input": []map[string]interface{}{
			{"type": "message", "role": "user", "content": "Compact this conversation."},
		},
	})
	defer resp.Body.Close()
	assert.True(t, resp.StatusCode == 400 || resp.StatusCode == 422,
		"expected 400 or 422, got %d", resp.StatusCode)
}

// ---------------------------------------------------------------------------
// Streaming store tests
// ---------------------------------------------------------------------------

// TestStreamStore_StripsSSEFraming verifies stored output items contain only
// item-type fields and NOT SSE envelope fields ("sequence_number", "output_index").
func TestStreamStore_StripsSSEFraming(t *testing.T) {
	s := newTestStack(t)
	req, _ := http.NewRequestWithContext(context.Background(), "POST", s.proxyURL+"/responses",
		bytes.NewReader([]byte(`{"model":"test","input":"hello","stream":true}`)))
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	sse := readSSE(t, resp)
	require.NotNil(t, sse.FinalResponse)

	getResp := doRequest(t, s.proxyURL, "GET", "/responses/"+sse.FinalResponse.ID, nil)
	var retrieved ResponseResource
	require.NoError(t, json.NewDecoder(getResp.Body).Decode(&retrieved))
	getResp.Body.Close()
	require.NotEmpty(t, retrieved.Output)

	for _, item := range retrieved.Output {
		var fields map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(item, &fields))
		assert.NotContains(t, fields, "sequence_number", "SSE envelope field must not be stored")
		assert.NotContains(t, fields, "output_index", "SSE envelope field must not be stored")
		assert.Contains(t, fields, "type", "stored item must have a type field")
	}
}

// TestWSContinuation_NoBuffer verifies WebSocket continuation persists across turns.
func TestWSContinuation_NoBuffer(t *testing.T) {
	s := newTestStack(t)
	ws := dialWS(t, s.proxyURL)

	ws.send(map[string]interface{}{
		"type": "response.create", "model": "test",
		"input": "Remember the code word: cobalt. Reply with OK.",
		"store": false,
	})
	r1, e1 := ws.readUntil(5 * time.Second)
	require.Empty(t, e1)
	require.Equal(t, "completed", r1.Status)

	ws.send(map[string]interface{}{
		"type":                 "response.create",
		"model":                "test",
		"input":                "What is the code word?",
		"previous_response_id": r1.ID,
		"store":                false,
	})
	r2, e2 := ws.readUntil(5 * time.Second)
	assert.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
}
