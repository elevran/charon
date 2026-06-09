package proxy_test

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/proxy"
)

// wsConn wraps a gorilla WebSocket connection with helpers.
type wsConn struct {
	conn *websocket.Conn
}

func dialWS(t *testing.T, serverURL string) *wsConn {
	t.Helper()
	u, _ := url.Parse(serverURL)
	u.Scheme = "ws"
	u.Path = "/responses"
	c, _, err := websocket.DefaultDialer.Dial(u.String(), nil)
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return &wsConn{conn: c}
}

func (w *wsConn) send(t *testing.T, msg interface{}) {
	t.Helper()
	b, _ := json.Marshal(msg)
	require.NoError(t, w.conn.WriteMessage(websocket.TextMessage, b))
}

func (w *wsConn) readUntilCompleted(t *testing.T, timeout time.Duration) (finalResp proxy.ResponseResource, errorCode string) {
	t.Helper()
	w.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, msgBytes, err := w.conn.ReadMessage()
		if err != nil {
			t.Logf("ws read ended: %v", err)
			return
		}
		var evt struct {
			Type     string                  `json:"type"`
			Response *proxy.ResponseResource `json:"response,omitempty"`
			Error    *struct {
				Code string `json:"code"`
			} `json:"error,omitempty"`
			Status int `json:"status"`
		}
		require.NoError(t, json.Unmarshal(msgBytes, &evt))

		switch evt.Type {
		case "response.completed":
			if evt.Response != nil {
				finalResp = *evt.Response
			}
			return
		case "response.failed":
			if evt.Response != nil {
				finalResp = *evt.Response
			}
			return
		case "error":
			if evt.Error != nil {
				errorCode = evt.Error.Code
			}
			return
		}
	}
}

func createMsg(model string, input interface{}, options ...map[string]interface{}) map[string]interface{} {
	msg := map[string]interface{}{
		"type":  "response.create",
		"model": model,
		"input": input,
	}
	if len(options) > 0 {
		for k, v := range options[0] {
			msg[k] = v
		}
	}
	return msg
}

// --- Tests ---

func TestWSBasicResponse(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	ws.send(t, createMsg("test", "hello"))
	resp, errCode := ws.readUntilCompleted(t, 5*time.Second)

	assert.Empty(t, errCode)
	assert.Equal(t, "completed", resp.Status)
	assert.NotEmpty(t, resp.ID)
	assert.NotEmpty(t, resp.Output)
}

func TestWSSequentialResponses(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	ws.send(t, createMsg("test", "first"))
	r1, e1 := ws.readUntilCompleted(t, 5*time.Second)
	assert.Empty(t, e1)
	assert.Equal(t, "completed", r1.Status)

	ws.send(t, createMsg("test", "second"))
	r2, e2 := ws.readUntilCompleted(t, 5*time.Second)
	assert.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
}

func TestWSContinuation(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false
	ws.send(t, createMsg("test", "Remember the code word: cobalt.", map[string]interface{}{
		"store": storeFalse,
	}))
	r1, e1 := ws.readUntilCompleted(t, 5*time.Second)
	require.Empty(t, e1)
	require.Equal(t, "completed", r1.Status)
	require.NotEmpty(t, r1.ID)

	// Turn 2 continues from turn 1 (store:false, lives in connection cache).
	ws.send(t, createMsg("test", "What is the code word?", map[string]interface{}{
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	}))
	r2, e2 := ws.readUntilCompleted(t, 5*time.Second)
	assert.Empty(t, e2)
	assert.Equal(t, "completed", r2.Status)
}

func TestWSPreviousResponseNotFound(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false
	ws.send(t, createMsg("test", "hello", map[string]interface{}{
		"previous_response_id": "resp_doesnotexist_at_all",
		"store":                storeFalse,
	}))
	_, errCode := ws.readUntilCompleted(t, 5*time.Second)
	assert.Equal(t, "previous_response_not_found", errCode)
}

func TestWSReconnectStoreFalseRecovery(t *testing.T) {
	s := startStack(t)

	// Session 1: Turn 1 with store:false.
	ws1 := dialWS(t, s.proxySrv.URL)
	storeFalse := false
	ws1.send(t, createMsg("test", "session 1 turn 1", map[string]interface{}{"store": storeFalse}))
	r1, _ := ws1.readUntilCompleted(t, 5*time.Second)
	require.Equal(t, "completed", r1.Status)
	turn1ID := r1.ID
	ws1.conn.Close()

	// Session 2 (new connection): try to continue from session 1's store:false response.
	ws2 := dialWS(t, s.proxySrv.URL)
	ws2.send(t, createMsg("test", "continue", map[string]interface{}{
		"previous_response_id": turn1ID,
		"store":                storeFalse,
	}))
	_, errCode := ws2.readUntilCompleted(t, 5*time.Second)
	assert.Equal(t, "previous_response_not_found", errCode,
		"store:false response must not be found on new connection")

	// Session 2 recovery: fresh turn without previous_response_id succeeds.
	ws2.send(t, createMsg("test", "fresh start", map[string]interface{}{"store": storeFalse}))
	rRecovery, errCodeRecovery := ws2.readUntilCompleted(t, 5*time.Second)
	assert.Empty(t, errCodeRecovery)
	assert.Equal(t, "completed", rRecovery.Status)
}

func TestWSFailedContinuationEvictsCache(t *testing.T) {
	s := startStack(t)
	ws := dialWS(t, s.proxySrv.URL)

	storeFalse := false

	// Turn 1: store:false, seed response.
	ws.send(t, createMsg("test", "Remember the code word: ember.", map[string]interface{}{"store": storeFalse}))
	r1, e1 := ws.readUntilCompleted(t, 5*time.Second)
	require.Empty(t, e1)
	require.Equal(t, "completed", r1.Status)

	// Turn 2: orphaned function_call_output (no preceding function_call) → failed.
	badInput := []map[string]interface{}{{
		"type":    "function_call_output",
		"call_id": "call_orphaned_999",
		"output":  "result",
	}}
	ws.send(t, createMsg("test", badInput, map[string]interface{}{
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	}))
	r2, errCode2 := ws.readUntilCompleted(t, 5*time.Second)
	isFailed := r2.Status == "failed" || errCode2 != ""
	assert.True(t, isFailed, "turn 2 must fail: status=%q errCode=%q", r2.Status, errCode2)

	// Turn 3: same previous_response_id should now return not_found (evicted).
	ws.send(t, createMsg("test", "continue", map[string]interface{}{
		"previous_response_id": r1.ID,
		"store":                storeFalse,
	}))
	_, errCode3 := ws.readUntilCompleted(t, 5*time.Second)
	assert.Equal(t, "previous_response_not_found", errCode3,
		"turn 1 cache entry must be evicted after failed continuation")
}
