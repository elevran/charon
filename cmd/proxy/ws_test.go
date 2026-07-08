package main

import (
	"encoding/json"
	"net/url"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// wsConn wraps a gorilla WebSocket connection with helpers.
type wsConn struct {
	conn *websocket.Conn
}

func dialWSConn(t *testing.T, serverURL string) *wsConn {
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

func (w *wsConn) readUntilCompleted(t *testing.T, timeout time.Duration) (finalResp ResponseResource, errorCode string) {
	t.Helper()
	w.conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, msgBytes, err := w.conn.ReadMessage()
		if err != nil {
			t.Logf("ws read ended: %v", err)
			return
		}
		var evt struct {
			Type     string            `json:"type"`
			Response *ResponseResource `json:"response,omitempty"`
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
	ws := dialWSConn(t, s.proxySrv.URL)

	ws.send(t, createMsg("test", "hello"))
	resp, errCode := ws.readUntilCompleted(t, 5*time.Second)

	assert.Empty(t, errCode)
	assert.Equal(t, "completed", resp.Status)
	assert.NotEmpty(t, resp.ID)
	assert.NotEmpty(t, resp.Output)
}
