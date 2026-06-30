package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(_ *http.Request) bool { return true },
}

// wsCache stores store:false responses for a single WebSocket connection.
// Key: canonical response ID.
// Value: full flat_context through that turn (prior flat_context + input + output).
type wsCache struct {
	mu    sync.Mutex
	items map[string][]json.RawMessage
}

func newWSCache() *wsCache { return &wsCache{items: make(map[string][]json.RawMessage)} }

func (c *wsCache) put(id string, ctx []json.RawMessage) {
	c.mu.Lock()
	c.items[id] = ctx
	c.mu.Unlock()
}

func (c *wsCache) get(id string) ([]json.RawMessage, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.items[id]
	return v, ok
}

func (c *wsCache) evict(id string) {
	c.mu.Lock()
	delete(c.items, id)
	c.mu.Unlock()
}

// wsCreateMsg is a response.create message from the WebSocket client.
type wsCreateMsg struct {
	Type               string            `json:"type"`
	Model              string            `json:"model"`
	Input              json.RawMessage   `json:"input"`
	PreviousResponseID *string           `json:"previous_response_id,omitempty"`
	Instructions       *string           `json:"instructions,omitempty"`
	Store              *bool             `json:"store,omitempty"`
	Background         bool              `json:"background,omitempty"`
	Tools              []json.RawMessage `json:"tools,omitempty"`
	ToolChoice         json.RawMessage   `json:"tool_choice,omitempty"`
}

func (m *wsCreateMsg) ShouldStore() bool { return m.Store == nil || *m.Store }

// wsErrorEvent is sent to the client when an error occurs at the protocol level.
type wsErrorEvent struct {
	Type   string         `json:"type"` // "error"
	Status int            `json:"status"`
	Error  *ResponseError `json:"error"`
}

// HandleListOrWS upgrades WebSocket connections or returns the empty list.
func (h *Handler) HandleListOrWS(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		h.HandleWebSocket(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"object":   "list",
		"data":     []interface{}{},
		"has_more": false,
	})
}

// HandleWebSocket handles the WebSocket upgrade for GET /responses.
func (h *Handler) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Error("ws upgrade", "err", err)
		return
	}
	defer func() { _ = conn.Close() }()

	cache := newWSCache()
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			return // client disconnected
		}

		var msg wsCreateMsg
		if err := json.Unmarshal(msgBytes, &msg); err != nil {
			h.wsSendError(conn, 400, "invalid_message", "failed to parse message")
			continue
		}
		if msg.Type != "response.create" {
			h.wsSendError(conn, 400, "invalid_message_type", "expected response.create")
			continue
		}

		var rawMsg map[string]json.RawMessage
		if err := json.Unmarshal(msgBytes, &rawMsg); err != nil {
			h.wsSendError(conn, 400, "invalid_message", "failed to parse message")
			continue
		}
		// Strip the WebSocket protocol field before forwarding to inference.
		delete(rawMsg, "type")

		h.wsTurn(ctx, conn, cache, msg, rawMsg)
	}
}

// wsTurn processes one response.create turn.
func (h *Handler) wsTurn(ctx context.Context, conn *websocket.Conn, cache *wsCache, msg wsCreateMsg, rawMsg map[string]json.RawMessage) {
	createdAt := time.Now()

	inputItems, err := inputToItems(msg.Input)
	if err != nil {
		h.wsSendError(conn, 400, "invalid_input", "invalid input format")
		return
	}

	var reservationID string
	var flatCtx []json.RawMessage

	if msg.PreviousResponseID != nil {
		// Check connection-local cache first (for store:false responses).
		if cached, ok := cache.get(*msg.PreviousResponseID); ok {
			flatCtx = cached
		} else {
			reservationID, flatCtx, err = h.charon.Resolve(ctx, *msg.PreviousResponseID)
			if err != nil {
				h.wsSendError(conn, 400, "previous_response_not_found",
					"previous response not found")
				return
			}
		}
	}

	// Validate: function_call_output without a matching function_call is invalid.
	if hasOrphanedFunctionCallOutput(inputItems, flatCtx) {
		// Evict the previous response from cache — it's now stale.
		if msg.PreviousResponseID != nil {
			cache.evict(*msg.PreviousResponseID)
		}
		h.wsSendFailedTurn(conn, createdAt, msg)
		return
	}

	infMap := buildInferenceMap(rawMsg, flatCtx, inputItems)
	infMap["stream"] = json.RawMessage("true")

	ch, err := h.inf.Stream(ctx, infMap)
	if err != nil {
		h.log.Error("ws inference stream", "err", err)
		h.wsSendError(conn, 502, "inference_error", "inference backend error")
		return
	}

	seq := 0
	outIdx := 0
	var canonicalID string
	var sentCreated bool
	var finalInfResp *inference.Response

	var sw *charon.StreamWriter
	buf := &streamBuffer{}

	flushToCharonWS := func() {
		if !msg.ShouldStore() || canonicalID == "" {
			buf.drain()
			return
		}
		items := buf.drain()
		if len(items) == 0 {
			return
		}
		if sw == nil {
			sw = h.charon.NewStreamWriter(ctx, canonicalID)
		}
		if err := sw.Append(items); err != nil {
			h.log.Error("ws charon stream append", "id", canonicalID, "err", err)
		}
	}

	for evt := range ch {
		if evt.Response != nil && evt.Response.ID != "" && canonicalID == "" {
			canonicalID = evt.Response.ID
		}

		if !sentCreated && canonicalID != "" {
			placeholder := buildResponseResource(&inference.Response{
				ID:     canonicalID,
				Status: "in_progress",
				Model:  msg.Model,
				Output: []json.RawMessage{},
			}, msg.PreviousResponseID, msg.ShouldStore(), msg.Background, createdAt, nil)
			h.wsSend(conn, sseEvent{Type: "response.created", SequenceNumber: seq, Response: placeholder})
			seq++
			sentCreated = true
		}

		switch evt.Type {
		case "response.output_item.added":
			h.wsSend(conn, sseEvent{Type: evt.Type, SequenceNumber: seq, OutputIndex: &outIdx, Item: evt.Item})
			seq++
		case "response.output_item.done":
			h.wsSend(conn, sseEvent{Type: evt.Type, SequenceNumber: seq, OutputIndex: &outIdx, Item: evt.Item})
			seq++
			buf.add(evt.Item)
			if buf.shouldFlush(h.storeBufferBytes) {
				flushToCharonWS()
			}
		case "response.completed":
			finalInfResp = evt.Response

		default:
			if len(evt.Raw) == 0 {
				continue
			}
			var raw map[string]json.RawMessage
			if err := json.Unmarshal(evt.Raw, &raw); err != nil {
				continue
			}
			raw["sequence_number"] = json.RawMessage(strconv.Itoa(seq))
			b, err := json.Marshal(raw)
			if err != nil {
				continue
			}
			if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
				h.log.Debug("ws write default", "err", err)
			}
			seq++
		}
	}

	if finalInfResp == nil {
		return
	}

	if msg.ShouldStore() {
		var usage json.RawMessage
		if finalInfResp.Usage != nil {
			usage, _ = json.Marshal(finalInfResp.Usage)
		}
		if sw != nil {
			if err := sw.Commit(charon.CommitRequest{
				ReservationID:      reservationID,
				PreviousResponseID: msg.PreviousResponseID,
				Instructions:       msg.Instructions,
				Input:              inputItems,
				FinalItems:         buf.drain(),
				Usage:              usage,
				Status:             finalInfResp.Status,
				Model:              finalInfResp.Model,
				Background:         msg.Background,
			}); err != nil {
				h.log.Error("ws charon stream commit", "id", canonicalID, "err", err)
			}
		} else {
			storeReq := charon.StoreRequest{
				ReservationID:      reservationID,
				PreviousResponseID: msg.PreviousResponseID,
				Instructions:       msg.Instructions,
				Input:              inputItems,
				Output:             buf.drain(),
				Usage:              usage,
				Status:             finalInfResp.Status,
				Model:              finalInfResp.Model,
				Background:         msg.Background,
			}
			if err := h.charon.Store(ctx, canonicalID, storeReq); err != nil {
				h.log.Error("ws charon store", "id", canonicalID, "err", err)
			}
		}
	} else {
		// store:false — cache assembled flat_context for subsequent turns.
		newCtx := make([]json.RawMessage, 0, len(flatCtx)+len(inputItems)+len(finalInfResp.Output))
		newCtx = append(newCtx, flatCtx...)
		newCtx = append(newCtx, inputItems...)
		newCtx = append(newCtx, finalInfResp.Output...)
		cache.put(canonicalID, newCtx)
	}

	completedAt := time.Now()
	completed := buildResponseResource(finalInfResp, msg.PreviousResponseID, msg.ShouldStore(), msg.Background, createdAt, &completedAt)
	h.wsSend(conn, sseEvent{Type: "response.completed", SequenceNumber: seq, Response: completed})
}

func (h *Handler) wsSend(conn *websocket.Conn, v any) {
	b, _ := json.Marshal(v)
	if err := conn.WriteMessage(websocket.TextMessage, b); err != nil {
		h.log.Debug("ws write", "err", err)
	}
}

func (h *Handler) wsSendError(conn *websocket.Conn, status int, code, message string) {
	h.wsSend(conn, wsErrorEvent{
		Type:   "error",
		Status: status,
		Error:  &ResponseError{Code: code, Message: message},
	})
}

func (h *Handler) wsSendFailedTurn(conn *websocket.Conn, now time.Time, msg wsCreateMsg) {
	id := fmt.Sprintf("resp_err_%d", now.UnixNano())
	createdResource := &ResponseResource{
		ID:        id,
		Object:    "response",
		CreatedAt: now.Unix(),
		Status:    "in_progress",
		Model:     msg.Model,
		Output:    []json.RawMessage{},
		Tools:     []json.RawMessage{},
		Metadata:  map[string]string{},
	}
	failedResource := &ResponseResource{
		ID:        id,
		Object:    "response",
		CreatedAt: now.Unix(),
		Status:    "failed",
		Model:     msg.Model,
		Output:    []json.RawMessage{},
		Error:     &ResponseError{Code: "invalid_input", Message: "function_call_output without preceding function_call"},
		Tools:     []json.RawMessage{},
		Metadata:  map[string]string{},
	}
	h.wsSend(conn, sseEvent{Type: "response.created", SequenceNumber: 0, Response: createdResource})
	h.wsSend(conn, sseEvent{Type: "response.failed", SequenceNumber: 1, Response: failedResource})
}

// hasOrphanedFunctionCallOutput returns true if inputItems contains a
// function_call_output whose call_id has no matching function_call in flatCtx.
func hasOrphanedFunctionCallOutput(inputItems, flatCtx []json.RawMessage) bool {
	type header struct {
		Type   string `json:"type"`
		CallID string `json:"call_id"`
	}

	// Collect call_ids from function_call items in flat_context.
	knownCallIDs := make(map[string]bool)
	for _, item := range flatCtx {
		var h header
		if json.Unmarshal(item, &h) == nil && h.Type == "function_call" && h.CallID != "" {
			knownCallIDs[h.CallID] = true
		}
	}

	for _, item := range inputItems {
		var h header
		if json.Unmarshal(item, &h) == nil && h.Type == "function_call_output" {
			if !knownCallIDs[h.CallID] {
				return true
			}
		}
	}
	return false
}
