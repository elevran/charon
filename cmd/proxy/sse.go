package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/elevran/charon/cmd/proxy/inference"
	"github.com/elevran/charon/internal/server"
)

// sseEvent is the wire format of a single SSE event.
type sseEvent struct {
	Type           string            `json:"type"`
	SequenceNumber int               `json:"sequence_number"`
	Response       *ResponseResource `json:"response,omitempty"`
	OutputIndex    *int              `json:"output_index,omitempty"`
	Item           json.RawMessage   `json:"item,omitempty"`
}

// writeSSE writes a single SSE event and flushes.
func writeSSE(w http.ResponseWriter, evt sseEvent) {
	b, _ := json.Marshal(evt)
	_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// handleStream implements POST /responses with stream:true.
//
// Output items are extracted from response.output_item.done events (bare item
// JSON, SSE framing stripped) and buffered until response.completed, then
// committed to Charon as a single store call. The client receives all SSE
// events in real-time.
//
// Event sequence emitted to client:
//
//	seq=0  response.created      {response: {id, status:"in_progress", output:[], ...}}
//	seq=1  response.output_item.added  {output_index:0, item:{partial}}
//	seq=2  response.output_item.done   {output_index:0, item:{complete}}
//	seq=3  response.completed    {response: {id, status:"completed", output:[...], usage:{...}}}
func (h *Handler) handleStream(w http.ResponseWriter, r *http.Request, req CreateRequest, rawReq map[string]json.RawMessage) {
	ctx := r.Context()
	createdAt := time.Now()

	inputItems, err := inputToItems(req.Input)
	if err != nil {
		server.WriteError(w, http.StatusBadRequest, "invalid input")
		return
	}

	tenantKey := r.Header.Get("X-Tenant-Key")

	prevID := ""
	if req.PreviousResponseID != nil {
		prevID = *req.PreviousResponseID
	}
	// store:false turns use GET /chain (no commit); store:true turns use
	// POST /staging (commits request blob, returns staging_id for later
	// chunked writes). See handler.hydrateContext for details.
	flatCtx, stagingID, err := h.hydrateContext(ctx, prevID, tenantKey, inputItems, req.ShouldStore())
	if err != nil {
		h.mapCharonError(w, err, "previous_response_not_found")
		return
	}

	infMap := buildInferenceMap(rawReq, flatCtx, inputItems)
	infMap["stream"] = json.RawMessage("true")

	ch, err := h.inf.Stream(ctx, infMap)
	if err != nil {
		h.log.Error("inference stream", "err", err)
		server.WriteError(w, http.StatusBadGateway, "inference error")
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	seq := 0
	outIdx := 0
	var canonicalID string
	var created bool
	var finalInfResp *inference.Response
	var outputItems []json.RawMessage // accumulate output items for Charon store

	for evt := range ch {
		if evt.Response != nil && evt.Response.ID != "" && canonicalID == "" {
			canonicalID = evt.Response.ID
		}

		if !created && canonicalID != "" {
			placeholder := buildResponseResource(&inference.Response{
				ID:     canonicalID,
				Status: "in_progress",
				Model:  req.Model,
				Output: []json.RawMessage{},
			}, req.PreviousResponseID, req.ShouldStore(), req.Background, createdAt, nil)
			writeSSE(w, sseEvent{Type: "response.created", SequenceNumber: seq, Response: placeholder})
			seq++
			created = true
		}

		switch evt.Type {
		case "response.output_item.added":
			writeSSE(w, sseEvent{Type: evt.Type, SequenceNumber: seq, OutputIndex: &outIdx, Item: evt.Item})
			seq++

		case "response.output_item.done":
			// Forward to client immediately (no buffering of client events).
			writeSSE(w, sseEvent{Type: evt.Type, SequenceNumber: seq, OutputIndex: &outIdx, Item: evt.Item})
			seq++
			outputItems = append(outputItems, evt.Item)

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
			_, _ = fmt.Fprintf(w, "data: %s\n\n", b)
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			seq++
		}
	}

	if finalInfResp == nil {
		return
	}

	if req.ShouldStore() && canonicalID != "" {
		finalInfResp.Output = outputItems
		responseBlob := marshalStoredResponse(finalInfResp, req.PreviousResponseID, req.Instructions, req.Background)
		if err := h.commitStoredResponse(ctx, stagingID, canonicalID, tenantKey, responseBlob); err != nil {
			return // do not emit response.completed — staging was aborted
		}
	}

	completedAt := time.Now()
	completedResource := buildResponseResource(finalInfResp, req.PreviousResponseID, req.ShouldStore(), req.Background, createdAt, &completedAt)
	writeSSE(w, sseEvent{Type: "response.completed", SequenceNumber: seq, Response: completedResource})
}
