package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
)

// sseEvent is the wire format of a single SSE event.
type sseEvent struct {
	Type           string           `json:"type"`
	SequenceNumber int              `json:"sequence_number"`
	Response       *ResponseResource `json:"response,omitempty"`
	OutputIndex    *int             `json:"output_index,omitempty"`
	Item           json.RawMessage  `json:"item,omitempty"`
}

// writeSSE writes a single SSE event and flushes. The connection must support
// http.Flusher (all Go HTTP response writers do in practice).
func writeSSE(w http.ResponseWriter, evt sseEvent) {
	b, _ := json.Marshal(evt)
	fmt.Fprintf(w, "data: %s\n\n", b)
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// handleStream implements POST /responses with stream:true.
//
// Event sequence emitted to client:
//
//	seq=0  response.created      {response: {id, status:"in_progress", output:[], ...}}
//	seq=1  response.output_item.added  {output_index:0, item:{partial}}
//	seq=2  response.output_item.done   {output_index:0, item:{complete}}
//	seq=3  response.completed    {response: {id, status:"completed", output:[...], usage:{...}}}
func (h *Handler) handleStream(w http.ResponseWriter, r *http.Request, req CreateRequest) {
	ctx := r.Context()
	now := time.Now()

	inputItems, err := inputToItems(req.Input)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid input")
		return
	}

	var reservationID string
	var flatCtx []json.RawMessage

	if req.PreviousResponseID != nil {
		reservationID, flatCtx, err = h.charon.Resolve(ctx, *req.PreviousResponseID)
		if err != nil {
			h.mapCharonError(w, err, "previous_response_not_found")
			return
		}
	}

	infReq := buildInferenceRequest(req, flatCtx, inputItems)
	infReq.Stream = true

	ch, err := h.inf.Stream(ctx, infReq)
	if err != nil {
		h.log.Error("inference stream", "err", err)
		writeError(w, http.StatusBadGateway, "inference error")
		return
	}

	// Set SSE headers before writing any body.
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")

	seq := 0
	var canonicalID string
	var finalInfResp *inference.Response

	// Emit response.created with a placeholder — we don't have the ID yet.
	// We'll use the ID from the first event.
	var created bool

	outIdx := 0

	for evt := range ch {
		// On first event we learn the canonical ID.
		if evt.Response != nil && evt.Response.ID != "" && canonicalID == "" {
			canonicalID = evt.Response.ID
		}

		if !created && canonicalID != "" {
			placeholder := buildResponseResource(&inference.Response{
				ID:     canonicalID,
				Status: "in_progress",
				Model:  req.Model,
				Output: []json.RawMessage{},
			}, req.PreviousResponseID, req.ShouldStore(), now)
			writeSSE(w, sseEvent{Type: "response.created", SequenceNumber: seq, Response: placeholder})
			seq++
			created = true
		}

		switch evt.Type {
		case "response.output_item.added":
			writeSSE(w, sseEvent{
				Type:           "response.output_item.added",
				SequenceNumber: seq,
				OutputIndex:    &outIdx,
				Item:           evt.Item,
			})
			seq++
		case "response.output_item.done":
			writeSSE(w, sseEvent{
				Type:           "response.output_item.done",
				SequenceNumber: seq,
				OutputIndex:    &outIdx,
				Item:           evt.Item,
			})
			seq++
		case "response.completed":
			finalInfResp = evt.Response
		}
	}

	if finalInfResp == nil {
		return
	}

	if req.ShouldStore() && canonicalID != "" {
		storeReq := charon.StoreRequest{
			ReservationID:      reservationID,
			PreviousResponseID: req.PreviousResponseID,
			Input:              inputItems,
			Output:             finalInfResp.Output,
			Status:             finalInfResp.Status,
			Model:              finalInfResp.Model,
		}
		if err := h.charon.Store(ctx, canonicalID, storeReq); err != nil {
			h.log.Error("charon store after stream", "id", canonicalID, "err", err)
		}
	}

	completedResource := buildResponseResource(finalInfResp, req.PreviousResponseID, req.ShouldStore(), now)
	writeSSE(w, sseEvent{Type: "response.completed", SequenceNumber: seq, Response: completedResource})
}
