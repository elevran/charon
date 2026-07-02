package proxy

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/httputil"
	"github.com/elevran/charon/internal/inference"
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

// streamBuffer accumulates bare output item JSON (SSE framing already stripped)
// and decides when to flush to Charon.
type streamBuffer struct {
	items      []json.RawMessage
	totalBytes int
}

func (b *streamBuffer) add(item json.RawMessage) {
	b.items = append(b.items, item)
	b.totalBytes += len(item)
}

// shouldFlush returns true when the buffer is non-empty and either:
//   - limitBytes == StoreBufferUnbuffered (no buffering: flush every item), or
//   - limitBytes > 0 and accumulated bytes have reached the threshold.
func (b *streamBuffer) shouldFlush(limitBytes int) bool {
	if len(b.items) == 0 {
		return false
	}
	return limitBytes == StoreBufferUnbuffered || b.totalBytes >= limitBytes
}

func (b *streamBuffer) drain() []json.RawMessage {
	items := b.items
	b.items = nil
	b.totalBytes = 0
	return items
}

// handleStream implements POST /responses with stream:true.
//
// Output items are extracted from response.output_item.done events (bare item
// JSON, SSE framing stripped) and forwarded to Charon in batches controlled
// by h.storeBufferBytes. The client receives all SSE events in real-time
// regardless of buffering mode.
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
		httputil.WriteError(w, http.StatusBadRequest, "invalid input")
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

	infMap := buildInferenceMap(rawReq, flatCtx, inputItems)
	infMap["stream"] = json.RawMessage("true")

	ch, err := h.inf.Stream(ctx, infMap)
	if err != nil {
		h.log.Error("inference stream", "err", err)
		httputil.WriteError(w, http.StatusBadGateway, "inference error")
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

	var sw *charon.StreamWriter // created lazily on first store flush
	buf := &streamBuffer{}

	flushToCharon := func() {
		if !req.ShouldStore() || canonicalID == "" {
			buf.drain() // store:false — discard staged chunks
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
			h.log.Error("charon stream append", "id", canonicalID, "err", err)
		}
	}

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
			// Buffer bare item JSON for Charon (SSE envelope stripped).
			buf.add(evt.Item)
			if buf.shouldFlush(h.storeBufferBytes) {
				flushToCharon()
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
		var usage json.RawMessage
		if finalInfResp.Usage != nil {
			usage, _ = json.Marshal(finalInfResp.Usage)
		}
		if sw != nil {
			// Streaming path: commit with any remaining buffered items.
			if err := sw.Commit(charon.CommitRequest{
				ReservationID:      reservationID,
				PreviousResponseID: req.PreviousResponseID,
				Instructions:       req.Instructions,
				Input:              inputItems,
				FinalItems:         buf.drain(),
				Usage:              usage,
				Status:             finalInfResp.Status,
				Model:              finalInfResp.Model,
				Background:         req.Background,
			}); err != nil {
				h.log.Error("charon stream commit", "id", canonicalID, "err", err)
			}
		} else {
			// No mid-stream flushes happened (output fit within buffer).
			// Use a single Store call — no streaming overhead.
			storeReq := charon.StoreRequest{
				ReservationID:      reservationID,
				PreviousResponseID: req.PreviousResponseID,
				Instructions:       req.Instructions,
				Input:              inputItems,
				Output:             buf.drain(),
				Usage:              usage,
				Status:             finalInfResp.Status,
				Model:              finalInfResp.Model,
				Background:         req.Background,
			}
			if err := h.charon.Store(ctx, canonicalID, storeReq); err != nil {
				h.log.Error("charon store after stream", "id", canonicalID, "err", err)
			}
		}
	}

	completedAt := time.Now()
	completedResource := buildResponseResource(finalInfResp, req.PreviousResponseID, req.ShouldStore(), req.Background, createdAt, &completedAt)
	writeSSE(w, sseEvent{Type: "response.completed", SequenceNumber: seq, Response: completedResource})
}
