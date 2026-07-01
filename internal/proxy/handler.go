package proxy

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/httputil"
	"github.com/elevran/charon/internal/inference"
)

// StoreBufferUnbuffered is a sentinel for storeBufferBytes meaning "no buffering".
const StoreBufferUnbuffered = -1

// Handler is the client-facing Responses API proxy handler.
type Handler struct {
	charon           *charon.Client
	inf              *inference.Client
	log              *slog.Logger
	storeBufferBytes int // 0 = default(64K); StoreBufferUnbuffered = no buffering; N>0 = N byte threshold
}

// NewHandler creates a Handler.
// storeBufferBytes controls when the proxy flushes buffered output items to Charon:
//   - 0  → use default (65536 = 64 KB)
//   - StoreBufferUnbuffered (-1) → no buffering: flush every output item immediately
//   - N>0 → flush when accumulated item JSON reaches N bytes
func NewHandler(ch *charon.Client, inf *inference.Client, log *slog.Logger, storeBufferBytes int) *Handler {
	if storeBufferBytes == 0 {
		storeBufferBytes = 65536
	}
	return &Handler{charon: ch, inf: inf, log: log, storeBufferBytes: storeBufferBytes}
}

// RegisterHandlers mounts Responses API routes on mux.
func RegisterHandlers(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("POST /responses", h.HandleCreate)
	mux.HandleFunc("GET /responses/{id}", h.HandleRetrieve)
	mux.HandleFunc("DELETE /responses/{id}", h.HandleDelete)
	mux.HandleFunc("POST /responses/compact", h.HandleCompact)
	mux.HandleFunc("GET /responses", h.HandleListOrWS) // WebSocket upgrade added in Phase 7
}

// HandleCreate handles POST /responses.
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req CreateRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Model == "" {
		httputil.WriteError(w, http.StatusBadRequest, "model is required")
		return
	}

	var rawReq map[string]json.RawMessage
	if err := json.Unmarshal(bodyBytes, &rawReq); err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	// Stream mode handled separately.
	if req.Stream {
		h.handleStream(w, r, req, rawReq)
		return
	}

	ctx := r.Context()

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
	createdAt := time.Now()
	infResp, err := h.inf.Complete(ctx, infMap)
	if err != nil {
		h.log.Error("inference complete", "err", err)
		httputil.WriteError(w, http.StatusBadGateway, "inference error")
		return
	}
	completedAt := time.Now()

	if req.ShouldStore() {
		storeReq := charon.StoreRequest{
			ReservationID:      reservationID,
			PreviousResponseID: req.PreviousResponseID,
			Instructions:       req.Instructions,
			Input:              inputItems,
			Output:             infResp.Output,
			Status:             infResp.Status,
			Model:              infResp.Model,
			Background:         req.Background,
		}
		if err := h.charon.Store(ctx, infResp.ID, storeReq); err != nil {
			h.log.Error("charon store", "id", infResp.ID, "err", err)
			// TODO(chainstore wiring): a Pebble write failure is synchronous and durable;
			// consider returning 500 instead of treating storage failure as non-fatal.
		}
	}

	resource := buildResponseResource(infResp, req.PreviousResponseID, req.ShouldStore(), req.Background, createdAt, &completedAt)
	httputil.WriteJSON(w, http.StatusOK, resource)
}

// HandleRetrieve handles GET /responses/{id}.
func (h *Handler) HandleRetrieve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	retrieved, err := h.charon.Retrieve(r.Context(), id)
	if err != nil {
		h.mapCharonError(w, err, "not_found")
		return
	}

	// Build a ResponseResource from the stored record.
	resource := &ResponseResource{
		ID:                 retrieved.ID,
		Object:             "response",
		CreatedAt:          retrieved.CreatedAt,
		Status:             retrieved.Status,
		Model:              retrieved.Model,
		Background:         retrieved.Background,
		Instructions:       retrieved.Instructions,
		PreviousResponseID: retrieved.PreviousResponseID,
		Output:             retrieved.Output,
		Store:              true,
		Tools:              []json.RawMessage{},
		ToolChoice:         "auto",
		Truncation:         "disabled",
		Temperature:        1.0,
		TopP:               1.0,
		Metadata:           map[string]string{},
		ServiceTier:        "default",
	}
	if resource.Output == nil {
		resource.Output = []json.RawMessage{}
	}
	httputil.WriteJSON(w, http.StatusOK, resource)
}

// HandleDelete handles DELETE /responses/{id}.
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.charon.Delete(r.Context(), id); err != nil {
		h.mapCharonError(w, err, "not_found")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleCompact handles POST /responses/compact.
func (h *Handler) HandleCompact(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Model string `json:"model"`
	}
	_ = json.NewDecoder(r.Body).Decode(&body)
	if body.Model == "" {
		httputil.WriteError(w, http.StatusBadRequest, "model is required")
		return
	}
	httputil.WriteError(w, http.StatusNotImplemented, "compact not implemented")
}

// HandleListOrWS is implemented in ws.go.

// handleStream delegates to sse.go.

// --- helpers ---

func (h *Handler) mapCharonError(w http.ResponseWriter, err error, notFoundCode string) {
	switch {
	case errors.Is(err, charon.ErrNotFound):
		httputil.WriteJSON(w, http.StatusNotFound, map[string]string{
			"code":    notFoundCode,
			"message": err.Error(),
		})
	case errors.Is(err, charon.ErrChainCorrupted):
		httputil.WriteJSON(w, http.StatusConflict, map[string]string{
			"code":    "chain_corrupted",
			"message": err.Error(),
		})
	default:
		h.log.Error("charon error", "err", err)
		httputil.WriteError(w, http.StatusInternalServerError, "internal server error")
	}
}
