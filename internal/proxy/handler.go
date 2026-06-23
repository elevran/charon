package proxy

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/elevran/charon/internal/charon"
	"github.com/elevran/charon/internal/inference"
)

// Handler is the client-facing Responses API proxy handler.
type Handler struct {
	charon           *charon.Client
	inf              *inference.Client
	log              *slog.Logger
	storeBufferBytes int // 0 = default(64K); -1 = no buffering; N>0 = N byte threshold
}

// NewHandler creates a Handler.
// storeBufferBytes controls when the proxy flushes buffered output items to Charon:
//   - 0  → use default (65536 = 64 KB)
//   - -1 → no buffering: flush every output item immediately
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
	var req CreateRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Model == "" {
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}

	// Stream mode handled separately.
	if req.Stream {
		h.handleStream(w, r, req)
		return
	}

	ctx := r.Context()

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
	createdAt := time.Now()
	infResp, err := h.inf.Complete(ctx, infReq)
	if err != nil {
		h.log.Error("inference complete", "err", err)
		writeError(w, http.StatusBadGateway, "inference error")
		return
	}
	completedAt := time.Now()

	if req.ShouldStore() {
		storeReq := charon.StoreRequest{
			ReservationID:      reservationID,
			PreviousResponseID: req.PreviousResponseID,
			Input:              inputItems,
			Output:             infResp.Output,
			Status:             infResp.Status,
			Model:              infResp.Model,
		}
		if err := h.charon.Store(ctx, infResp.ID, storeReq); err != nil {
			h.log.Error("charon store", "id", infResp.ID, "err", err)
			// Non-fatal: response was generated, storage failed.
		}
	}

	resource := buildResponseResource(infResp, req.PreviousResponseID, req.ShouldStore(), createdAt, &completedAt)
	writeJSON(w, http.StatusOK, resource)
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
	writeJSON(w, http.StatusOK, resource)
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
		writeError(w, http.StatusBadRequest, "model is required")
		return
	}
	writeError(w, http.StatusNotImplemented, "compact not implemented")
}

// HandleListOrWS is implemented in ws.go.

// handleStream delegates to sse.go.

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func (h *Handler) mapCharonError(w http.ResponseWriter, err error, notFoundCode string) {
	switch {
	case errors.Is(err, charon.ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{
			"code":    notFoundCode,
			"message": err.Error(),
		})
	case errors.Is(err, charon.ErrChainCorrupted):
		writeJSON(w, http.StatusConflict, map[string]string{
			"code":    "chain_corrupted",
			"message": err.Error(),
		})
	default:
		h.log.Error("charon error", "err", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
	}
}
