package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/elevran/charon/cmd/proxy/inference"
	"github.com/elevran/charon/internal/server"
	"github.com/elevran/charon/pkg/charon"
)

// Handler is the client-facing Responses API proxy handler.
type Handler struct {
	charon charon.Backend
	inf    inference.Backend
	log    *slog.Logger
}

// NewHandler creates a Handler.
func NewHandler(ch charon.Backend, inf inference.Backend, log *slog.Logger) *Handler {
	return &Handler{charon: ch, inf: inf, log: log}
}

// RegisterHandlers mounts Responses API routes on mux.
// All requests that do not match a /responses route are reverse-proxied
// verbatim to the inference backend.
func RegisterHandlers(mux *http.ServeMux, h *Handler) {
	mux.HandleFunc("POST /responses", h.HandleCreate)
	mux.HandleFunc("GET /responses/{id}", h.HandleRetrieve)
	mux.HandleFunc("DELETE /responses/{id}", h.HandleDelete)
	mux.HandleFunc("POST /responses/compact", h.HandleCompact)
	mux.HandleFunc("GET /responses", h.HandleListOrWS) // WebSocket upgrade added in Phase 7

	// Catch-all: forward everything not matched above (e.g. GET /models,
	// GET /model/info) to the inference backend verbatim.
	// Any future proxy-owned paths (e.g. /healthz) must be registered
	// before this line so the more-specific pattern wins.
	mux.Handle("/", newPassthroughProxy(h.inf.BaseURL(), h.log))
}

// HandleCreate handles POST /responses.
func (h *Handler) HandleCreate(w http.ResponseWriter, r *http.Request) {
	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		server.WriteError(w, http.StatusBadRequest, "failed to read request body")
		return
	}

	var req CreateRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		server.WriteError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	if req.Model == "" {
		server.WriteError(w, http.StatusBadRequest, "model is required")
		return
	}

	var rawReq map[string]json.RawMessage
	if err := json.Unmarshal(bodyBytes, &rawReq); err != nil {
		server.WriteError(w, http.StatusBadRequest, "invalid request body")
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
		server.WriteError(w, http.StatusBadRequest, "invalid input")
		return
	}

	tenantKey := r.Header.Get("X-Tenant-Key")

	prevID := ""
	if req.PreviousResponseID != nil {
		prevID = *req.PreviousResponseID
	}
	// store:false turns hydrate context via GET /chain (no commit of the
	// current turn's request blob); store:true turns open a staging record
	// via POST /staging (commits the request blob, returns staging_id).
	// The shared helper handles both branches.
	flatCtx, stagingID, err := h.hydrateContext(ctx, prevID, tenantKey, inputItems, req.ShouldStore())
	if err != nil {
		h.mapCharonError(w, err, "previous_response_not_found")
		return
	}

	infMap := buildInferenceMap(rawReq, flatCtx, inputItems)
	createdAt := time.Now()
	infResp, err := h.inf.Complete(ctx, infMap)
	if err != nil {
		h.log.Error("inference complete", "err", err)
		server.WriteError(w, http.StatusBadGateway, "inference error")
		return
	}
	completedAt := time.Now()

	if req.ShouldStore() {
		responseBlob := marshalStoredResponse(infResp, req.PreviousResponseID, req.Instructions, req.Background)
		if err := h.charon.Store(ctx, infResp.ID, stagingID, tenantKey, responseBlob); err != nil {
			h.log.Error("charon store", "id", infResp.ID, "err", err)
			server.WriteError(w, http.StatusInternalServerError, "storage error")
			return
		}
	}

	resource := buildResponseResource(infResp, req.PreviousResponseID, req.ShouldStore(), req.Background, createdAt, &completedAt)
	server.WriteJSON(w, http.StatusOK, resource)
}

// HandleRetrieve handles GET /responses/{id}.
func (h *Handler) HandleRetrieve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")

	retrieved, err := h.charon.Retrieve(r.Context(), id, tenantKey)
	if err != nil {
		h.mapCharonError(w, err, "not_found")
		return
	}

	var stored storedResponse
	if err := json.Unmarshal(retrieved.ResponseBlob, &stored); err != nil {
		h.log.Error("unmarshal stored response", "id", id, "err", err)
		server.WriteError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	output := stored.Output
	if output == nil {
		output = []json.RawMessage{}
	}
	resource := &ResponseResource{
		ID:                 stored.ID,
		Object:             "response",
		CreatedAt:          retrieved.CreatedAt,
		Status:             stored.Status,
		Model:              stored.Model,
		Background:         stored.Background,
		Instructions:       stored.Instructions,
		PreviousResponseID: stored.PreviousResponseID,
		Output:             output,
		Store:              true,
		Tools:              []json.RawMessage{},
		ToolChoice:         "auto",
		Truncation:         "disabled",
		Temperature:        1.0,
		TopP:               1.0,
		Metadata:           map[string]string{},
		ServiceTier:        "default",
	}
	server.WriteJSON(w, http.StatusOK, resource)
}

// HandleDelete handles DELETE /responses/{id}.
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")
	if err := h.charon.Delete(r.Context(), id, tenantKey); err != nil {
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
		server.WriteError(w, http.StatusBadRequest, "model is required")
		return
	}
	server.WriteError(w, http.StatusNotImplemented, "compact not implemented")
}

// HandleListOrWS is implemented in ws.go.

// handleStream delegates to sse.go.

// --- helpers ---

// hydrateContext fetches the prior-context items for a turn, choosing the
// Charon entry call based on whether the current turn will be persisted.
//
//   - shouldStore == true → POST /staging?prev=<id> (carries the request
//     blob as the body and returns a staging_id for the subsequent
//     PutChunks + Complete sequence). The returned staging_id is "" when
//     prevID is empty (first turn still gets a staging id, but callers
//     that don't need a stage-only response can ignore it).
//   - shouldStore == false → GET /chain/<id> (read-only, no commit). The
//     returned staging_id is always "" — store:false turns never produce
//     a staging record.
//
// In both branches the returned flatCtx is a JSON-flat projection of the
// chain rooted at prevID, ready to be merged with the current turn's
// inputItems and forwarded to the inference backend.
func (h *Handler) hydrateContext(ctx context.Context, prevID, tenantKey string, inputItems []json.RawMessage, shouldStore bool) (flatCtx []json.RawMessage, stagingID string, err error) {
	if shouldStore {
		requestBlob, _ := json.Marshal(storedRequest{Input: inputItems})
		var turns []charon.ResolveTurn
		stagingID, turns, err = h.charon.Resolve(ctx, prevID, tenantKey, requestBlob)
		if err != nil {
			return nil, "", err
		}
		flatCtx = turnsToFlatCtx(turns)
		return flatCtx, stagingID, nil
	}

	if prevID == "" {
		// First turn, store:false: nothing to fetch.
		return nil, "", nil
	}
	turns, err := h.charon.GetChain(ctx, prevID, tenantKey)
	if err != nil {
		return nil, "", err
	}
	flatCtx = turnsToFlatCtx(turns)
	return flatCtx, "", nil
}

func (h *Handler) mapCharonError(w http.ResponseWriter, err error, notFoundCode string) {
	switch {
	case errors.Is(err, charon.ErrNotFound):
		server.WriteJSON(w, http.StatusNotFound, map[string]string{
			"code":    notFoundCode,
			"message": err.Error(),
		})
	case errors.Is(err, charon.ErrChainCorrupted):
		server.WriteJSON(w, http.StatusConflict, map[string]string{
			"code":    "chain_corrupted",
			"message": err.Error(),
		})
	default:
		h.log.Error("charon error", "err", err)
		server.WriteError(w, http.StatusInternalServerError, "internal server error")
	}
}
