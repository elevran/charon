package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"

	"github.com/openai/openai-go/responses"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/store"
)

// Handler wires ContextStore to HTTP endpoints.
type Handler struct {
	svc *store.ContextStore
	log *slog.Logger
}

// NewHandler creates a Handler.
func NewHandler(svc *store.ContextStore, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func mapStatus(err error) (int, string) {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return http.StatusNotFound, "not found"
	case errors.Is(err, storage.ErrChainCorrupted):
		return http.StatusConflict, "chain corrupted"
	case errors.Is(err, storage.ErrStoreFull):
		return http.StatusInsufficientStorage, "store full"
	default:
		return http.StatusInternalServerError, "internal server error"
	}
}

// HandleResolve handles GET /responses/{id}/context.
func (h *Handler) HandleResolve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	reservationID, flatContext, err := h.svc.Resolve(r.Context(), id)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("resolve", "id", id, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	writeJSON(w, http.StatusOK, model.ResolveResponse{
		ReservationID: reservationID,
		FlatContext:   flatContext,
	})
}

// HandleStore handles POST /responses/{id}.
func (h *Handler) HandleStore(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req model.StoreRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		_ = r.Body.Close()
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	_ = r.Body.Close()
	if err := h.svc.Store(r.Context(), id, req); err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("store", "id", id, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleRetrieve handles GET /responses/{id}.
func (h *Handler) HandleRetrieve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	meta, payload, err := h.svc.Retrieve(r.Context(), id)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("retrieve", "id", id, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	if meta.Status == model.StatusDeleted {
		h.log.Error("corrupt index: deleted record returned by retrieve", "id", meta.ID)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	input, err := unmarshalInputItems(payload.InputItems)
	if err != nil {
		h.log.Error("unmarshal input items", "id", id, "err", err)
		writeError(w, http.StatusInternalServerError, "internal server error")
		return
	}

	var usage *responses.ResponseUsage
	if len(payload.Usage) > 0 {
		usage = new(responses.ResponseUsage)
		if err := json.Unmarshal(payload.Usage, usage); err != nil {
			h.log.Error("unmarshal usage", "id", id, "err", err)
			writeError(w, http.StatusInternalServerError, "internal server error")
			return
		}
	}

	writeJSON(w, http.StatusOK, model.RetrieveResponse{
		ID:                 meta.ID,
		PreviousResponseID: meta.PreviousResponseID,
		Status:             responses.ResponseStatus(meta.Status),
		Model:              meta.Model,
		CreatedAt:          meta.CreatedAt,
		ExpiresAt:          meta.ExpiresAt,
		Input:              input,
		Output:             payload.OutputItems,
		Usage:              usage,
	})
}

// HandleAppendChunk handles PATCH /responses/{id}.
// Body type "chunk" appends output items to the in-progress stream stage.
// Body type "commit" finalises the stream and commits the response to storage.
func (h *Handler) HandleAppendChunk(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req model.ChunkRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	var err error
	switch req.Type {
	case "chunk":
		err = h.svc.AppendChunk(r.Context(), id, req.Seq, req.Items)
	case "commit":
		err = h.svc.CommitStream(r.Context(), id, req)
	default:
		writeError(w, http.StatusBadRequest, "unknown chunk type: must be 'chunk' or 'commit'")
		return
	}
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("append chunk", "id", id, "type", req.Type, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleDelete handles DELETE /responses/{id}.
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.svc.Delete(r.Context(), id); err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("delete", "id", id, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleHealthz handles GET /healthz (liveness probe).
func (h *Handler) HandleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// HandleReadyz handles GET /readyz (readiness probe).
// Returns 503 if the storage backend is unreachable.
func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Ping(r.Context()); err != nil {
		h.log.Error("readyz: storage ping failed", "err", err)
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "storage unavailable"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func unmarshalInputItems(items []json.RawMessage) (responses.ResponseInputParam, error) {
	params := make(responses.ResponseInputParam, len(items))
	for i, raw := range items {
		if err := json.Unmarshal(raw, &params[i]); err != nil {
			return nil, err
		}
	}
	return params, nil
}
