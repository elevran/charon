package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"

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
	case errors.Is(err, storage.ErrChainTooDeep):
		return http.StatusUnprocessableEntity, "chain_too_deep"
	case errors.Is(err, storage.ErrContextTooLarge):
		return http.StatusUnprocessableEntity, "context_too_large"
	default:
		return http.StatusInternalServerError, "internal server error"
	}
}

// HandleResolve handles GET /responses/{id}/context.
// Accepts an optional max_bytes query parameter (integer bytes, e.g. ?max_bytes=1048576) to cap
// the assembled context size. Unlike the storage.max_context_bytes config field, this parameter
// does not accept unit suffixes (KB, MB, GB) — only bare integers are valid.
func (h *Handler) HandleResolve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var maxBytes int64
	if s := r.URL.Query().Get("max_bytes"); s != "" {
		v, err := strconv.ParseInt(s, 10, 64)
		if err != nil || v <= 0 {
			writeError(w, http.StatusBadRequest, "max_bytes must be a positive integer")
			return
		}
		maxBytes = v
	}

	reservationID, flatContext, err := h.svc.Resolve(r.Context(), id, maxBytes)
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
		Instructions:       payload.Instructions,
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

// HandleListInputItems handles GET /responses/{id}/input_items.
// Supports ?after=<cursor>&limit=<n> pagination. All input items are returned
// as stored; compaction items are not filtered because they represent prior
// context that was folded into this turn's input.
func (h *Handler) HandleListInputItems(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, payload, err := h.svc.Retrieve(r.Context(), id)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("retrieve for input_items", "id", id, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	page, parseErr := paginateItems(r, payload.InputItems)
	if parseErr != nil {
		writeError(w, http.StatusBadRequest, parseErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, page)
}

// HandleListOutputItems handles GET /responses/{id}/output_items.
// Supports ?after=<cursor>&limit=<n> pagination. Compaction items are excluded.
func (h *Handler) HandleListOutputItems(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	_, payload, err := h.svc.Retrieve(r.Context(), id)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("retrieve for output_items", "id", id, "err", err)
		}
		writeError(w, status, msg)
		return
	}
	page, parseErr := paginateItems(r, filterCompactionItems(payload.OutputItems))
	if parseErr != nil {
		writeError(w, http.StatusBadRequest, parseErr.Error())
		return
	}
	writeJSON(w, http.StatusOK, page)
}

const defaultPageLimit = 100

// paginateItems applies ?after and ?limit to a slice of items and returns an ItemsPage.
func paginateItems(r *http.Request, items []json.RawMessage) (model.ItemsPage, error) {
	limit := defaultPageLimit
	if s := r.URL.Query().Get("limit"); s != "" {
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			return model.ItemsPage{}, fmt.Errorf("limit must be a positive integer")
		}
		if n > defaultPageLimit {
			n = defaultPageLimit
		}
		limit = n
	}

	start := 0
	if after := r.URL.Query().Get("after"); after != "" {
		dec, err := base64.StdEncoding.DecodeString(after)
		if err != nil {
			return model.ItemsPage{}, fmt.Errorf("invalid cursor")
		}
		idx, err := strconv.Atoi(string(dec))
		if err != nil || idx < 0 {
			return model.ItemsPage{}, fmt.Errorf("invalid cursor")
		}
		start = idx
	}

	if start >= len(items) {
		return model.ItemsPage{Items: []json.RawMessage{}}, nil
	}

	end := start + limit
	hasMore := end < len(items)
	if end > len(items) {
		end = len(items)
	}

	page := model.ItemsPage{
		Items:   items[start:end],
		HasMore: hasMore,
	}
	if hasMore {
		cursor := base64.StdEncoding.EncodeToString([]byte(strconv.Itoa(end)))
		page.NextCursor = &cursor
	}
	return page, nil
}

// filterCompactionItems returns items with compaction-type items removed.
func filterCompactionItems(items []json.RawMessage) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(items))
	for _, raw := range items {
		var t struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(raw, &t); err != nil || t.Type == string(model.ItemTypeCompaction) {
			continue
		}
		out = append(out, raw)
	}
	return out
}
