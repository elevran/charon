package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/elevran/charon/internal/chainstore"
	"github.com/elevran/charon/internal/httputil"
)

// Handler wires chainstore.Store to HTTP endpoints.
type Handler struct {
	svc          *chainstore.Store
	log          *slog.Logger
	maxBodyBytes int64
}

// NewHandler creates a Handler.
func NewHandler(svc *chainstore.Store, log *slog.Logger) *Handler {
	return &Handler{svc: svc, log: log}
}

// WithMaxBodyBytes sets the per-request body size cap. When non-zero, bodies
// larger than n bytes are rejected with 413. Wire from chainstore.Config.MaxBytes.
func (h *Handler) WithMaxBodyBytes(n int64) *Handler {
	h.maxBodyBytes = n
	return h
}

// blobToRaw converts a raw-bytes blob to json.RawMessage for the wire format.
// Empty (but non-nil) blobs are treated as absent and returned as nil, which
// marshals as JSON null, to avoid producing invalid JSON from empty byte slices.
func blobToRaw(b []byte) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	return json.RawMessage(b)
}

func mapStatus(err error) (int, string) {
	switch {
	case errors.Is(err, chainstore.ErrNotFound):
		return http.StatusNotFound, "not found"
	case errors.Is(err, chainstore.ErrChainCorrupted):
		return http.StatusConflict, "chain corrupted"
	case errors.Is(err, chainstore.ErrChainExpired):
		return http.StatusConflict, "chain expired"
	case errors.Is(err, chainstore.ErrStoreFull):
		return http.StatusInsufficientStorage, "store full"
	case errors.Is(err, chainstore.ErrUnknownStaging):
		return http.StatusUnprocessableEntity, "unknown staging id"
	case errors.Is(err, chainstore.ErrChainTooDeep):
		return http.StatusUnprocessableEntity, "chain too deep"
	default:
		return http.StatusInternalServerError, "internal server error"
	}
}

// resolveResponseTurn is one turn in the resolve JSON response.
type resolveResponseTurn struct {
	RequestBlob  json.RawMessage `json:"request_blob"`
	ResponseBlob json.RawMessage `json:"response_blob"`
}

// resolveResponse is the JSON body returned by POST /responses?prev={id}.
type resolveResponse struct {
	StagingID string                `json:"staging_id"`
	Turns     []resolveResponseTurn `json:"turns"`
}

// HandleResolve handles POST /responses?prev={id}.
// Reads the raw request blob from the body, stages it, and returns the turn
// history root-first plus an opaque staging ID for the subsequent store call.
// The optional "prev" query parameter specifies the previous response ID;
// when absent the request is a first-turn with no prior context.
func (h *Handler) HandleResolve(w http.ResponseWriter, r *http.Request) {
	prevID := r.URL.Query().Get("prev")
	tenantKey := r.Header.Get("X-Tenant-Key")

	if h.maxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	}
	requestBlob, err := io.ReadAll(r.Body)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	_ = r.Body.Close()

	stagingID, turns, err := h.svc.ResolveAndStage(r.Context(), prevID, tenantKey, requestBlob)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("resolve", "prev_id", prevID, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}

	respTurns := make([]resolveResponseTurn, len(turns))
	for i, t := range turns {
		respTurns[i] = resolveResponseTurn{
			RequestBlob:  blobToRaw(t.RequestBlob),
			ResponseBlob: blobToRaw(t.ResponseBlob),
		}
	}

	w.Header().Set("X-Staging-ID", stagingID)
	httputil.WriteJSON(w, http.StatusOK, resolveResponse{
		StagingID: stagingID,
		Turns:     respTurns,
	})
}

// HandleStore handles POST /responses/{id}.
// Reads raw response blob from the body and the staging ID from the "req" query
// parameter, then durably commits the turn.
func (h *Handler) HandleStore(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")
	stagingID := r.URL.Query().Get("req")
	if len(stagingID) > 64 {
		httputil.WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}

	if h.maxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	}
	responseBlob, err := io.ReadAll(r.Body)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	_ = r.Body.Close()

	if err := h.svc.StoreWithStaging(r.Context(), stagingID, id, "", tenantKey, responseBlob); err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("store", "id", id, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleRetrieve handles GET /responses/{id}.
// Returns the response blob as a raw body and exposes public node metadata in
// response headers.
func (h *Handler) HandleRetrieve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")

	node, turn, err := h.svc.Retrieve(r.Context(), id, tenantKey)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("retrieve", "id", id, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}

	pub := chainstore.PublicFromNode(node, h.svc.TTL())
	w.Header().Set("X-Created-At", strconv.FormatInt(pub.CreatedAt, 10))
	w.Header().Set("X-Expires-At", strconv.FormatInt(pub.ExpiresAt, 10))
	w.Header().Set("X-Depth", strconv.FormatUint(uint64(pub.Depth), 10))
	w.Header().Set("X-Status", strconv.FormatUint(uint64(pub.Status), 10))
	w.Header().Set("X-Version", strconv.FormatUint(uint64(pub.Version), 10))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(turn.ResponseBlob)
}

// HandleDelete handles DELETE /responses/{id}.
func (h *Handler) HandleDelete(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")

	if err := h.svc.Delete(r.Context(), id, tenantKey, false); err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("delete", "id", id, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleHealthz handles GET /healthz (liveness probe).
func (h *Handler) HandleHealthz(w http.ResponseWriter, _ *http.Request) {
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// HandleReadyz handles GET /readyz (readiness probe).
// Returns 503 if the storage backend is unreachable.
func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Ping(r.Context()); err != nil {
		h.log.Error("readyz: storage ping failed", "err", err)
		httputil.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "storage unavailable"})
		return
	}
	httputil.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
