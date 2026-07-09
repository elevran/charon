package server

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/google/uuid"

	"github.com/elevran/charon/internal/chainstore"
)

// maxChunkBytes is the hard upper bound on a streaming-chunk PUT body.
// 1 MB default / 4 MB cap keeps the proxy→Charon ingest path bounded: small
// enough to stay under typical 256 MB per-process caps with 50+ concurrent
// inferences, large enough that per-batch HTTP framing overhead is small.
const (
	defaultMaxChunkBytes = 1 << 20
	maxChunkBytes        = 4 << 20
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

// writeErr maps a chainstore error to its HTTP status, logs at error level
// when the cause is unexpected (500), writes the standard error body, and
// returns so callers can `defer h.writeErr(...)` or use it as a tail return.
//
// op/id are diagnostic tags logged only on internal errors.
func (h *Handler) writeErr(w http.ResponseWriter, op, id string, err error) {
	status, msg := mapStatus(err)
	if status == http.StatusInternalServerError {
		h.log.Error(op, "id", id, "err", err)
	}
	WriteError(w, status, msg)
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
	case errors.Is(err, chainstore.ErrResponseIDTaken):
		return http.StatusConflict, "response_id conflict"
	case errors.Is(err, chainstore.ErrResponseIDRequired):
		return http.StatusBadRequest, "response_id required"
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

// resolveResponse is the JSON body returned by POST /staging?prev={id}.
type resolveResponse struct {
	StagingID string                `json:"staging_id"`
	Turns     []resolveResponseTurn `json:"turns"`
}

// chainResponse is the JSON body returned by GET /chain/{id}.
// Turns are root-first, matching the order returned by POST /staging.
type chainResponse struct {
	Turns []resolveResponseTurn `json:"turns"`
}

// bufferedStoreRequest is the JSON body accepted by POST /responses.
type bufferedStoreRequest struct {
	PrevID       string          `json:"prev_id"`
	ResponseID   string          `json:"response_id"`
	RequestBlob  json.RawMessage `json:"request_blob"`
	ResponseBlob json.RawMessage `json:"response_blob"`
}

// bufferedStoreResponse is the JSON body returned by POST /responses.
type bufferedStoreResponse struct {
	ResponseID string                `json:"response_id"`
	Turns      []resolveResponseTurn `json:"turns"`
}

// HandleBufferedStore handles POST /responses.
// Atomic (non-streaming) path: the proxy already holds both the request and
// response blobs and wants to commit them in a single round trip. The
// response_id may be supplied by the caller; when absent the server generates
// a UUID. Returns 201 Created with Location: /responses/<id> and a JSON body
// containing the assigned response_id and the resolved turn history.
func (h *Handler) HandleBufferedStore(w http.ResponseWriter, r *http.Request) {
	tenantKey := r.Header.Get("X-Tenant-Key")
	b, ok := h.readBodyOr400(w, r)
	if !ok {
		return
	}
	var req bufferedStoreRequest
	if err := json.Unmarshal(b, &req); err != nil {
		WriteError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	responseID := req.ResponseID
	if responseID == "" {
		responseID = uuid.NewString()
	}

	stagingID, turns, err := h.svc.ResolveAndStage(r.Context(), req.PrevID, tenantKey, []byte(req.RequestBlob))
	if err != nil {
		h.writeErr(w, "buffered-resolve", req.PrevID, err)
		return
	}

	if err := h.svc.StoreWithStaging(r.Context(), stagingID, responseID, "", tenantKey, []byte(req.ResponseBlob)); err != nil {
		h.writeErr(w, "buffered-store", responseID, err)
		return
	}

	respTurns := make([]resolveResponseTurn, len(turns))
	for i, t := range turns {
		respTurns[i] = resolveResponseTurn{
			RequestBlob:  blobToRaw(t.RequestBlob),
			ResponseBlob: blobToRaw(t.ResponseBlob),
		}
	}

	w.Header().Set("Location", "/responses/"+responseID)
	WriteJSON(w, http.StatusCreated, bufferedStoreResponse{
		ResponseID: responseID,
		Turns:      respTurns,
	})
}

// HandleGetChain handles GET /chain/{id}.
//
// Read-only chain fetch: walks the chain rooted at id (root-first) and
// returns each turn's (request_blob, response_blob) without creating or
// committing any staging state. The chain itself is unchanged from the
// caller's perspective — no new turn is persisted. Used by the proxy for
// store:false turns (and for buffered store:true turns, where the proxy
// wants context but intends to commit the request blob atomically via
// POST /responses rather than opening a staging record here).
//
// Side effects on the chainstore are limited to LRU metadata updates
// (LastAccessUnix, bucket promotion) — these are internal to eviction
// and do not affect the chain's content or shape.
func (h *Handler) HandleGetChain(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")

	turns, err := h.svc.Resolve(r.Context(), id, tenantKey)
	if err != nil {
		h.writeErr(w, "get-chain", id, err)
		return
	}

	respTurns := make([]resolveResponseTurn, len(turns))
	for i, t := range turns {
		respTurns[i] = resolveResponseTurn{
			RequestBlob:  blobToRaw(t.RequestBlob),
			ResponseBlob: blobToRaw(t.ResponseBlob),
		}
	}
	WriteJSON(w, http.StatusOK, chainResponse{Turns: respTurns})
}

// HandleOpenStaging handles POST /staging?prev={id}.
// Reads the raw request blob from the body, stages it, and returns the turn
// history root-first plus an opaque staging ID for the subsequent PUTs that
// append chunks. The optional "prev" query parameter specifies the previous
// response ID; when absent the request is a first-turn with no prior context.
// Returns 201 Created with Location: /staging/<stagingID>.
func (h *Handler) HandleOpenStaging(w http.ResponseWriter, r *http.Request) {
	prevID := r.URL.Query().Get("prev")
	tenantKey := r.Header.Get("X-Tenant-Key")

	if h.maxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	}
	requestBlob, err := io.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "failed to read request body")
		return
	}
	_ = r.Body.Close()

	stagingID, turns, err := h.svc.ResolveAndStage(r.Context(), prevID, tenantKey, requestBlob)
	if err != nil {
		h.writeErr(w, "resolve", prevID, err)
		return
	}

	respTurns := make([]resolveResponseTurn, len(turns))
	for i, t := range turns {
		respTurns[i] = resolveResponseTurn{
			RequestBlob:  blobToRaw(t.RequestBlob),
			ResponseBlob: blobToRaw(t.ResponseBlob),
		}
	}

	w.Header().Set("Location", "/staging/"+stagingID)
	w.Header().Set("X-Staging-ID", stagingID)
	WriteJSON(w, http.StatusCreated, resolveResponse{
		StagingID: stagingID,
		Turns:     respTurns,
	})
}

// HandleStagingStatus handles GET /staging/<id>.
//   - 200 OK with the assembled in-progress body while chunks are arriving.
//   - 410 Gone once the staging record has flipped to complete or aborted.
//     For complete records, the response includes Location: /responses/<id>
//     so the caller can follow it to the canonical final URL.
//   - 404 Not Found if no staging record was ever created for this id.
func (h *Handler) HandleStagingStatus(w http.ResponseWriter, r *http.Request) {
	stagingID := r.PathValue("id")
	if len(stagingID) > 64 {
		WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	ctx := r.Context()

	// Done-marker present: terminal state.
	doneID, err := h.svc.GetStagingDone(ctx, stagingID)
	if err == nil {
		if doneID != "" {
			// Completed: redirect to the canonical committed resource.
			w.Header().Set("Location", "/responses/"+doneID)
			w.WriteHeader(http.StatusSeeOther)
		} else {
			// Aborted or expired: resource is permanently gone.
			w.WriteHeader(http.StatusGone)
		}
		return
	}
	if !errors.Is(err, chainstore.ErrUnknownStaging) {
		h.writeErr(w, "staging-status", stagingID, err)
		return
	}

	// 200 path: in-progress. Read the assembled data and stream it.
	node, turn, err := h.svc.RetrieveStaging(ctx, stagingID)
	if err != nil {
		h.writeErr(w, "staging-retrieve", stagingID, err)
		return
	}
	pub := chainstore.PublicFromNode(node, h.svc.TTL())
	w.Header().Set("X-Created-At", strconv.FormatInt(pub.CreatedAt, 10))
	w.Header().Set("X-Depth", strconv.FormatUint(uint64(pub.Depth), 10))
	w.Header().Set("X-Status", strconv.FormatUint(uint64(pub.Status), 10))
	w.Header().Set("X-Staging-Response-ID", node.ResponseID)
	w.Header().Set("Content-Type", "application/octet-stream")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(turn.ResponseBlob)
}

// readBodyOr400 reads up to maxBodyBytes; on overflow or read error it
// writes a 400 and returns ok==false. The caller MUST stop processing
// when ok==false (the response is already written); the returned bytes
// are only valid when ok==true.
func (h *Handler) readBodyOr400(w http.ResponseWriter, r *http.Request) (b []byte, ok bool) {
	if h.maxBodyBytes > 0 {
		r.Body = http.MaxBytesReader(w, r.Body, h.maxBodyBytes)
	}
	var err error
	b, err = io.ReadAll(r.Body)
	_ = r.Body.Close()
	if err != nil {
		WriteError(w, http.StatusBadRequest, "failed to read request body")
		return nil, false
	}
	return b, true
}

// HandleAppendChunk handles PUT /staging/<stagingID>/chunks/<k>.
// The chunk number is in the URL path (NOT the query string) so the request
// is genuinely idempotent at the wire level — same URL = same chunk key =
// safe to retry. The body is one HTTP batch (1 MB default, 4 MB hard cap).
// Charon splits the body into ≤256 KB Pebble chunks starting at internal
// offset k.
//
// Server-validated ordering: a re-send with k < next_expected is treated as
// an idempotent replay (returns 200 OK with the current next_expected). A
// k > next_expected is a gap (returns 409 with ErrChunkOutOfRange and the
// expected k). k == next_expected writes the new chunk(s) atomically and
// returns 202 Accepted with the new next_expected.
//
// Early responseID binding: query param response_id=... on any PUT binds
// the staging record to that responseID. First binding wins; conflicting
// bindings return 409 (ErrResponseIDTaken). After binding, the response
// includes Location: /responses/<responseID>.
//
// Tenant isolation: the stagingID is a 128-bit random UUID; only the proxy
// that opened the turn knows it.
func (h *Handler) HandleAppendChunk(w http.ResponseWriter, r *http.Request) {
	stagingID := r.PathValue("id")
	kStr := r.PathValue("k")
	responseID := r.URL.Query().Get("response_id")

	if len(stagingID) > 64 {
		WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	if responseID != "" && len(responseID) > 255 {
		WriteError(w, http.StatusBadRequest, "response_id exceeds 255 bytes")
		return
	}
	k64, err := strconv.ParseUint(kStr, 10, 32)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "invalid chunk number")
		return
	}
	k := uint32(k64)

	// Cap per-chunk reads independently of the global maxBodyBytes. Default
	// 1 MB; configurable up to 4 MB via WithMaxChunkBytes.
	r.Body = http.MaxBytesReader(w, r.Body, defaultMaxChunkBytes)
	chunkBody, err := io.ReadAll(r.Body)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "failed to read chunk body")
		return
	}
	_ = r.Body.Close()
	if len(chunkBody) == 0 {
		WriteError(w, http.StatusBadRequest, "empty chunk body")
		return
	}

	// Early binding: separate transaction so a binding conflict is
	// reported cleanly without a partial chunk write.
	if responseID != "" {
		if err := h.svc.BindResponseID(r.Context(), stagingID, responseID); err != nil {
			h.writeErr(w, "bind-response-id", stagingID, err)
			return
		}
	}

	nextExpected, err := h.svc.AppendChunk(r.Context(), stagingID, k, chunkBody)
	if err != nil {
		h.writeErr(w, "append-chunk", stagingID, err)
		return
	}

	// If the proxy sent k < next_expected this was an idempotent replay;
	// surface that with 200 OK + the current next_expected. Otherwise
	// the chunk was newly written: 202 Accepted.
	status := http.StatusAccepted
	if k < nextExpected {
		status = http.StatusOK
	}
	WriteJSON(w, status, map[string]uint32{
		"received":      k,
		"expected_next": nextExpected,
	})
}

// HandleComplete handles PUT /staging/<stagingID>/complete?response_id=...&total=...
// The terminal commit call. Writes the manifest + final Node + deletes the
// staging record (and bound respidx) in one Pebble batch. Returns 201
// Created with Location: /responses/<final_id> and a body containing the
// final responseID.
func (h *Handler) HandleComplete(w http.ResponseWriter, r *http.Request) {
	stagingID := r.PathValue("id")
	responseID := r.URL.Query().Get("response_id")
	totalStr := r.URL.Query().Get("total")
	tenantKey := r.Header.Get("X-Tenant-Key")

	if len(stagingID) > 64 {
		WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	if totalStr == "" {
		WriteError(w, http.StatusBadRequest, "missing total")
		return
	}
	total, err := strconv.ParseUint(totalStr, 10, 32)
	if err != nil {
		WriteError(w, http.StatusBadRequest, "invalid total")
		return
	}
	if responseID != "" && len(responseID) > 255 {
		WriteError(w, http.StatusBadRequest, "response_id exceeds 255 bytes")
		return
	}

	finalID, err := h.svc.CompleteStreaming(r.Context(), stagingID, responseID, tenantKey, uint32(total))
	if err != nil {
		h.writeErr(w, "complete", stagingID, err)
		return
	}
	w.Header().Set("Location", "/responses/"+finalID)
	WriteJSON(w, http.StatusCreated, map[string]string{"response_id": finalID})
}

// HandleAbort handles PUT /staging/<stagingID>/abort.
// Marks the staging record as terminally failed. Deletes the staging
// record, all its chunks, and the respidx entry. Idempotent.
func (h *Handler) HandleAbort(w http.ResponseWriter, r *http.Request) {
	stagingID := r.PathValue("id")
	if len(stagingID) > 64 {
		WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	if err := h.svc.AbortStaging(r.Context(), stagingID); err != nil {
		h.writeErr(w, "abort", stagingID, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleRetrieve handles GET /responses/{id}.
// Returns the response blob as a raw body and exposes public node metadata in
// response headers.
func (h *Handler) HandleRetrieve(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")

	node, turn, err := h.svc.Retrieve(r.Context(), id, tenantKey)
	if err != nil {
		h.writeErr(w, "retrieve", id, err)
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
		h.writeErr(w, "delete", id, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleHealthz handles GET /healthz (liveness probe).
func (h *Handler) HandleHealthz(w http.ResponseWriter, _ *http.Request) {
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// HandleReadyz handles GET /readyz (readiness probe).
// Returns 503 if the storage backend is unreachable.
func (h *Handler) HandleReadyz(w http.ResponseWriter, r *http.Request) {
	if err := h.svc.Ping(r.Context()); err != nil {
		h.log.Error("readyz: storage ping failed", "err", err)
		WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "storage unavailable"})
		return
	}
	WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}
