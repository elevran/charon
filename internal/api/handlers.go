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

// defaultMaxChunkBytes is the per-PUT chunk size used when the handler is
// not configured via WithMaxChunkBytes. 1 MB is the safe-default body size
// for proxy→Charon streaming ingest: small enough that 50+ concurrent
// inferences on a single host stay well under typical 256 MB per-process
// caps, large enough that the per-batch HTTP framing overhead is small.
const defaultMaxChunkBytes = 1 << 20

// maxChunkBytesCap is the hard upper bound for the configured chunk limit.
// Configured limits above this are clamped. 4 MB is the proxy→Charon body
// maximum — anything larger risks unbounded memory growth and a single
// body that exceeds typical reverse-proxy body limits.
const maxChunkBytesCap = 4 << 20

// Handler wires chainstore.Store to HTTP endpoints.
type Handler struct {
	svc           *chainstore.Store
	log           *slog.Logger
	maxBodyBytes  int64
	maxChunkBytes int64
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

// WithMaxChunkBytes overrides the per-streaming-chunk PUT body cap (default
// 1 MB, hard cap 4 MB). Use this to tighten or relax the proxy→Charon
// memory budget during streaming ingest. Independent of maxBodyBytes which
// caps the legacy POST (and can be GBs).
func (h *Handler) WithMaxChunkBytes(n int64) *Handler {
	if n > maxChunkBytesCap {
		n = maxChunkBytesCap
	}
	h.maxChunkBytes = n
	return h
}

// chunkLimit returns the configured chunk limit or the default.
func (h *Handler) chunkLimit() int64 {
	if h.maxChunkBytes > 0 {
		return h.maxChunkBytes
	}
	return defaultMaxChunkBytes
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

// resolveResponse is the JSON body returned by POST /responses?prev={id}.
type resolveResponse struct {
	StagingID string                `json:"staging_id"`
	Turns     []resolveResponseTurn `json:"turns"`
}

// HandleResolve handles POST /responses?prev={id} (back-compat alias for
// POST /responses/staging).
//
// Reads the raw request blob from the body, stages it, and returns the turn
// history root-first plus an opaque staging ID for the subsequent PUTs that
// append chunks. The optional "prev" query parameter specifies the previous
// response ID; when absent the request is a first-turn with no prior context.
//
// Returns 201 Created with Location: /responses/staging/<stagingID> and a
// JSON body containing the staging_id and the resolved turns.
func (h *Handler) HandleResolve(w http.ResponseWriter, r *http.Request) {
	h.openStaging(w, r)
}

// HandleOpenStaging handles POST /responses/staging (canonical path).
// Same semantics as HandleResolve; routed via /responses/staging/<id> for
// subsequent PUTs.
func (h *Handler) HandleOpenStaging(w http.ResponseWriter, r *http.Request) {
	h.openStaging(w, r)
}

func (h *Handler) openStaging(w http.ResponseWriter, r *http.Request) {
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

	w.Header().Set("Location", "/responses/staging/"+stagingID)
	w.Header().Set("X-Staging-ID", stagingID)
	httputil.WriteJSON(w, http.StatusCreated, resolveResponse{
		StagingID: stagingID,
		Turns:     respTurns,
	})
}

// HandleStagingStatus handles GET /responses/staging/<id>.
//   - 200 OK with the assembled in-progress body while chunks are arriving.
//   - 410 Gone once the staging record has flipped to complete or aborted.
//     For complete records, the response includes Location: /responses/<id>
//     so the caller can follow it to the canonical final URL.
//   - 404 Not Found if no staging record was ever created for this id.
func (h *Handler) HandleStagingStatus(w http.ResponseWriter, r *http.Request) {
	stagingID := r.PathValue("id")
	if len(stagingID) > 64 {
		httputil.WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	ctx := r.Context()

	// 410 path: done-marker present (complete or aborted).
	doneID, err := h.svc.GetStagingDone(ctx, stagingID)
	if err == nil {
		if doneID != "" {
			w.Header().Set("Location", "/responses/"+doneID)
		}
		w.WriteHeader(http.StatusGone)
		return
	}
	if !errors.Is(err, chainstore.ErrUnknownStaging) {
		status, msg := mapStatus(err)
		httputil.WriteError(w, status, msg)
		return
	}

	// 200 path: in-progress. Read the assembled data and stream it.
	node, turn, err := h.svc.RetrieveStaging(ctx, stagingID)
	if err != nil {
		status, msg := mapStatus(err)
		httputil.WriteError(w, status, msg)
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

// HandleStore handles POST /responses/{id}.
// Reads raw response blob from the body and the staging ID from the "req" query
// parameter, then durably commits the turn.
//
// Streaming variant: callers that PUT batches via HandleAppendChunk signal a
// chunked commit either explicitly via ?chunks=N&total=M on this POST, or
// implicitly by leaving those off and relying on the staging record to
// already carry pre-existing chunks. The body is ignored for chunked
// commits — chunks under the staging record's ResponseBlobID are the
// source of truth.
func (h *Handler) HandleStore(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	tenantKey := r.Header.Get("X-Tenant-Key")
	stagingID := r.URL.Query().Get("req")
	q := r.URL.Query()
	chunksQ := q.Get("chunks")
	totalQ := q.Get("total")
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

	var commitErr error
	switch {
	case chunksQ != "":
		// Streaming commit: client supplied chunk count and total size.
		chunks, parseErr := strconv.ParseUint(chunksQ, 10, 32)
		if parseErr != nil {
			httputil.WriteError(w, http.StatusBadRequest, "invalid chunks")
			return
		}
		total, parseErr := strconv.ParseUint(totalQ, 10, 32)
		if parseErr != nil {
			httputil.WriteError(w, http.StatusBadRequest, "invalid total")
			return
		}
		commitErr = h.svc.StreamStore(r.Context(), stagingID, id, "", tenantKey, uint32(chunks), uint32(total))
	case stagingID != "":
		// No explicit chunks/total: detect via the staging record. If the
		// proxy PUT batches via HandleAppendChunk then the staging record
		// already carries them; commit as a chunked node. Otherwise fall
		// through to the legacy single-blob path with the body.
		hasChunks, chunks, total, peekErr := h.svc.PeekStreamingState(r.Context(), stagingID)
		if peekErr != nil {
			status, msg := mapStatus(peekErr)
			httputil.WriteError(w, status, msg)
			return
		}
		if hasChunks {
			commitErr = h.svc.StreamStore(r.Context(), stagingID, id, "", tenantKey, chunks, total)
		} else {
			commitErr = h.svc.StoreWithStaging(r.Context(), stagingID, id, "", tenantKey, responseBlob)
		}
	default:
		commitErr = h.svc.StoreWithStaging(r.Context(), "", id, "", tenantKey, responseBlob)
	}

	if commitErr != nil {
		status, msg := mapStatus(commitErr)
		if status == http.StatusInternalServerError {
			h.log.Error("store", "id", id, "err", commitErr)
		}
		httputil.WriteError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HandleAppendChunk handles PUT /responses/staging/<stagingID>/chunks/<k>.
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
		httputil.WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	if responseID != "" && len(responseID) > 255 {
		httputil.WriteError(w, http.StatusBadRequest, "response_id exceeds 255 bytes")
		return
	}
	k64, err := strconv.ParseUint(kStr, 10, 32)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid chunk number")
		return
	}
	k := uint32(k64)

	// Cap per-chunk reads independently of the global maxBodyBytes. Default
	// 1 MB; configurable up to 4 MB via WithMaxChunkBytes.
	r.Body = http.MaxBytesReader(w, r.Body, h.chunkLimit())
	chunkBody, err := io.ReadAll(r.Body)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "failed to read chunk body")
		return
	}
	_ = r.Body.Close()
	if len(chunkBody) == 0 {
		httputil.WriteError(w, http.StatusBadRequest, "empty chunk body")
		return
	}

	// Early binding: separate transaction so a binding conflict is
	// reported cleanly without a partial chunk write.
	if responseID != "" {
		if err := h.svc.BindResponseID(r.Context(), stagingID, responseID); err != nil {
			status, msg := mapStatus(err)
			if status == http.StatusInternalServerError {
				h.log.Error("bind response id", "staging", stagingID, "err", err)
			}
			httputil.WriteError(w, status, msg)
			return
		}
	}

	nextExpected, err := h.svc.AppendChunk(r.Context(), stagingID, k, chunkBody)
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("append chunk", "staging", stagingID, "k", k, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}

	// If the proxy sent k < next_expected this was an idempotent replay;
	// surface that with 200 OK + the current next_expected. Otherwise
	// the chunk was newly written: 202 Accepted.
	if k < nextExpected {
		httputil.WriteJSON(w, http.StatusOK, map[string]uint32{
			"received":      k,
			"expected_next": nextExpected,
		})
		return
	}
	httputil.WriteJSON(w, http.StatusAccepted, map[string]uint32{
		"received":      k,
		"expected_next": nextExpected,
	})
}

// HandleComplete handles PUT /responses/staging/<stagingID>/complete?response_id=...&total=...
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
		httputil.WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	if totalStr == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing total")
		return
	}
	total, err := strconv.ParseUint(totalStr, 10, 32)
	if err != nil {
		httputil.WriteError(w, http.StatusBadRequest, "invalid total")
		return
	}
	if responseID != "" && len(responseID) > 255 {
		httputil.WriteError(w, http.StatusBadRequest, "response_id exceeds 255 bytes")
		return
	}

	finalID, err := h.svc.CompleteStreaming(r.Context(), stagingID, responseID, "", tenantKey, 0, nil, uint32(total))
	if err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("complete", "staging", stagingID, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}
	w.Header().Set("Location", "/responses/"+finalID)
	httputil.WriteJSON(w, http.StatusCreated, map[string]string{"response_id": finalID})
}

// HandleAbort handles PUT /responses/staging/<stagingID>/abort.
// Marks the staging record as terminally failed. Deletes the staging
// record, all its chunks, and the respidx entry. Idempotent.
func (h *Handler) HandleAbort(w http.ResponseWriter, r *http.Request) {
	stagingID := r.PathValue("id")
	if len(stagingID) > 64 {
		httputil.WriteError(w, http.StatusBadRequest, "invalid staging id")
		return
	}
	if err := h.svc.AbortStaging(r.Context(), stagingID); err != nil {
		status, msg := mapStatus(err)
		if status == http.StatusInternalServerError {
			h.log.Error("abort", "staging", stagingID, "err", err)
		}
		httputil.WriteError(w, status, msg)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// HandleByResponseID handles GET /responses/by-response-id/{rid}.
// Reverse lookup: returns a 302 redirect to /responses/<id> where id is
// the staging record bound to rid. Useful when a caller knows the
// responseID but not the stagingID.
func (h *Handler) HandleByResponseID(w http.ResponseWriter, r *http.Request) {
	rid := r.PathValue("rid")
	if rid == "" {
		httputil.WriteError(w, http.StatusBadRequest, "missing response id")
		return
	}
	stagingID, err := h.svc.LookupStagingByResponseID(r.Context(), rid)
	if err != nil {
		status, msg := mapStatus(err)
		httputil.WriteError(w, status, msg)
		return
	}
	w.Header().Set("Location", "/responses/staging/"+stagingID)
	w.WriteHeader(http.StatusFound)
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
