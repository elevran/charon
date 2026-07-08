package chainstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// internalChunkSize is the maximum size of a single Pebble chunk. HTTP
// bodies (up to 1 MB default / 4 MB max) are split into chunks of at most
// this many bytes. The read path streams chunks directly from the Pebble
// iterator, so smaller chunks reduce per-chunk resident memory on read;
// the split is invisible to clients and to the read API.
const internalChunkSize = 256 * 1024 // 256 KB

// splitChunks partitions data into Pebble-sized chunks at internalChunkSize
// boundaries, returning the resulting ChunkEntry slice. The last chunk may
// be smaller (partial). Caller passes the first internal offset; subsequent
// chunks go at offset+1, offset+2, ...
//
// Note: 0-byte tail chunks are skipped — a body whose length is an exact
// multiple of internalChunkSize yields exactly (len/internalChunkSize)
// chunks, not +1. This keeps the manifest's ChunkCount field consistent
// with ListChunks's returned count.
func splitChunks(blobID BlobID, firstOffset uint32, data []byte) []ChunkEntry {
	if len(data) == 0 {
		return nil
	}
	fullChunks := len(data) / internalChunkSize
	out := make([]ChunkEntry, 0, fullChunks+1)
	for i := 0; i < fullChunks; i++ {
		out = append(out, ChunkEntry{
			BlobID: blobID,
			Offset: firstOffset + uint32(i),
			Data:   data[i*internalChunkSize : (i+1)*internalChunkSize],
		})
	}
	if rem := len(data) % internalChunkSize; rem > 0 {
		out = append(out, ChunkEntry{
			BlobID: blobID,
			Offset: firstOffset + uint32(fullChunks),
			Data:   data[fullChunks*internalChunkSize : fullChunks*internalChunkSize+rem],
		})
	}
	return out
}

// AppendChunk writes one HTTP batch under stagingID's pre-allocated chunk
// namespace, splitting the body into ≤256 KB Pebble chunks. k is the
// expected wire-level chunk ordinal (the index of the FIRST internal
// Pebble chunk of this batch). Server-validated ordering: a re-send with
// k <= next_expected is treated as an idempotent replay and returns
// (next_expected, nil) without rewriting; k == next_expected writes the
// chunk and returns the new next_expected; k > next_expected is a gap
// (ErrChunkOutOfRange) — the proxy must re-send the missing chunks
// before the gap can be filled.
//
// The body is invisible to reads until the manifest is written by CompleteStreaming —
// GetManifest returns ErrNotFound before that.
//
// Replays at the same k are safe: the Pebble Set is last-write-wins on
// the chunk key, but the server-level dedup skips rewriting identical
// replays so a 200 (vs 202) is observable. The proxy's retry semantics
// (e.g., "complete=true on the last chunk") are no longer entangled with
// chunk writes — Complete is a separate call.
func (s *Store) AppendChunk(ctx context.Context, stagingID string, k uint32, data []byte) (uint32, error) {
	if len(data) == 0 {
		return 0, errors.New("chainstore.AppendChunk: empty body")
	}
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return 0, fmt.Errorf("chainstore.AppendChunk: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return 0, fmt.Errorf("chainstore.AppendChunk: staging lookup: %w", err)
	}
	if staged.ResponseBlobID == (BlobID{}) {
		return 0, errors.New("chainstore.AppendChunk: staging node missing ResponseBlobID namespace")
	}
	nextExpected, err := s.backend.StagingNextOffset(ctx, sid)
	if err != nil {
		return 0, fmt.Errorf("chainstore.AppendChunk: read next offset: %w", err)
	}
	if k < nextExpected {
		// Replay: chunk already written, no-op.
		return nextExpected, nil
	}
	if k > nextExpected {
		return nextExpected, fmt.Errorf("%w: got %d, expected %d", ErrChunkOutOfRange, k, nextExpected)
	}
	// k == nextExpected: write the new chunk(s).
	internalChunks := splitChunks(staged.ResponseBlobID, k, data)
	if len(internalChunks) == 0 {
		return nextExpected, errors.New("chainstore.AppendChunk: empty body after split")
	}
	newNext := k + uint32(len(internalChunks))
	tx := Transaction{
		PutChunks:      internalChunks,
		PutStagingNext: []StagingNextEntry{{StagingID: sid, NextOffset: newNext}},
	}
	if err := s.backend.Commit(ctx, tx); err != nil {
		return nextExpected, fmt.Errorf("chainstore.AppendChunk: commit: %w", err)
	}
	return newNext, nil
}

// BindResponseID sets the responseID on a staging record. After binding,
// subsequent PUTs that carry a different response_id receive
// ErrResponseIDTaken. Idempotent: re-binding to the same value is a no-op.
// Also writes the respidx reverse-lookup key so GET /responses/by-response-id/{rid}
// can resolve the staging record.
func (s *Store) BindResponseID(ctx context.Context, stagingID, responseID string) error {
	if responseID == "" {
		return errors.New("chainstore.BindResponseID: empty response id")
	}
	if len(responseID) > 255 {
		return errors.New("chainstore.BindResponseID: response id exceeds 255 bytes")
	}
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return fmt.Errorf("chainstore.BindResponseID: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return fmt.Errorf("chainstore.BindResponseID: staging lookup: %w", err)
	}
	if staged.ResponseID != "" && staged.ResponseID != responseID {
		return fmt.Errorf("%w: bound to %q, rejected %q", ErrResponseIDTaken, staged.ResponseID, responseID)
	}
	if staged.ResponseID == responseID {
		return nil // idempotent
	}
	staged.ResponseID = responseID
	tx := Transaction{
		PutStagingNodes:    []StagingEntry{{StagingID: sid, Node: staged}},
		PutResponseIDIndex: []ResponseIDIndexEntry{{ResponseID: responseID, StagingID: sid}},
	}
	if err := s.backend.Commit(ctx, tx); err != nil {
		return fmt.Errorf("chainstore.BindResponseID: commit: %w", err)
	}
	return nil
}

// LookupStagingByResponseID returns the stagingID bound to responseID.
func (s *Store) LookupStagingByResponseID(ctx context.Context, responseID string) (string, error) {
	sid, err := s.backend.LookupStagingByResponseID(ctx, responseID)
	if err != nil {
		return "", err
	}
	uid := uuid.UUID(sid)
	return uid.String(), nil
}

// GetStagingDone returns the done-marker for a staging record.
// Returns ErrUnknownStaging if the marker is absent (in-progress).
func (s *Store) GetStagingDone(ctx context.Context, stagingID string) (string, error) {
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return "", fmt.Errorf("chainstore.GetStagingDone: %w", err)
	}
	return s.backend.GetStagingDone(ctx, sid)
}

// RetrieveStaging reads a staging record's assembled body (concatenation
// of all chunks written so far). Used by GET /responses/staging/<id> for
// in-progress reads. Returns ErrNotFound if the staging record is absent
// (or has flipped to done — the handler should check GetStagingDone first).
func (s *Store) RetrieveStaging(ctx context.Context, stagingID string) (Node, Turn, error) {
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return Node{}, Turn{}, fmt.Errorf("chainstore.RetrieveStaging: %w", err)
	}
	node, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return Node{}, Turn{}, fmt.Errorf("chainstore.RetrieveStaging: %w", err)
	}
	reqBlob, respBlob, err := s.backend.GetBlobs(ctx, node)
	if err != nil {
		return Node{}, Turn{}, fmt.Errorf("chainstore.RetrieveStaging: get blobs: %w", err)
	}
	return node, Turn{
		ResponseID:   node.ResponseID,
		RequestBlob:  reqBlob,
		ResponseBlob: respBlob,
	}, nil
}

// AbortStaging marks a staging record as terminally failed. Deletes the
// staging record + its chunks + pfxStagingNext + pfxStagingRID +
// (optionally) pfxRespIdx. Writes pfxStagingDone (empty value) so that
// subsequent GET /staging/{id} returns 410 Gone. Idempotent.
func (s *Store) AbortStaging(ctx context.Context, stagingID string) error {
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return fmt.Errorf("chainstore.AbortStaging: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		if errors.Is(err, ErrUnknownStaging) {
			// Already gone or never-existed. The done-marker (which is
			// the only remaining side effect of an abort) is preserved.
			return nil
		}
		return fmt.Errorf("chainstore.AbortStaging: %w", err)
	}
	tx := Transaction{
		DeleteStagingNodes: []BlobID{sid},
		// Always set the done-marker; this is what makes GET /staging
		// return 410 even after the staging record is gone. Value is
		// empty for aborted.
		PutStagingDone: []StagingDoneEntry{{StagingID: sid, ResponseID: ""}},
	}
	// chunks (and the manifest, if any) live under the staging record's
	// ResponseBlobID. expandChunkedDelete handles both DeleteManifests and
	// DeleteChunks for chunked payloads and is a no-op for single-blob.
	staged.BlobType = BlobTypeChunked
	s.expandChunkedDelete(ctx, &tx, staged)
	if staged.ResponseID != "" {
		tx.DeleteResponseIDIndex = []string{staged.ResponseID}
	}
	if err := s.backend.Commit(ctx, tx); err != nil {
		return fmt.Errorf("chainstore.AbortStaging: commit: %w", err)
	}
	return nil
}

// parseStagingIDBlobID converts a stagingID UUID-string into a BlobID-shaped
// value usable as a pfxStaging key.
func parseStagingIDBlobID(s string) (BlobID, error) {
	uid, err := uuid.Parse(s)
	if err != nil {
		return BlobID{}, fmt.Errorf("invalid staging id: %w", err)
	}
	return BlobID(uid), nil
}

// CompleteStreaming atomically writes the final Node (BlobType=Chunked), the
// manifest, the responseID index entry, and the staging-done marker — but
// does NOT delete the staging record or its chunks. Storage stays keyed
// by stagingID; the chunks remain under pfxChunk + respBlobID and are
// looked up via the staging record on subsequent /responses/{responseID}
// reads. GET /staging/{id} will return 410 Gone because pfxStagingDone
// is set.
//
// responseID: optional. If non-empty, must match the value already bound
// to the staging record (if any). If the staging record has no bound
// responseID yet, this call binds it. If both are unset, Charon returns
// an error (the data would be unreachable via /responses/{id} after
// /staging/{id} flips to 410).
//
// totalSize is the cumulative byte count across ALL chunks. Currently
// recorded in the Node's ResponseBlobSize and the ManifestEntry's
// TotalSize for read-time validation; not used as a control value.
//
// Crash safety: the batch is atomic. A crash before the batch lands
// leaves the staging record + chunks in their prior state (no manifest
// → reads return ErrNotFound via the assembled-blob path). On restart
// the proxy can simply retry the /complete call.
func (s *Store) CompleteStreaming(ctx context.Context, stagingID, responseID, tenantKey string, totalSize uint32) (string, error) {
	if responseID != "" && len(responseID) > responseIDMaxLen {
		return "", fmt.Errorf("chainstore.Complete: responseID exceeds %d bytes (len=%d)", responseIDMaxLen, len(responseID))
	}
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return "", fmt.Errorf("chainstore.Complete: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return "", fmt.Errorf("chainstore.Complete: staging lookup: %w", err)
	}
	respBlobID := staged.ResponseBlobID
	if respBlobID == (BlobID{}) {
		return "", errors.New("chainstore.Complete: staging node missing ResponseBlobID namespace")
	}

	// Resolve the final responseID from (in priority order):
	//   1. The caller-supplied responseID (must match any bound value).
	//   2. The staging record's bound responseID (set by an earlier PUT).
	boundID := staged.ResponseID
	finalID := responseID
	switch {
	case finalID != "" && boundID != "" && finalID != boundID:
		return "", fmt.Errorf("chainstore.Complete: responseID %q conflicts with bound %q", finalID, boundID)
	case finalID == "" && boundID != "":
		finalID = boundID
	case finalID == "" && boundID == "":
		// Caller must supply responseID on /complete (or have bound it
		// earlier via a chunk PUT). Refuse to invent a UUID here —
		// without a bound responseID the data would be unreachable via
		// /responses/{id} after /staging/{id} flips to 410.
		return "", ErrResponseIDRequired
	}

	id := nodeID(tenantKey, finalID)

	// chunkCount comes from the server-tracked pfxStagingNext — the
	// authoritative count of internal Pebble chunks written so far.
	chunkCount, err := s.backend.StagingNextOffset(ctx, sid)
	if err != nil {
		return "", fmt.Errorf("chainstore.Complete: read chunk count: %w", err)
	}

	node := staged
	node.ID = id
	node.ResponseID = finalID
	node.ResponseBlobID = respBlobID
	node.ResponseBlobSize = totalSize
	node.Status = NodeStatusCompleted
	node.BlobType = BlobTypeChunked

	tx := Transaction{
		PutNodes: []Node{node},
		PutManifests: []ManifestEntry{{
			BlobID:     respBlobID,
			ChunkCount: chunkCount,
			TotalSize:  totalSize,
		}},
		PutResponseIDIndex: []ResponseIDIndexEntry{{ResponseID: finalID, StagingID: sid}},
		// KEEP the staging record + pfxStagingNext + pfxStagingRID — the
		// chunks live under pfxChunk + respBlobID and the staging record
		// maps responseID → blobID for /responses/{responseID} reads.
		// Only the done-marker is added; the staging record becomes
		// "complete" rather than "deleted" so reads via
		// /responses/{responseID} can still resolve the chunks.
		PutStagingDone: []StagingDoneEntry{{StagingID: sid, ResponseID: finalID}},
		StatsDelta: StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(totalSize) + int64(staged.RequestBlobSize),
		},
	}
	if node.ParentID != (NodeID{}) {
		tx.PutChildren = []ChildEntry{{Parent: node.ParentID, Child: id}}
	}

	if err := s.backend.Commit(ctx, tx); err != nil {
		return "", fmt.Errorf("chainstore.Complete: commit: %w", err)
	}

	s.applyStatsAndMaybeNotify(tx.StatsDelta)
	return finalID, nil
}
