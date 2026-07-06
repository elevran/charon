package chainstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// PeekStreamingState inspects a staging record and reports whether it carries
// pre-existing chunks. When chunks are present, the caller commits via
// StreamStore; otherwise via StoreWithStaging. A staging record has chunks
// iff ListChunks returns a non-empty slice under the staging node's
// ResponseBlobID (the chunk namespace is pre-allocated when the staging
// node is created by ResolveAndStage).
//
// A missing staging record (ErrUnknownStaging) is treated as "no chunks" —
// callers fall through to the legacy single-blob path. Other errors
// propagate.
func (s *Store) PeekStreamingState(ctx context.Context, stagingID string) (hasChunks bool, chunkCount uint32, totalSize uint32, err error) {
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return false, 0, 0, fmt.Errorf("chainstore.PeekStreamingState: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		if errors.Is(err, ErrUnknownStaging) {
			return false, 0, 0, nil
		}
		return false, 0, 0, fmt.Errorf("chainstore.PeekStreamingState: %w", err)
	}
	if staged.ResponseBlobID == (BlobID{}) {
		return false, 0, 0, nil
	}
	chunks, err := s.backend.ListChunks(ctx, staged.ResponseBlobID)
	if err != nil {
		return false, 0, 0, fmt.Errorf("chainstore.PeekStreamingState: %w", err)
	}
	if len(chunks) == 0 {
		return false, 0, 0, nil
	}
	var total uint32
	for _, c := range chunks {
		total += uint32(len(c.Data))
	}
	return true, uint32(len(chunks)), total, nil
}

// AppendChunk writes one batch of output items under stagingID's pre-allocated
// chunk namespace.  The chunk is not visible to reads until Commit (which
// writes the manifest) — reads of a chunked ResponseBlobID without a manifest
// return ErrNotFound from GetManifest.
//
// offset is 0-based item index; duplicates at the same (namespace, offset) are
// idempotent (last-write-wins by Pebble set semantics).  The caller (HTTP
// handler or proxy) is responsible for retry semantics.
func (s *Store) AppendChunk(ctx context.Context, stagingID string, offset uint32, data []byte) error {
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return fmt.Errorf("chainstore.AppendChunk: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return fmt.Errorf("chainstore.AppendChunk: staging lookup: %w", err)
	}
	if staged.ResponseBlobID == (BlobID{}) {
		return errors.New("chainstore.AppendChunk: staging node missing ResponseBlobID namespace")
	}
	tx := Transaction{
		PutChunks: []ChunkEntry{{
			BlobID: staged.ResponseBlobID,
			Offset: offset,
			Data:   data,
		}},
	}
	if err := s.backend.Commit(ctx, tx); err != nil {
		return fmt.Errorf("chainstore.AppendChunk: commit: %w", err)
	}
	return nil
}

// StreamStore commits a chunked blob: writes the manifest and the final Node
// (BlobType=Chunked) in one atomic transaction. The caller passes the
// assembled chunk count and total byte size — the manifest is the source of
// truth, NOT a re-scan of the chunks (which would be an unbounded O(N) read).
//
// stagingID is the value returned by ResolveAndStage. previousResponseID and
// tenantKey are used exactly as in StoreWithStaging to bind the new node into
// the parent chain.
//
// Crash safety:
//   - Crash before any chunk lands: orphaned staging record (no chunks).
//   - Crash after some chunks, before manifest: orphan chunks under the
//     staging ID's ResponseBlobID — reaped by the staging TTL reaper.
//   - Crash after manifest: chunked blob is fully visible; staging reaper
//     finds and deletes the (now-stale) staging record on next run.
func (s *Store) StreamStore(ctx context.Context, stagingID, responseID, _, tenantKey string, chunkCount uint32, totalSize uint32) error {
	if len(responseID) > 255 {
		return fmt.Errorf("chainstore.StreamStore: responseID exceeds 255 bytes (len=%d)", len(responseID))
	}
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return fmt.Errorf("chainstore.StreamStore: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return fmt.Errorf("chainstore.StreamStore: staging lookup: %w", err)
	}

	id := nodeID(tenantKey, responseID)
	respBlobID := staged.ResponseBlobID
	if respBlobID == (BlobID{}) {
		return errors.New("chainstore.StreamStore: staging node missing ResponseBlobID namespace")
	}

	node := staged
	node.ID = id
	node.ResponseID = responseID
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
		DeleteStagingNodes: []BlobID{sid},
		StatsDelta: StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(totalSize) + int64(staged.RequestBlobSize),
		},
	}

	if node.ParentID != (NodeID{}) {
		tx.PutChildren = []ChildEntry{{Parent: node.ParentID, Child: id}}
	}

	if err := s.backend.Commit(ctx, tx); err != nil {
		return fmt.Errorf("chainstore.StreamStore: commit: %w", err)
	}

	entries := s.entries.Add(1)
	totalBytes := s.bytes.Add(tx.StatsDelta.BytesDelta)
	if (s.cfg.MaxEntries > 0 && entries > s.cfg.MaxEntries) ||
		(s.cfg.MaxBytes > 0 && totalBytes > s.cfg.MaxBytes) {
		s.notifyCapacityExceeded()
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

// StreamStoreCommit atomically writes the final chunk, the manifest, the final
// Node (BlobType=Chunked), and deletes the staging record — all in one Pebble
// batch. This is the combined "PUT last chunk + commit" entry point used when
// the proxy signals complete=true on the last PUT. It saves one HTTP round
// trip versus the two-call pattern (AppendChunk + StreamStore).
//
// Parameters:
//   - finalOffset: 0-based ordinal of THIS chunk in the response. The chunk
//     count recorded in the manifest is finalOffset+1 (chunks are 0-indexed).
//   - finalData: bytes of the final chunk.
//   - totalSize: cumulative byte count across ALL chunks including this one
//     (sum of the bytes of every chunk the proxy sent).
//
// Crash safety is identical to StreamStore: the manifest is the atomic commit
// point. A crash before the batch lands leaves chunks under the staging
// record's ResponseBlobID, reaped by the staging TTL reaper.
func (s *Store) StreamStoreCommit(ctx context.Context, stagingID, responseID, _, tenantKey string, finalOffset uint32, finalData []byte, totalSize uint32) error {
	if len(responseID) > 255 {
		return fmt.Errorf("chainstore.StreamStoreCommit: responseID exceeds 255 bytes (len=%d)", len(responseID))
	}
	if len(finalData) == 0 {
		return errors.New("chainstore.StreamStoreCommit: empty final chunk")
	}
	sid, err := parseStagingIDBlobID(stagingID)
	if err != nil {
		return fmt.Errorf("chainstore.StreamStoreCommit: %w", err)
	}
	staged, err := s.backend.GetStagingNode(ctx, sid)
	if err != nil {
		return fmt.Errorf("chainstore.StreamStoreCommit: staging lookup: %w", err)
	}
	respBlobID := staged.ResponseBlobID
	if respBlobID == (BlobID{}) {
		return errors.New("chainstore.StreamStoreCommit: staging node missing ResponseBlobID namespace")
	}

	id := nodeID(tenantKey, responseID)
	chunkCount := finalOffset + 1

	node := staged
	node.ID = id
	node.ResponseID = responseID
	node.ResponseBlobID = respBlobID
	node.ResponseBlobSize = totalSize
	node.Status = NodeStatusCompleted
	node.BlobType = BlobTypeChunked

	tx := Transaction{
		PutNodes: []Node{node},
		PutChunks: []ChunkEntry{{
			BlobID: respBlobID,
			Offset: finalOffset,
			Data:   finalData,
		}},
		PutManifests: []ManifestEntry{{
			BlobID:     respBlobID,
			ChunkCount: chunkCount,
			TotalSize:  totalSize,
		}},
		DeleteStagingNodes: []BlobID{sid},
		StatsDelta: StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(totalSize) + int64(staged.RequestBlobSize),
		},
	}
	if node.ParentID != (NodeID{}) {
		tx.PutChildren = []ChildEntry{{Parent: node.ParentID, Child: id}}
	}

	if err := s.backend.Commit(ctx, tx); err != nil {
		return fmt.Errorf("chainstore.StreamStoreCommit: commit: %w", err)
	}

	entries := s.entries.Add(1)
	totalBytes := s.bytes.Add(tx.StatsDelta.BytesDelta)
	if (s.cfg.MaxEntries > 0 && entries > s.cfg.MaxEntries) ||
		(s.cfg.MaxBytes > 0 && totalBytes > s.cfg.MaxBytes) {
		s.notifyCapacityExceeded()
	}
	return nil
}
