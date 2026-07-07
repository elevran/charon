package chainstore

import (
	"context"
	"slices"
	"time"
)

const evictBatchSize = 100

// notifyCapacityExceeded signals the eviction goroutine to run evictOldest
// outside its normal ticker cadence. The channel is buffered size 1:
// concurrent Store calls coalesce — a single pending nudge suffices.
func (s *Store) notifyCapacityExceeded() {
	select {
	case s.nudgeEvict <- struct{}{}:
		s.nudgeCount.Add(1) // observable in tests via export_test.go
	default: // already a nudge pending; drop
	}
}

// runUntilDone runs fn on every tick and nudge until ctx is cancelled.
// nudge may be nil, in which case only the ticker drives fn.
func runUntilDone(ctx context.Context, tick <-chan time.Time, nudge <-chan struct{}, fn func()) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick:
			fn()
		case <-nudge:
			fn()
		}
	}
}

// evictionLoop is the long-running capacity-eviction goroutine.
// Triggered by either the periodic ticker or a nudge from Store after a write
// pushes the store over capacity.
func (s *Store) evictionLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.EvictionInterval)
	defer ticker.Stop()
	runUntilDone(ctx, ticker.C, s.nudgeEvict, func() { s.evictOldest(ctx) })
}

// ttlLoop is the long-running TTL-reaper goroutine.
// It drives both node TTL reaping and staging-record TTL reaping.
func (s *Store) ttlLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.TTLInterval)
	defer ticker.Stop()
	runUntilDone(ctx, ticker.C, nil, func() {
		s.ttlReap(ctx)
		s.reapStaging(ctx)
	})
}

// reapStaging deletes staging records (and their associated request blobs)
// whose Node.CreatedAt is older than Config.StagingTTL. These are orphaned
// records left by a proxy crash between ResolveAndStage and StoreWithStaging
// or between StreamStore.AppendChunk and StreamStore.Commit.
//
// Orphan chunks (pfxChunk keys written by AppendChunk whose manifest never
// landed) are detected by listing chunks under the orphaned staging node's
// ResponseBlobID and adding them to the same transaction. Their Node carries
// the staging key which equals the ResponseBlobID used as the chunk namespace.
//
// Note: when the manifest HAS been written but the staging record was not
// yet deleted, the manifest remains and the chunked blob is still committed;
// reapStaging only removes staging records older than StagingTTL, so a
// "stuck" manifest leaves the readable blob intact and is corrected on the
// next successful StoreWithStaging/Commit (idempotent) or on a future phase
// that adds startup reconciliation.
func (s *Store) reapStaging(ctx context.Context) {
	if s.cfg.StagingTTL <= 0 {
		return
	}
	entries, err := s.backend.ListStagingOlderThan(ctx, s.clock.Now().Add(-s.cfg.StagingTTL))
	if err != nil {
		s.cfg.Log.Error("chainstore: reapStaging scan error", "err", err)
		return
	}
	if len(entries) == 0 {
		return
	}
	tx := Transaction{}
	for _, se := range entries {
		// Skip completed or aborted records: they carry a done-marker and their
		// chunks are the only copy of the committed response blob.
		if _, err := s.backend.GetStagingDone(ctx, se.StagingID); err == nil {
			continue
		}
		tx.DeleteStagingNodes = append(tx.DeleteStagingNodes, se.StagingID)
		if se.Node.RequestBlobID != (BlobID{}) {
			tx.DeleteBlobs = append(tx.DeleteBlobs, se.Node.RequestBlobID)
		}
		// If the staging node carried orphan streaming chunks, sweep them too.
		// (Stage node names the ResponseBlobID we used as the chunk namespace.)
		if se.Node.ResponseBlobID != (BlobID{}) {
			chunks, cErr := s.backend.ListChunks(ctx, se.Node.ResponseBlobID)
			if cErr != nil {
				s.cfg.Log.Error("chainstore: reapStaging ListChunks", "blobID", se.Node.ResponseBlobID, "err", cErr)
			} else {
				for _, c := range chunks {
					tx.DeleteChunks = append(tx.DeleteChunks, ChunkEntry{BlobID: c.BlobID, Offset: c.Offset})
				}
			}
		}
	}
	if err := s.backend.Commit(ctx, tx); err != nil {
		s.cfg.Log.Error("chainstore: reapStaging commit error", "count", len(entries), "err", err)
		if s.metrics != nil {
			s.metrics.stagingReapErrTotal.Inc()
		}
		return
	}
	if s.metrics != nil {
		s.metrics.stagingReapedTotal.Add(float64(len(entries)))
	}
}

// evictOldest removes nodes from the oldest bucket until the store is under
// configured limits. Optimistic: re-reads Node via GetNode before deleting to
// guard against a concurrent Resolve that promoted the node to a newer bucket.
// Bounded by maxEvictionIterations to prevent an infinite loop when all candidates
// are concurrently promoted by Resolve on every iteration.
func (s *Store) evictOldest(ctx context.Context) {
	const maxEvictionIterations = 16
	for range maxEvictionIterations {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if (s.cfg.MaxEntries <= 0 || s.entries.Load() <= s.cfg.MaxEntries) &&
			(s.cfg.MaxBytes <= 0 || s.bytes.Load() <= s.cfg.MaxBytes) {
			return
		}
		bucket, err := s.backend.OldestBucket(ctx)
		if err != nil {
			return // ErrNotFound = store is empty
		}
		candidates, err := s.backend.GetEvictionCandidates(ctx, bucket, evictBatchSize)
		if err != nil || len(candidates) == 0 {
			return
		}
		for _, id := range candidates {
			node, err := s.backend.GetNode(ctx, id)
			if err != nil {
				continue // already deleted
			}
			// Optimistic guard: skip if node was promoted to a newer bucket between
			// GetEvictionCandidates and this GetNode call.
			if node.BucketID != bucket {
				continue
			}
			s.deleteNode(ctx, node)
			if s.metrics != nil {
				s.metrics.evictionsTotal.Inc()
			}
		}
	}
}

// ttlReap removes expired nodes. Walks buckets oldest-first via OldestBucket +
// GetEvictionCandidates. Stops when the oldest non-empty bucket is newer than
// the TTL cutoff (bucket * BucketDuration > now - TTL).
//
// Stale-bucket handling: a bucket whose LRU entries point to hot nodes
// (LastAccessUnix recent, no bucket-crossing Resolve yet) stalls the reaper.
// When len(candidates) == batchSize and all are fresh, the bucket is suspect —
// increment staleSkips and try again. Bounded by maxStaleBucketSkips to
// prevent an infinite loop in pathological cases.
func (s *Store) ttlReap(ctx context.Context) {
	if s.cfg.TTL <= 0 {
		return
	}
	cutoff := s.clock.Now().Add(-s.cfg.TTL).Unix()
	dur := int64(s.cfg.bucketDuration().Seconds())
	const maxStaleBucketSkips = 16

	staleSkips := 0
	for {
		bucket, err := s.backend.OldestBucket(ctx)
		if err != nil {
			return // store empty
		}

		// Stop when this bucket's time range is newer than the TTL cutoff.
		if int64(bucket)*dur > cutoff {
			return
		}

		candidates, err := s.backend.GetEvictionCandidates(ctx, bucket, evictBatchSize)
		if err != nil || len(candidates) == 0 {
			return
		}

		allFresh := true
		for _, id := range candidates {
			node, err := s.backend.GetNode(ctx, id)
			if err != nil {
				continue // already deleted (stale LRU entry)
			}
			if node.LastAccessUnix >= cutoff {
				// Hot node with a stale bucket entry — will self-heal on next access.
				continue
			}
			allFresh = false
			s.deleteSubtree(ctx, id)
			if s.metrics != nil {
				s.metrics.ttlExpirationsTotal.Inc()
			}
		}

		if allFresh {
			if len(candidates) == evictBatchSize && staleSkips < maxStaleBucketSkips {
				staleSkips++
				continue
			}
			return // partial bucket (no expired nodes) or skip bound hit
		}
		staleSkips = 0
	}
}

// appendNodeToDeleteTx appends the per-node delete mutations into tx.
// Used by both deleteNode and deleteSubtree to avoid duplication.
// For chunked nodes (Node.BlobType == BlobTypeChunked), the single-blob delete
// is skipped; chunks are deleted by querying the manifest and appending per-offset
// DeleteChunks.  The caller is responsible for this expansion (do not fold into
// this helper — it avoids taking a manifest fetch inside a building tx).
func appendNodeToDeleteTx(tx *Transaction, node Node) {
	tx.DeleteNodes = append(tx.DeleteNodes, node.ID)
	if node.RequestBlobID != (BlobID{}) {
		tx.DeleteBlobs = append(tx.DeleteBlobs, node.RequestBlobID)
	}
	if node.ResponseBlobID != (BlobID{}) && node.BlobType != BlobTypeChunked {
		tx.DeleteBlobs = append(tx.DeleteBlobs, node.ResponseBlobID)
	}
	// NewBucket=0 signals delete-only (no new LRU entry written).
	tx.BucketMoves = append(tx.BucketMoves, BucketMove{
		NodeID:    node.ID,
		OldBucket: node.BucketID,
		NewBucket: 0,
	})
	if node.ParentID != (NodeID{}) {
		tx.DeleteChildren = append(tx.DeleteChildren, ChildEntry{Parent: node.ParentID, Child: node.ID})
	}
	blobBytes := int64(node.RequestBlobSize) + int64(node.ResponseBlobSize)
	tx.StatsDelta.EntryDelta--
	tx.StatsDelta.BytesDelta -= blobBytes
}

// expandChunkedDelete fetches the manifest and appends per-chunk deletes plus
// the manifest delete into tx. No-op when node has no chunked payload.
func (s *Store) expandChunkedDelete(ctx context.Context, tx *Transaction, node Node) {
	if node.BlobType != BlobTypeChunked || node.ResponseBlobID == (BlobID{}) {
		return
	}
	tx.DeleteManifests = append(tx.DeleteManifests, node.ResponseBlobID)
	chunks, err := s.backend.ListChunks(ctx, node.ResponseBlobID)
	if err != nil {
		s.cfg.Log.Error("chainstore: expandChunkedDelete ListChunks", "err", err)
		return
	}
	for _, c := range chunks {
		tx.DeleteChunks = append(tx.DeleteChunks, ChunkEntry{BlobID: c.BlobID, Offset: c.Offset})
	}
}

// deleteNode deletes a single node and its blobs (no cascade — leaves children
// with a dangling parent pointer, which is intentional for capacity eviction).
func (s *Store) deleteNode(ctx context.Context, node Node) {
	tx := Transaction{}
	appendNodeToDeleteTx(&tx, node)
	s.expandChunkedDelete(ctx, &tx, node)
	blobBytes := int64(node.RequestBlobSize) + int64(node.ResponseBlobSize)

	// Decrement before commit so concurrent capacity checks see the updated count
	// immediately. If the commit fails, compensate to restore the correct count.
	s.entries.Add(-1)
	totalBytes := s.bytes.Add(-blobBytes)
	if err := s.backend.Commit(ctx, tx); err != nil {
		s.entries.Add(1)
		s.bytes.Add(blobBytes)
		s.cfg.Log.Error("chainstore: deleteNode commit error", "err", err)
		return
	}
	s.metricsAfterMutation(s.entries.Load(), totalBytes)
	s.cache.invalidateNodes(tx.DeleteNodes)
}

// deleteSubtree deletes root and all descendants via BFS using GetChildren.
// Performs one Transaction per BFS level (capped at levelCap nodes) to keep
// WAL writes bounded and let other writers interleave.
//
// Crash safety: deleteSubtree is crash-safe but not atomic. A crash between BFS
// levels leaves the root deleted but children intact with dangling parent pointers.
// Orphaned descendants are never accessed again and will be reclaimed by capacity
// eviction. If stricter cleanup is required, add a compaction-time orphan scan.
func (s *Store) deleteSubtree(ctx context.Context, root NodeID) {
	frontier := []NodeID{root}
	for len(frontier) > 0 {
		// Gather children of every node at this BFS level before deleting.
		var next []NodeID
		for _, cur := range frontier {
			children, err := s.backend.GetChildren(ctx, cur)
			if err != nil && err != ErrNotFound {
				s.cfg.Log.Error("chainstore: deleteSubtree GetChildren error", "err", err)
				return
			}
			next = append(next, children...)
		}

		// Delete this level in chunks to keep transaction size bounded.
		const levelCap = 1000
		for chunk := range slices.Chunk(frontier, levelCap) {
			tx := Transaction{}
			for _, id := range chunk {
				node, err := s.backend.GetNode(ctx, id)
				if err != nil {
					continue // already deleted
				}
				appendNodeToDeleteTx(&tx, node)
				// For chunked nodes, expand the manifest-based chunk deletes here
				// so each BFS-level transaction remains atomic.
				s.expandChunkedDelete(ctx, &tx, node)
			}

			if err := s.backend.Commit(ctx, tx); err != nil {
				s.cfg.Log.Error("chainstore: deleteSubtree commit error", "err", err)
				return
			}
			// Update counters only after a successful commit.
			s.entries.Add(tx.StatsDelta.EntryDelta)
			totalBytes := s.bytes.Add(tx.StatsDelta.BytesDelta)
			s.metricsAfterMutation(s.entries.Load(), totalBytes)
			s.cache.invalidateNodes(tx.DeleteNodes)
		}

		frontier = next
	}
}
