package chainstore

import (
	"context"
	"log"
	"time"
)

// notifyCapacityExceeded signals the eviction goroutine to run evictOldest
// outside its normal ticker cadence. The channel is buffered size 1:
// concurrent Store calls coalesce — a single pending nudge suffices.
func (s *Store) notifyCapacityExceeded() {
	select {
	case s.nudgeEvict <- struct{}{}:
	default: // already a nudge pending; drop
	}
}

// evictionLoop is the long-running capacity-eviction goroutine.
// Triggered by either the periodic ticker or a nudge from Store after a write
// pushes the store over capacity.
func (s *Store) evictionLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.EvictionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.evictOldest(ctx)
		case <-s.nudgeEvict:
			s.evictOldest(ctx)
		}
	}
}

// ttlLoop is the long-running TTL-reaper goroutine.
func (s *Store) ttlLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.TTLInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ttlReap(ctx)
		}
	}
}

// evictOldest removes nodes from the oldest bucket until the store is under
// configured limits. Optimistic: re-reads Node via GetNode before deleting to
// guard against a concurrent Resolve that promoted the node to a newer bucket.
func (s *Store) evictOldest(ctx context.Context) {
	const batchSize = 100
	for {
		if (s.cfg.MaxEntries <= 0 || s.entries.Load() <= s.cfg.MaxEntries) &&
			(s.cfg.MaxBytes <= 0 || s.bytes.Load() <= s.cfg.MaxBytes) {
			return
		}
		bucket, err := s.backend.OldestBucket(ctx)
		if err != nil {
			return // ErrNotFound = store is empty
		}
		candidates, err := s.backend.GetEvictionCandidates(ctx, bucket, batchSize)
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
	const batchSize = 100
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

		candidates, err := s.backend.GetEvictionCandidates(ctx, bucket, batchSize)
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
		}

		if allFresh {
			if len(candidates) == batchSize && staleSkips < maxStaleBucketSkips {
				staleSkips++
				continue
			}
			return // partial bucket (no expired nodes) or skip bound hit
		}
		staleSkips = 0
	}
}

// deleteNode deletes a single node and its blobs (no cascade — leaves children
// with a dangling parent pointer, which is intentional for capacity eviction).
func (s *Store) deleteNode(ctx context.Context, node Node) {
	blobBytes := int64(node.RequestBlobSize) + int64(node.ResponseBlobSize)

	tx := Transaction{
		DeleteNodes: []NodeID{node.ID},
		// NewBucket=0 signals delete-only (no new LRU entry written).
		BucketMoves: []BucketMove{{NodeID: node.ID, OldBucket: node.BucketID, NewBucket: 0}},
		StatsDelta: StatsDelta{
			EntryDelta: -1,
			BytesDelta: -blobBytes,
		},
	}
	if node.RequestBlobID != (BlobID{}) {
		tx.DeleteBlobs = append(tx.DeleteBlobs, node.RequestBlobID)
	}
	if node.ResponseBlobID != (BlobID{}) {
		tx.DeleteBlobs = append(tx.DeleteBlobs, node.ResponseBlobID)
	}
	if node.ParentID != (NodeID{}) {
		tx.DeleteChildren = []ChildEntry{{Parent: node.ParentID, Child: node.ID}}
	}

	if err := s.backend.Commit(ctx, tx); err != nil {
		log.Printf("chainstore: deleteNode commit error: %v", err)
		return
	}
	s.entries.Add(-1)
	s.bytes.Add(-blobBytes)
}

// deleteSubtree deletes root and all descendants via BFS using GetChildren.
// Performs one Transaction per BFS level (capped at levelCap nodes) to keep
// WAL writes bounded and let other writers interleave.
func (s *Store) deleteSubtree(ctx context.Context, root NodeID) {
	frontier := []NodeID{root}
	for len(frontier) > 0 {
		// Gather children of every node at this BFS level before deleting.
		var next []NodeID
		for _, cur := range frontier {
			children, _ := s.backend.GetChildren(ctx, cur)
			next = append(next, children...)
		}

		// Delete this level in chunks to keep transaction size bounded.
		const levelCap = 1000
		for len(frontier) > 0 {
			chunk := chunkNodes(frontier, levelCap)
			frontier = frontier[len(chunk):]

			tx := Transaction{}
			for _, id := range chunk {
				node, err := s.backend.GetNode(ctx, id)
				if err != nil {
					continue // already deleted
				}
				tx.DeleteNodes = append(tx.DeleteNodes, id)
				if node.RequestBlobID != (BlobID{}) {
					tx.DeleteBlobs = append(tx.DeleteBlobs, node.RequestBlobID)
				}
				if node.ResponseBlobID != (BlobID{}) {
					tx.DeleteBlobs = append(tx.DeleteBlobs, node.ResponseBlobID)
				}
				// NewBucket=0 = delete-only LRU cleanup.
				tx.BucketMoves = append(tx.BucketMoves, BucketMove{
					NodeID:    id,
					OldBucket: node.BucketID,
					NewBucket: 0,
				})
				tx.StatsDelta.BytesDelta -= int64(node.RequestBlobSize) + int64(node.ResponseBlobSize)
				tx.StatsDelta.EntryDelta--
				// Remove the upward child link (parent → this node).
				if node.ParentID != (NodeID{}) {
					tx.DeleteChildren = append(tx.DeleteChildren, ChildEntry{Parent: node.ParentID, Child: id})
				}
				s.entries.Add(-1)
				s.bytes.Add(-int64(node.RequestBlobSize) - int64(node.ResponseBlobSize))
			}

			if err := s.backend.Commit(ctx, tx); err != nil {
				log.Printf("chainstore: deleteSubtree commit error: %v", err)
			}
		}

		frontier = next
	}
}

func chunkNodes(nodes []NodeID, size int) []NodeID {
	if len(nodes) <= size {
		return nodes
	}
	return nodes[:size]
}
