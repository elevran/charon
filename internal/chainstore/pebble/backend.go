package pebble

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"

	"github.com/elevran/charon/internal/chainstore"
)

// Backend implements chainstore.Backend using CockroachDB Pebble as the
// underlying KV engine. All key encoding, snapshot management, and batch
// construction are internal to this package.
type Backend struct{ db *crdbpebble.DB }

// mapErr translates Pebble-specific errors to chainstore sentinel errors.
func mapErr(err error) error {
	if err == crdbpebble.ErrNotFound {
		return chainstore.ErrNotFound
	}
	return err
}

// nodeIDMax is used as an upper-bound child ID in range scans.
var nodeIDMax = chainstore.NodeID{
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
	0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
}

// LoadChain opens a Pebble snapshot for consistent reads, then walks parent pointers.
// A snapshot prevents a concurrent deletion from causing a mid-walk ErrNotFound
// on an otherwise intact chain. ResponseID is fetched from the pfxResponseID key
// within the same snapshot so all fields of each returned Node are consistent.
func (b *Backend) LoadChain(_ context.Context, leaf chainstore.NodeID) ([]chainstore.Node, error) {
	snap := b.db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	var nodes []chainstore.Node
	cur := leaf
	for {
		val, closer, err := snap.Get(metaKey(cur))
		if err != nil {
			if closer != nil {
				_ = closer.Close()
			}
			if err != crdbpebble.ErrNotFound {
				return nil, err
			}
			if len(nodes) == 0 {
				return nil, chainstore.ErrNotFound
			}
			// Ancestor absent after finding at least one node: the parent was
			// capacity-evicted (non-cascading deleteNode). Not disk corruption.
			return nil, chainstore.ErrChainExpired
		}
		node, err := decodeNode(val)
		_ = closer.Close()
		if err != nil {
			return nil, fmt.Errorf("chainstore/pebble: decode node %x: %w", cur, err)
		}
		if ridVal, ridCloser, ridErr := snap.Get(responseIDKey(cur)); ridErr == nil {
			node.ResponseID = string(ridVal)
			_ = ridCloser.Close()
		}
		nodes = append(nodes, node)
		if node.ParentID == (chainstore.NodeID{}) {
			break
		}
		cur = node.ParentID
	}
	return nodes, nil
}

// GetNode fetches a single node's metadata. ErrNotFound if absent.
func (b *Backend) GetNode(_ context.Context, id chainstore.NodeID) (chainstore.Node, error) {
	val, closer, err := b.db.Get(metaKey(id))
	if err != nil {
		return chainstore.Node{}, mapErr(err)
	}
	defer func() { _ = closer.Close() }()
	node, err := decodeNode(val)
	if err != nil {
		return chainstore.Node{}, fmt.Errorf("chainstore/pebble: decode node %x: %w", id, err)
	}
	if ridVal, ridCloser, ridErr := b.db.Get(responseIDKey(id)); ridErr == nil {
		node.ResponseID = string(ridVal)
		_ = ridCloser.Close()
	}
	return node, nil
}

// GetBlobs fetches both blobs for a node in one call.
// Either blob is nil if its BlobID is zero (not yet stored).
// Returns (requestBlob, responseBlob, err).
//
// Dispatches on Node.BlobType:
//   - BlobTypeSingle: response blob fetched from pfxBlob (one read)
//   - BlobTypeChunked: chunks fetched from pfxChunk and reassembled in order
//
// NOTE: blob reads are not snapshot-isolated with respect to LoadChain. A
// concurrent Store that completes a turn (writing ResponseBlobID) between
// LoadChain and GetBlobs may cause Resolve to return an unexpectedly non-nil
// ResponseBlob for a node that appeared incomplete in the chain snapshot. This
// is a benign over-read (the data is valid) and will be addressed in a future
// phase when Resolve is refactored to use a snapshot-spanning read path.
func (b *Backend) GetBlobs(_ context.Context, node chainstore.Node) ([]byte, []byte, error) {
	requestBlob, err := b.fetchBlob(node.RequestBlobID)
	if err != nil {
		return nil, nil, err
	}
	if node.ResponseBlobID == (chainstore.BlobID{}) {
		return requestBlob, nil, nil
	}
	responseBlob, err := b.fetchResponseBlob(node)
	if err != nil {
		return nil, nil, err
	}
	return requestBlob, responseBlob, nil
}

// fetchResponseBlob reads a response blob, dispatching on Node.BlobType.
// The zero-BlobID guard is inlined in GetBlobs so that callers can short-circuit
// before any backend work.
func (b *Backend) fetchResponseBlob(node chainstore.Node) ([]byte, error) {
	if node.BlobType == chainstore.BlobTypeChunked {
		return b.assembleChunked(node.ResponseBlobID)
	}
	return b.fetchBlob(node.ResponseBlobID)
}

// assembleChunked reads the manifest for blobID, then scans pfxChunk+blobID for
// every chunk and concatenates them in offset order. To avoid materializing
// every chunk body in an intermediate ChunkEntry slice, we stream chunks
// directly from the iterator into the output buffer — halving memory and
// memcpy on chunked reads. Reads are NOT snapshot-isolated against a concurrent
// StreamStore commit; partial reads are benign.
func (b *Backend) assembleChunked(blobID chainstore.BlobID) ([]byte, error) {
	manifest, err := b.GetManifest(context.Background(), blobID)
	if err != nil {
		return nil, err
	}
	lower := chunkKeyPrefix(blobID)
	upper := make([]byte, len(lower))
	copy(upper, lower)
	upper[0] = pfxChunk + 1
	iter, err := b.db.NewIter(&crdbpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("chainstore/pebble: assembleChunked iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	out := make([]byte, 0, manifest.TotalSize)
	count := uint32(0)
	for iter.First(); iter.Valid(); iter.Next() {
		val, vErr := iter.ValueAndErr()
		if vErr != nil {
			return nil, vErr
		}
		out = append(out, val...)
		count++
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	if count != manifest.ChunkCount {
		return nil, fmt.Errorf("chainstore/pebble: chunked blob %x has %d chunks, manifest declares %d", blobID, count, manifest.ChunkCount)
	}
	return out, nil
}

// fetchBlob retrieves one blob by ID; returns nil (not an error) for a zero BlobID.
func (b *Backend) fetchBlob(id chainstore.BlobID) ([]byte, error) {
	if id == (chainstore.BlobID{}) {
		return nil, nil
	}
	val, closer, err := b.db.Get(blobKey(id))
	if err != nil {
		return nil, mapErr(err)
	}
	defer func() { _ = closer.Close() }()
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// GetManifest returns the manifest for a chunked blob. ErrNotFound if absent.
func (b *Backend) GetManifest(_ context.Context, blobID chainstore.BlobID) (chainstore.ManifestEntry, error) {
	val, closer, err := b.db.Get(manifestKey(blobID))
	if err != nil {
		return chainstore.ManifestEntry{}, mapErr(err)
	}
	defer func() { _ = closer.Close() }()
	m, err := decodeManifest(val)
	if err != nil {
		return chainstore.ManifestEntry{}, fmt.Errorf("chainstore/pebble: decode manifest %x: %w", blobID, err)
	}
	m.BlobID = blobID
	return m, nil
}

// ListChunks scans pfxChunk+blobID in offset order and returns all chunks.
func (b *Backend) ListChunks(_ context.Context, blobID chainstore.BlobID) ([]chainstore.ChunkEntry, error) {
	lower := chunkKeyPrefix(blobID)
	upper := make([]byte, len(lower))
	copy(upper, lower)
	upper[0] = pfxChunk + 1 // exclusive upper bound at the next prefix

	iter, err := b.db.NewIter(&crdbpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("chainstore/pebble: ListChunks iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	var out []chainstore.ChunkEntry
	for iter.First(); iter.Valid(); iter.Next() {
		val, err := iter.ValueAndErr()
		if err != nil {
			return nil, err
		}
		offset := binary.BigEndian.Uint32(iter.Key()[17:21])
		buf := make([]byte, len(val))
		copy(buf, val)
		out = append(out, chainstore.ChunkEntry{
			BlobID: blobID,
			Offset: offset,
			Data:   buf,
		})
	}
	return out, iter.Error()
}

// Commit translates a domain Transaction into a pebble.Batch and commits atomically.
func (b *Backend) Commit(ctx context.Context, tx chainstore.Transaction) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	batch := b.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, n := range tx.PutNodes {
		if err := batch.Set(metaKey(n.ID), encodeNode(n), nil); err != nil {
			return err
		}
		if n.ResponseID != "" {
			if err := batch.Set(responseIDKey(n.ID), []byte(n.ResponseID), nil); err != nil {
				return err
			}
		}
		if n.ParentID != (chainstore.NodeID{}) {
			if err := batch.Set(childKey(n.ParentID, n.ID), nil, nil); err != nil {
				return err
			}
		}
		// Write lru entry. BucketID=0 is a sentinel for "unset" — bucket 0 would require a
		// Unix timestamp from 1970, which never occurs in practice.
		if n.BucketID != 0 {
			if err := batch.Set(lruKey(n.BucketID, n.ID), nil, nil); err != nil {
				return err
			}
		}
	}

	for _, id := range tx.DeleteNodes {
		if err := batch.Delete(metaKey(id), nil); err != nil {
			return err
		}
		if err := batch.Delete(responseIDKey(id), nil); err != nil {
			return err
		}
	}

	for _, c := range tx.DeleteChildren {
		if err := batch.Delete(childKey(c.Parent, c.Child), nil); err != nil {
			return err
		}
	}

	for _, c := range tx.PutChildren {
		if err := batch.Set(childKey(c.Parent, c.Child), nil, nil); err != nil {
			return err
		}
	}

	for _, be := range tx.PutBlobs {
		if err := batch.Set(blobKey(be.BlobID), be.Data, nil); err != nil {
			return err
		}
	}

	for _, bid := range tx.DeleteBlobs {
		if err := batch.Delete(blobKey(bid), nil); err != nil {
			return err
		}
	}

	for _, c := range tx.PutChunks {
		if err := batch.Set(chunkKey(c.BlobID, c.Offset), c.Data, nil); err != nil {
			return err
		}
	}

	for _, c := range tx.DeleteChunks {
		if err := batch.Delete(chunkKey(c.BlobID, c.Offset), nil); err != nil {
			return err
		}
	}

	for _, m := range tx.PutManifests {
		if err := batch.Set(manifestKey(m.BlobID), encodeManifest(m), nil); err != nil {
			return err
		}
	}

	for _, bid := range tx.DeleteManifests {
		if err := batch.Delete(manifestKey(bid), nil); err != nil {
			return err
		}
	}

	for _, bm := range tx.BucketMoves {
		// OldBucket=0 means "no old entry to delete" (reserved sentinel — never a real bucket).
		if bm.OldBucket != 0 {
			if err := batch.Delete(lruKey(bm.OldBucket, bm.NodeID), nil); err != nil {
				return err
			}
		}
		// NewBucket=0 means "delete only — no new LRU entry" (used by deleteNode/deleteSubtree).
		if bm.NewBucket != 0 {
			if err := batch.Set(lruKey(bm.NewBucket, bm.NodeID), nil, nil); err != nil {
				return err
			}
		}
	}

	for _, se := range tx.PutStagingNodes {
		if err := batch.Set(stagingKey(se.StagingID), encodeNode(se.Node), nil); err != nil {
			return err
		}
	}

	for _, sid := range tx.DeleteStagingNodes {
		if err := batch.Delete(stagingKey(sid), nil); err != nil {
			return err
		}
	}

	if err := b.applyStatsDelta(batch, tx.StatsDelta); err != nil {
		return err
	}

	return batch.Commit(crdbpebble.Sync)
}

// applyStatsDelta updates the persistent stats counter via Pebble's MERGE operation.
func (b *Backend) applyStatsDelta(batch *crdbpebble.Batch, d chainstore.StatsDelta) error {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(d.EntryDelta))
	binary.BigEndian.PutUint64(buf[8:16], uint64(d.BytesDelta))
	return batch.Merge(statsKey, buf[:], nil)
}

// OldestBucket uses SeekToFirst on the lru prefix — O(log N), no full scan.
func (b *Backend) OldestBucket(_ context.Context) (chainstore.BucketID, error) {
	iter, err := b.db.NewIter(&crdbpebble.IterOptions{
		LowerBound: []byte{pfxLRU},
		UpperBound: []byte{pfxLRU + 1},
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()
	if !iter.First() {
		if err := iter.Error(); err != nil {
			return 0, err
		}
		return 0, chainstore.ErrNotFound
	}
	bucket := chainstore.BucketID(binary.BigEndian.Uint64(iter.Key()[1:9]))
	return bucket, iter.Error()
}

// GetEvictionCandidates scans lru entries for the given bucket, returns up to limit NodeIDs.
func (b *Backend) GetEvictionCandidates(_ context.Context, bucket chainstore.BucketID, limit int) ([]chainstore.NodeID, error) {
	lower := lruKey(bucket, chainstore.NodeID{})
	// Guard against bucket+1 wrapping to 0 when bucket == MaxUint64.
	var upper []byte
	if bucket == ^chainstore.BucketID(0) {
		upper = []byte{pfxLRU + 1}
	} else {
		upper = lruKey(bucket+1, chainstore.NodeID{})
	}
	iter, err := b.db.NewIter(&crdbpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()
	var ids []chainstore.NodeID
	for iter.First(); iter.Valid() && len(ids) < limit; iter.Next() {
		var id chainstore.NodeID
		copy(id[:], iter.Key()[9:])
		ids = append(ids, id)
	}
	return ids, iter.Error()
}

// GetChildren returns the direct children of parentID by scanning the pfxChildren prefix.
func (b *Backend) GetChildren(_ context.Context, parentID chainstore.NodeID) ([]chainstore.NodeID, error) {
	lower := childKey(parentID, chainstore.NodeID{})
	upper := childKey(parentID, nodeIDMax)

	iter, err := b.db.NewIter(&crdbpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var children []chainstore.NodeID
	for iter.First(); iter.Valid(); iter.Next() {
		var child chainstore.NodeID
		copy(child[:], iter.Key()[21:41])
		children = append(children, child)
	}
	return children, iter.Error()
}

// GetStagingNode fetches the partial Node stored under a staging key.
// Returns ErrUnknownStaging if the staging record is absent.
func (b *Backend) GetStagingNode(_ context.Context, stagingID chainstore.BlobID) (chainstore.Node, error) {
	val, closer, err := b.db.Get(stagingKey(stagingID))
	if err != nil {
		if err == crdbpebble.ErrNotFound {
			return chainstore.Node{}, chainstore.ErrUnknownStaging
		}
		return chainstore.Node{}, err
	}
	defer func() { _ = closer.Close() }()
	node, err := decodeNode(val)
	if err != nil {
		return chainstore.Node{}, fmt.Errorf("chainstore/pebble: decode staging node %x: %w", stagingID, err)
	}
	return node, nil
}

// ListStagingOlderThan scans all pfxStaging keys and returns entries whose
// Node.CreatedAt is before cutoff. The caller uses these to delete orphaned
// staging records left by a proxy crash.
func (b *Backend) ListStagingOlderThan(_ context.Context, cutoff time.Time) ([]chainstore.StagingEntry, error) {
	lower := []byte{pfxStaging}
	upper := []byte{pfxStaging + 1}
	iter, err := b.db.NewIter(&crdbpebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, fmt.Errorf("chainstore/pebble: ListStagingOlderThan iter: %w", err)
	}
	defer func() { _ = iter.Close() }()

	cutoffSecs := cutoff.Unix()
	var results []chainstore.StagingEntry
	for iter.First(); iter.Valid(); iter.Next() {
		node, err := decodeNode(iter.Value())
		if err != nil {
			continue // skip corrupt records
		}
		if node.CreatedAt >= cutoffSecs {
			continue
		}
		var sid chainstore.BlobID
		copy(sid[:], iter.Key()[1:]) // skip 1-byte prefix, copy 16-byte staging UUID
		results = append(results, chainstore.StagingEntry{StagingID: sid, Node: node})
	}
	return results, iter.Error()
}

// Stats returns the persistent entry count and total blob bytes.
func (b *Backend) Stats(_ context.Context) (int64, int64, error) {
	val, closer, err := b.db.Get(statsKey)
	if err != nil {
		if err == crdbpebble.ErrNotFound {
			return 0, 0, nil // stats key absent means empty db
		}
		return 0, 0, mapErr(err)
	}
	defer func() { _ = closer.Close() }()
	if len(val) < 16 {
		return 0, 0, fmt.Errorf("chainstore/pebble: corrupt stats record (len=%d)", len(val))
	}
	entries := int64(binary.BigEndian.Uint64(val[0:8]))
	bytes := int64(binary.BigEndian.Uint64(val[8:16]))
	return entries, bytes, nil
}

// Close releases all pebble resources.
func (b *Backend) Close() error {
	return b.db.Close()
}

// OpenBackend opens a Pebble database at dir and returns the raw Backend.
// Use this when you need direct backend access (e.g. in tests to inspect node state).
// For normal use prefer Open, which wires the backend into a *chainstore.Store.
// opts may be nil; StatsMerger is always set to enable stats accumulation.
func OpenBackend(dir string, opts *crdbpebble.Options) (*Backend, error) {
	if opts == nil {
		opts = &crdbpebble.Options{}
	}
	opts.Merger = StatsMerger
	db, err := crdbpebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &Backend{db: db}, nil
}

// NewBackend wraps an already-opened pebble.DB into a Backend.
// Use this when you need to open Pebble with custom options not surfaced by
// OpenBackend (e.g. ReadOnly mode for cmd/cache-check). The caller retains
// ownership of db and must Close it after the Backend is no longer needed.
//
// The caller is REQUIRED to have opened db with opts.Merger = StatsMerger.
// Without it, pebble.Open on a previously-written store will fail with a
// merger-name mismatch on the stats key.
func NewBackend(db *crdbpebble.DB) *Backend {
	return &Backend{db: db}
}

// Open creates a pebble.Backend at dir, wires it into cfg, and returns a
// fully-started *chainstore.Store. It is the standard entry point for production use.
// Pass dir="" with vfs.NewMem() in Options.FS for in-memory use in tests.
// opts may be nil; StatsMerger is always set to enable stats accumulation.
func Open(ctx context.Context, dir string, opts *crdbpebble.Options, cfg chainstore.Config) (*chainstore.Store, error) {
	b, err := OpenBackend(dir, opts)
	if err != nil {
		return nil, err
	}
	cfg.Backend = b
	return chainstore.New(ctx, cfg)
}
