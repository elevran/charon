package pebble

import (
	"context"
	"encoding/binary"
	"fmt"

	crdbpebble "github.com/cockroachdb/pebble"

	"github.com/elevran/charon/pkg/chainstore"
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
// on an otherwise intact chain.
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
			return nil, chainstore.ErrChainCorrupted
		}
		node := decodeNode(val)
		_ = closer.Close()
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
	return decodeNode(val), nil
}

// GetBlob fetches the opaque blob for a node.
func (b *Backend) GetBlob(_ context.Context, node chainstore.Node) ([]byte, error) {
	val, closer, err := b.db.Get(blobKey(node.BlobID))
	if err != nil {
		return nil, mapErr(err)
	}
	defer func() { _ = closer.Close() }()
	// Copy the value since the closer invalidates the slice.
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// Commit translates a domain Transaction into a pebble.Batch and commits atomically.
func (b *Backend) Commit(_ context.Context, tx chainstore.Transaction) error {
	batch := b.db.NewBatch()
	defer func() { _ = batch.Close() }()

	for _, n := range tx.PutNodes {
		if err := batch.Set(metaKey(n.ID), encodeNode(n), nil); err != nil {
			return err
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

	for _, bm := range tx.BucketMoves {
		if err := batch.Delete(lruKey(bm.OldBucket, bm.NodeID), nil); err != nil {
			return err
		}
		if err := batch.Set(lruKey(bm.NewBucket, bm.NodeID), nil, nil); err != nil {
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
	upper := lruKey(bucket+1, chainstore.NodeID{})
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

// Open creates a pebble.Backend at dir, wires it into cfg, and returns a
// fully-started *chainstore.Store. It is the standard entry point for production use.
// Pass dir="" with vfs.NewMem() in Options.FS for in-memory use in tests.
// opts may be nil; StatsMerger is always set to enable stats accumulation.
func Open(dir string, opts *crdbpebble.Options, cfg chainstore.Config) (*chainstore.Store, error) {
	if opts == nil {
		opts = &crdbpebble.Options{}
	}
	opts.Merger = StatsMerger
	db, err := crdbpebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}
	cfg.Backend = &Backend{db: db}
	return chainstore.New(cfg)
}
