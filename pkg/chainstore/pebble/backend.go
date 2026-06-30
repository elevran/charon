package pebble

import (
	"context"
	"encoding/binary"
	"fmt"

	gogopebble "github.com/cockroachdb/pebble"
	"github.com/elevran/charon/pkg/chainstore"
)

// Backend implements chainstore.Backend using CockroachDB Pebble as the
// underlying KV engine. All key encoding, snapshot management, and batch
// construction are internal to this package.
type Backend struct{ db *gogopebble.DB }

// LoadChain opens a Pebble snapshot for consistent reads, then walks parent pointers.
// A snapshot prevents a concurrent deletion from causing a mid-walk ErrNotFound
// on an otherwise intact chain.
func (b *Backend) LoadChain(ctx context.Context, leaf chainstore.NodeID) ([]chainstore.Node, error) {
	snap := b.db.NewSnapshot()
	defer snap.Close()
	var nodes []chainstore.Node
	cur := leaf
	for {
		val, closer, err := snap.Get(metaKey(cur))
		if err != nil {
			if closer != nil {
				closer.Close()
			}
			if err == gogopebble.ErrNotFound {
				if len(nodes) == 0 {
					return nil, chainstore.ErrNotFound
				}
				return nil, chainstore.ErrChainCorrupted
			}
			return nil, chainstore.ErrChainCorrupted
		}
		node := decodeNode(val)
		closer.Close()
		nodes = append(nodes, node)
		if node.ParentID == (chainstore.NodeID{}) {
			break
		}
		cur = node.ParentID
	}
	return nodes, nil
}

// GetNode fetches a single node's metadata. ErrNotFound if absent.
func (b *Backend) GetNode(ctx context.Context, id chainstore.NodeID) (chainstore.Node, error) {
	val, closer, err := b.db.Get(metaKey(id))
	if err != nil {
		if err == gogopebble.ErrNotFound {
			return chainstore.Node{}, chainstore.ErrNotFound
		}
		return chainstore.Node{}, err
	}
	defer closer.Close()
	return decodeNode(val), nil
}

// GetBlob fetches the opaque blob for a node. Uses BlobType to dispatch.
func (b *Backend) GetBlob(ctx context.Context, node chainstore.Node) ([]byte, error) {
	if node.BlobType == chainstore.BlobTypeChunked {
		return nil, chainstore.ErrNotImplemented
	}
	val, closer, err := b.db.Get(blobKey(node.BlobID))
	if err != nil {
		if err == gogopebble.ErrNotFound {
			return nil, chainstore.ErrNotFound
		}
		return nil, err
	}
	defer closer.Close()
	// Copy the value since the closer invalidates the slice.
	out := make([]byte, len(val))
	copy(out, val)
	return out, nil
}

// Commit translates a domain Transaction into a pebble.Batch and commits atomically.
func (b *Backend) Commit(ctx context.Context, tx chainstore.Transaction) error {
	batch := b.db.NewBatch()

	for _, n := range tx.PutNodes {
		if err := batch.Set(metaKey(n.ID), encodeNode(n), nil); err != nil {
			return err
		}
		if n.ParentID != (chainstore.NodeID{}) {
			if err := batch.Set(childKey(n.ParentID, n.ID), nil, nil); err != nil {
				return err
			}
		}
		// Write lru entry for new node (only written at initial insert here; moves via BucketMoves).
		if err := batch.Set(lruKey(n.BucketID, n.ID), nil, nil); err != nil {
			return err
		}
	}

	for _, id := range tx.DeleteNodes {
		if err := batch.Delete(metaKey(id), nil); err != nil {
			return err
		}
	}

	for _, parent := range tx.DeleteChildren {
		// We need to scan for children of this parent and delete them.
		// However, in the deletion case, the Transaction should specify
		// what child relationships to remove. For simplicity here we accept
		// (parent, child) pairs. DeleteChildren contains parent IDs — we need
		// to also know which children. Since DeleteNodes contains the node IDs,
		// we cross-reference them.
		for _, id := range tx.DeleteNodes {
			if err := batch.Delete(childKey(parent, id), nil); err != nil {
				return err
			}
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

	return batch.Commit(gogopebble.Sync)
}

// applyStatsDelta updates the persistent stats counter via Pebble's MERGE operation.
func (b *Backend) applyStatsDelta(batch *gogopebble.Batch, d chainstore.StatsDelta) error {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], uint64(d.EntryDelta))
	binary.BigEndian.PutUint64(buf[8:16], uint64(d.BytesDelta))
	return batch.Merge(statsKey, buf[:], nil)
}

// OldestBucket uses SeekToFirst on the lru prefix — O(log N), no full scan.
func (b *Backend) OldestBucket(ctx context.Context) (chainstore.BucketID, error) {
	iter, err := b.db.NewIter(&gogopebble.IterOptions{
		LowerBound: []byte{pfxLRU},
		UpperBound: []byte{pfxLRU + 1},
	})
	if err != nil {
		return 0, err
	}
	defer iter.Close()
	if !iter.First() {
		return 0, chainstore.ErrNotFound
	}
	bucket := chainstore.BucketID(binary.BigEndian.Uint64(iter.Key()[1:9]))
	return bucket, iter.Error()
}

// GetEvictionCandidates scans lru entries for the given bucket, returns up to limit NodeIDs.
func (b *Backend) GetEvictionCandidates(ctx context.Context, bucket chainstore.BucketID, limit int) ([]chainstore.NodeID, error) {
	lower := lruKey(bucket, chainstore.NodeID{})
	upper := lruKey(bucket+1, chainstore.NodeID{})
	iter, err := b.db.NewIter(&gogopebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()
	var ids []chainstore.NodeID
	for iter.First(); iter.Valid() && len(ids) < limit; iter.Next() {
		var id chainstore.NodeID
		copy(id[:], iter.Key()[9:])
		ids = append(ids, id)
	}
	return ids, iter.Error()
}

// GetChildren returns the direct children of parentID by scanning the pfxChildren prefix.
func (b *Backend) GetChildren(ctx context.Context, parentID chainstore.NodeID) ([]chainstore.NodeID, error) {
	// childKey layout: pfxChildren(1) + parent(20) + child(20) = 41 bytes
	lower := childKey(parentID, chainstore.NodeID{})
	// Upper bound: increment last byte of lower (safe because child section is all zeros for lower).
	// Actually, we want all keys with pfxChildren + parentID prefix.
	// Upper = pfxChildren + parentID + {0xff...ff} = just increment the parent by one.
	upper := make([]byte, 41)
	upper[0] = pfxChildren
	copy(upper[1:21], parentID[:])
	// Set child section to all 0xff for upper bound.
	for i := 21; i < 41; i++ {
		upper[i] = 0xff
	}

	iter, err := b.db.NewIter(&gogopebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer iter.Close()

	var children []chainstore.NodeID
	for iter.First(); iter.Valid(); iter.Next() {
		var child chainstore.NodeID
		copy(child[:], iter.Key()[21:41])
		children = append(children, child)
	}
	return children, iter.Error()
}

// Stats returns the persistent entry count and total blob bytes.
func (b *Backend) Stats(ctx context.Context) (int64, int64, error) {
	val, closer, err := b.db.Get(statsKey)
	if err != nil {
		if err == gogopebble.ErrNotFound {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	defer closer.Close()
	if len(val) < 16 {
		return 0, 0, fmt.Errorf("chainstore/pebble: corrupt stats record (len=%d)", len(val))
	}
	entries := int64(binary.BigEndian.Uint64(val[0:8]))
	bytes := int64(binary.BigEndian.Uint64(val[8:16]))
	return entries, bytes, nil
}

// PutChunk is a Phase 6 stub.
func (b *Backend) PutChunk(ctx context.Context, blobID chainstore.BlobID, seq uint32, data []byte) error {
	return chainstore.ErrNotImplemented
}

// PutManifest is a Phase 6 stub.
func (b *Backend) PutManifest(ctx context.Context, blobID chainstore.BlobID, m chainstore.Manifest) error {
	return chainstore.ErrNotImplemented
}

// GetManifest is a Phase 6 stub.
func (b *Backend) GetManifest(ctx context.Context, blobID chainstore.BlobID) (chainstore.Manifest, error) {
	return chainstore.Manifest{}, chainstore.ErrNotImplemented
}

// GetChunk is a Phase 6 stub.
func (b *Backend) GetChunk(ctx context.Context, blobID chainstore.BlobID, seq uint32) ([]byte, error) {
	return nil, chainstore.ErrNotImplemented
}

// Close releases all pebble resources.
func (b *Backend) Close() error {
	return b.db.Close()
}
