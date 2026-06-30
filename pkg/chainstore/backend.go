package chainstore

import "context"

// Backend is the only interface the business logic sees.
// Implementations map these domain operations to their native KV primitives.
// No Batch, Iterator, Scan, or key encodings appear above this interface.
// Named chainstore.Backend at call sites; concrete types are pebble.Backend,
// dynamodb.Backend, etc. — qualified by their sub-package name.
type Backend interface {
	// LoadChain walks from leaf to root and returns nodes leaf-first.
	// pebble.Backend: wraps traversal in a snapshot for consistent reads.
	// Cloud backends: optimistic reads + ErrChainCorrupted on mid-walk miss.
	LoadChain(ctx context.Context, leaf NodeID) ([]Node, error)

	// GetNode fetches a single node's metadata. ErrNotFound if absent.
	GetNode(ctx context.Context, id NodeID) (Node, error)

	// GetBlob fetches the opaque blob for a node. ErrNotFound if absent.
	// Uses Node.BlobID + Node.BlobType to dispatch between single-blob and
	// chunked reads (Phase 6 chunked reads implemented via GetManifest/GetChunk).
	GetBlob(ctx context.Context, node Node) ([]byte, error)

	// Commit atomically applies all mutations in tx.
	// OpID makes commits idempotent — safe to retry on timeout (cloud backends).
	// pebble.Backend: tx → pebble.Batch committed with pebble.Sync.
	// dynamodb.Backend: tx → TransactWriteItems (25-item batching for large tx).
	// fdb.Backend: tx → FDB transaction with MVCC.
	Commit(ctx context.Context, tx Transaction) error

	// GetEvictionCandidates returns up to limit nodes from the given bucket.
	// Results within the bucket may be in any order.
	// pebble.Backend: iterator scan on lru prefix.
	// dynamodb.Backend: Query(PK=bucket, Limit=limit).
	GetEvictionCandidates(ctx context.Context, bucket BucketID, limit int) ([]NodeID, error)

	// OldestBucket returns the BucketID of the oldest non-empty bucket,
	// or ErrNotFound if the store is empty.
	// pebble.Backend: SeekToFirst() on the lru prefix.
	// dynamodb.Backend: Query(PK="LRU", ScanIndexForward=true, Limit=1).
	OldestBucket(ctx context.Context) (BucketID, error)

	// GetChildren returns the direct children of parentID.
	GetChildren(ctx context.Context, parentID NodeID) ([]NodeID, error)

	// Stats returns current entry count and total blob bytes.
	Stats(ctx context.Context) (entries int64, bytes int64, err error)

	// --- Phase 6 stubs: defined now so the interface is stable across phases.
	// Implementations return ErrNotImplemented until Phase 6. ---

	// PutChunk writes a single chunk durably. (Phase 6)
	PutChunk(ctx context.Context, blobID BlobID, seq uint32, data []byte) error

	// PutManifest writes the manifest record — the commit point for a chunked blob. (Phase 6)
	PutManifest(ctx context.Context, blobID BlobID, m Manifest) error

	// GetManifest reads the manifest. ErrNotFound if blob is non-chunked. (Phase 6)
	GetManifest(ctx context.Context, blobID BlobID) (Manifest, error)

	// GetChunk reads one chunk by blob ID and sequence number. (Phase 6)
	GetChunk(ctx context.Context, blobID BlobID, seq uint32) ([]byte, error)

	// Close releases all resources.
	Close() error
}
