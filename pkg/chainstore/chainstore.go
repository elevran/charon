package chainstore

import (
	"context"
	"crypto/sha1"

	"github.com/google/uuid"
)

// NodeID is a 20-byte opaque node identifier computed by Charon.
// NodeID = sha1(tenantKey + responseID). Callers never construct NodeIDs directly —
// all public Store methods accept string response IDs and an optional tenant key.
type NodeID [20]byte

// nodeID computes the internal NodeID from an external response ID and tenant key.
// tenantKey may be empty for single-tenant deployments.
func nodeID(tenantKey, responseID string) NodeID {
	return NodeID(sha1.Sum([]byte(tenantKey + responseID)))
}

// BucketID identifies an LRU time bucket: BucketID = UnixSeconds / BucketDuration.
type BucketID uint64

// BlobID is a 16-byte UUID identifying a blob.
// A Node holds a BlobID pointer so the blob bytes never need to move during bucket promotions.
type BlobID [16]byte

// Turn is the data returned to callers: the node's ID and its opaque blob.
// Blob encoding is the caller's concern — Charon stores and returns it verbatim.
type Turn struct {
	ID   NodeID
	Blob []byte // opaque bytes; caller owns encoding (e.g. JSON)
}

// Node is the domain metadata record for one conversation turn.
// This is what business logic reads and writes — no encoding details leak out.
// Encoding (binary layout, key prefixes) is entirely backend-internal.
type Node struct {
	Version        uint8 // schema version; currently 1
	ID             NodeID
	ParentID       NodeID   // zero value = root node
	BlobID         BlobID   // UUID → single blob or chunked blob; indirection avoids blob copy on promotion
	LastAccessUnix int64    // Unix seconds; updated on access (actual promotion strategy may batch to reduce write amplification)
	CreatedAt      int64    // Unix seconds; set at store time, never updated
	ExpiresAt      int64    // Unix seconds; 0 = no expiry; stored explicitly so TTL config is not needed at read time
	BucketID       BucketID // bucket at last promotion; ground truth for eviction order
	BlobSize       uint32
	Depth          uint32 // 0 = root; enables slice preallocation in LoadChain
	Status         uint8  // NodeStatusCompleted or NodeStatusFailed; drives failed-response path
	BlobType       uint8  // BlobTypeSingle or BlobTypeChunked; drives read dispatch
}

// Node status constants.
const (
	NodeStatusCompleted uint8 = iota
	NodeStatusFailed
)

// Blob type constants.
const (
	BlobTypeSingle  uint8 = iota // blob stored as one key
	BlobTypeChunked              // blob stored as N chunks with a manifest (Phase 6)
)

// BlobEntry carries a blob's ID and raw bytes in a Transaction.
type BlobEntry struct {
	BlobID BlobID
	Data   []byte
}

// BucketMove records a bucket-index promotion for one node.
type BucketMove struct {
	NodeID    NodeID
	OldBucket BucketID
	NewBucket BucketID
}

// ChildEntry declares a parent→child relationship to be recorded in the backend.
// Required so the TTL reaper can walk subtrees via GetChildren.
type ChildEntry struct {
	Parent NodeID
	Child  NodeID
}

// StatsDelta carries atomic counter adjustments for a Transaction.
// Applied together with the KV mutations so counts stay consistent across restarts.
type StatsDelta struct {
	EntryDelta int64
	BytesDelta int64
}

// Transaction describes what to change, not how.
// Each backend translates it to its native atomic primitive.
// OpID makes commits idempotent — backends that support retries use it to detect
// duplicate deliveries; local backends (Pebble) ignore it.
type Transaction struct {
	OpID uuid.UUID

	PutNodes    []Node
	DeleteNodes []NodeID

	PutBlobs    []BlobEntry
	DeleteBlobs []BlobID

	PutChildren    []ChildEntry
	DeleteChildren []ChildEntry

	BucketMoves []BucketMove

	StatsDelta StatsDelta
}

// Manifest is the commit record for a chunked blob.
// Written atomically as the last step of a chunked-blob store to make it readable.
type Manifest struct {
	BlobID     BlobID
	ChunkCount uint32
	TotalSize  uint64
}

// Backend is the only interface the business logic sees.
// Each implementation maps these domain operations to its native storage primitives.
// No key encodings, batch primitives, or scan APIs appear above this boundary.
type Backend interface {
	// LoadChain walks from leaf to root and returns nodes leaf-first.
	// Returns ErrNotFound if leaf is absent; ErrChainCorrupted if a parent pointer dangles.
	LoadChain(ctx context.Context, leaf NodeID) ([]Node, error)

	// GetNode fetches a single node's metadata. ErrNotFound if absent.
	GetNode(ctx context.Context, id NodeID) (Node, error)

	// GetBlob fetches the opaque blob for a node. ErrNotFound if absent.
	// Dispatches on BlobType: single-blob reads directly; chunked returns ErrNotImplemented until Phase 6.
	GetBlob(ctx context.Context, node Node) ([]byte, error)

	// Commit atomically applies all mutations in tx.
	// OpID makes commits idempotent — safe to retry on transient failures.
	Commit(ctx context.Context, tx Transaction) error

	// GetEvictionCandidates returns up to limit node IDs from the given bucket.
	// Results within the bucket may be in any order.
	GetEvictionCandidates(ctx context.Context, bucket BucketID, limit int) ([]NodeID, error)

	// OldestBucket returns the BucketID of the oldest non-empty bucket,
	// or ErrNotFound if the store is empty.
	OldestBucket(ctx context.Context) (BucketID, error)

	// GetChildren returns the direct children of parentID.
	GetChildren(ctx context.Context, parentID NodeID) ([]NodeID, error)

	// Stats returns current entry count and total blob bytes.
	Stats(ctx context.Context) (entries int64, bytes int64, err error)

	// --- Chunked-blob support: implementations return ErrNotImplemented until Phase 6. ---

	// PutChunk writes a single chunk durably.
	PutChunk(ctx context.Context, blobID BlobID, seq uint32, data []byte) error

	// PutManifest writes the manifest — the commit point that makes a chunked blob readable.
	PutManifest(ctx context.Context, blobID BlobID, m Manifest) error

	// GetManifest reads the manifest. ErrNotFound if blob is not chunked.
	GetManifest(ctx context.Context, blobID BlobID) (Manifest, error)

	// GetChunk reads one chunk by blob ID and sequence number.
	GetChunk(ctx context.Context, blobID BlobID, seq uint32) ([]byte, error)

	// Close releases all resources.
	Close() error
}
