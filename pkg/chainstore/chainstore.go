package chainstore

import (
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

// BlobID is a 16-byte UUID identifying a blob in pfxBlob.
// Indirection: Node.BlobID → pfxBlob:blobID → blob bytes.
// The blob never moves; only the tiny BlobID pointer is updated on staging rename.
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
	ID             NodeID
	ParentID       NodeID   // zero value = root node
	BlobID         BlobID   // UUID → pfxBlob or pfxChunk+pfxManifest; indirection avoids blob copy
	LastAccessUnix int64    // Unix seconds; updated on every access
	CreatedAt      int64    // Unix seconds; set at store time, never updated
	ExpiresAt      int64    // Unix seconds; 0 = no expiry; drives TTL reaper cutoff
	BucketID       BucketID // bucket at last promotion; ground truth for eviction order
	BlobSize       uint32
	Depth          uint32 // 0 = root; enables slice preallocation in LoadChain
	Status         uint8  // 0=completed, 1=failed; drives failed-response path in Store
	BlobType       uint8  // 0=single (pfxBlob), 1=chunked (pfxManifest+pfxChunk); drives read path
	Version        uint8  // schema version; currently 1
}

// Node status constants.
const (
	NodeStatusCompleted uint8 = 0
	NodeStatusFailed    uint8 = 1
)

// Blob type constants.
const (
	BlobTypeSingle  uint8 = 0 // blob stored as one key under pfxBlob
	BlobTypeChunked uint8 = 1 // blob stored as N chunks under pfxChunk + manifest under pfxManifest
)

// BlobEntry carries a blob's ID and raw bytes in a Transaction.
// Key is BlobID (UUID minted at write time) — the blob is stored at pfxBlob:blobID.
// The Node references this BlobID via Node.BlobID.
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

// ChildEntry declares a parent→child relationship to be recorded in pfxChildren.
// Required because TTL reaper's deleteSubtree walks via GetChildren.
type ChildEntry struct {
	Parent NodeID
	Child  NodeID
}

// StatsDelta is applied atomically with each Transaction.
type StatsDelta struct {
	EntryDelta int64
	BytesDelta int64
}

// Transaction is a description of what to change, not how.
// Each backend translates it to its native atomic primitive.
// OpID makes commits idempotent — backends that support retries use it; Pebble ignores it.
type Transaction struct {
	OpID uuid.UUID

	PutNodes    []Node
	DeleteNodes []NodeID

	PutBlobs    []BlobEntry
	DeleteBlobs []BlobID

	PutChildren    []ChildEntry
	DeleteChildren []NodeID

	BucketMoves []BucketMove

	StatsDelta StatsDelta
}

// Manifest is the commit record for a chunked blob (Phase 6).
type Manifest struct {
	BlobID     BlobID
	ChunkCount uint32
	TotalSize  uint64
}
