package chainstore

import (
	"errors"
	"time"
)

// Sentinel errors returned by *Store methods.
var (
	ErrNotFound       = errors.New("not found")
	ErrChainCorrupted = errors.New("chain corrupted: missing node in parent chain")
	ErrChainExpired   = errors.New("chain expired: ancestor node was capacity-evicted")
	ErrStoreFull      = errors.New("store full: configured capacity exceeded")
	ErrChainTooDeep   = errors.New("chain too deep: depth would overflow")
	ErrNotImplemented = errors.New("not implemented")
)

// Turn is the data returned to callers by Resolve.
// RequestBlob and ResponseBlob are verbatim bytes stored by the proxy at turn creation
// and turn completion respectively. ResponseBlob is nil for turns not yet completed.
type Turn struct {
	ResponseID   string
	RequestBlob  []byte
	ResponseBlob []byte
}

// PublicNode is the subset of Node fields safe to expose in HTTP responses.
// Internal eviction details (BlobIDs, BucketID, NodeID, ParentID) are absent.
type PublicNode struct {
	CreatedAt int64 // Unix seconds; set at store time
	ExpiresAt int64 // Unix seconds; 0 if TTL is disabled
	Depth     uint32
	Status    uint8
	Version   uint8
}

// PublicFromNode constructs a PublicNode from a Node and the store's configured TTL.
// ExpiresAt is computed as LastAccessUnix + ttl; 0 when ttl is zero (TTL disabled).
func PublicFromNode(n Node, ttl time.Duration) PublicNode {
	var expiresAt int64
	if ttl > 0 {
		expiresAt = n.LastAccessUnix + int64(ttl.Seconds())
	}
	return PublicNode{
		CreatedAt: n.CreatedAt,
		ExpiresAt: expiresAt,
		Depth:     n.Depth,
		Status:    n.Status,
		Version:   n.Version,
	}
}
