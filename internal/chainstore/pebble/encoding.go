package pebble

import (
	"encoding/binary"

	"github.com/elevran/charon/internal/chainstore"
)

// Node wire layout (91 bytes, big-endian for multi-byte fields):
//
//	offset  size  field
//	  0       1    Version
//	  1      20    ID
//	 21      20    ParentID       (zero = root)
//	 41      16    BlobID
//	 57       8    LastAccessUnix
//	 65       8    CreatedAt
//	 73       8    BucketID
//	 81       4    BlobSize
//	 85       4    Depth
//	 89       1    Status         (0=completed, 1=failed)
//	 90       1    BlobType       (0=single, 1=chunked)
//
// Big-endian is used for multi-byte numeric fields for consistency with lruKey,
// where big-endian BucketID encoding is required for correct lexicographic sort order.
const nodeSize = 91

func encodeNode(n chainstore.Node) []byte {
	b := make([]byte, nodeSize)
	b[0] = n.Version
	copy(b[1:21], n.ID[:])
	copy(b[21:41], n.ParentID[:])
	copy(b[41:57], n.BlobID[:])
	binary.BigEndian.PutUint64(b[57:65], uint64(n.LastAccessUnix))
	binary.BigEndian.PutUint64(b[65:73], uint64(n.CreatedAt))
	binary.BigEndian.PutUint64(b[73:81], uint64(n.BucketID))
	binary.BigEndian.PutUint32(b[81:85], n.BlobSize)
	binary.BigEndian.PutUint32(b[85:89], n.Depth)
	b[89] = n.Status
	b[90] = n.BlobType
	return b
}

func decodeNode(b []byte) chainstore.Node {
	var n chainstore.Node
	n.Version = b[0]
	copy(n.ID[:], b[1:21])
	copy(n.ParentID[:], b[21:41])
	copy(n.BlobID[:], b[41:57])
	n.LastAccessUnix = int64(binary.BigEndian.Uint64(b[57:65]))
	n.CreatedAt = int64(binary.BigEndian.Uint64(b[65:73]))
	n.BucketID = chainstore.BucketID(binary.BigEndian.Uint64(b[73:81]))
	n.BlobSize = binary.BigEndian.Uint32(b[81:85])
	n.Depth = binary.BigEndian.Uint32(b[85:89])
	n.Status = b[89]
	n.BlobType = b[90]
	return n
}
