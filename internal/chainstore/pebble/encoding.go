package pebble

import (
	"encoding/binary"

	"github.com/elevran/charon/internal/chainstore"
)

// Node wire layout (111 bytes, big-endian for multi-byte fields):
//
//	offset  size  field
//	  0       1    Version
//	  1      20    ID
//	 21      20    ParentID         (zero = root)
//	 41      16    RequestBlobID    (zero until request is stored)
//	 57      16    ResponseBlobID   (zero until turn is completed)
//	 73       8    LastAccessUnix
//	 81       8    CreatedAt
//	 89       8    BucketID
//	 97       4    RequestBlobSize
//	101       4    ResponseBlobSize
//	105       4    Depth
//	109       1    Status           (0=completed, 1=failed)
//	110       1    BlobType         (0=single, 1=chunked — Phase 6)
//
// Node.ResponseID is NOT encoded here; it is stored as a separate
// pfxResponseID key (see keys.go) to keep this record fixed-size.
//
// Big-endian is used for multi-byte numeric fields for consistency with lruKey,
// where big-endian BucketID encoding is required for correct lexicographic sort order.
const nodeSize = 111

func encodeNode(n chainstore.Node) []byte {
	b := make([]byte, nodeSize)
	b[0] = n.Version
	copy(b[1:21], n.ID[:])
	copy(b[21:41], n.ParentID[:])
	copy(b[41:57], n.RequestBlobID[:])
	copy(b[57:73], n.ResponseBlobID[:])
	binary.BigEndian.PutUint64(b[73:81], uint64(n.LastAccessUnix))
	binary.BigEndian.PutUint64(b[81:89], uint64(n.CreatedAt))
	binary.BigEndian.PutUint64(b[89:97], uint64(n.BucketID))
	binary.BigEndian.PutUint32(b[97:101], n.RequestBlobSize)
	binary.BigEndian.PutUint32(b[101:105], n.ResponseBlobSize)
	binary.BigEndian.PutUint32(b[105:109], n.Depth)
	b[109] = n.Status
	b[110] = n.BlobType
	return b
}

func decodeNode(b []byte) chainstore.Node {
	var n chainstore.Node
	n.Version = b[0]
	copy(n.ID[:], b[1:21])
	copy(n.ParentID[:], b[21:41])
	copy(n.RequestBlobID[:], b[41:57])
	copy(n.ResponseBlobID[:], b[57:73])
	n.LastAccessUnix = int64(binary.BigEndian.Uint64(b[73:81]))
	n.CreatedAt = int64(binary.BigEndian.Uint64(b[81:89]))
	n.BucketID = chainstore.BucketID(binary.BigEndian.Uint64(b[89:97]))
	n.RequestBlobSize = binary.BigEndian.Uint32(b[97:101])
	n.ResponseBlobSize = binary.BigEndian.Uint32(b[101:105])
	n.Depth = binary.BigEndian.Uint32(b[105:109])
	n.Status = b[109]
	n.BlobType = b[110]
	return n
}
