package pebble

import (
	"encoding/binary"

	"github.com/elevran/charon/pkg/chainstore"
)

// Node wire layout (116 bytes, big-endian):
//
//	offset  size  field
//	  0      20    ID
//	 20      20    ParentID       (zero = root)
//	 40      16    BlobID         (UUID → pfxBlob or pfxChunk+pfxManifest)
//	 56       8    LastAccessUnix
//	 64       8    CreatedAt
//	 72       8    ExpiresAt      (0 = no expiry)
//	 80       8    BucketID
//	 88       4    BlobSize
//	 92       4    Depth
//	 96       1    Status         (0=completed, 1=failed)
//	 97       1    BlobType       (0=single, 1=chunked)
//	 98       1    Version
//	 99      17    reserved (zeroed)
const nodeSize = 116

func encodeNode(n chainstore.Node) []byte {
	b := make([]byte, nodeSize)
	copy(b[0:20], n.ID[:])
	copy(b[20:40], n.ParentID[:])
	copy(b[40:56], n.BlobID[:])
	binary.BigEndian.PutUint64(b[56:64], uint64(n.LastAccessUnix))
	binary.BigEndian.PutUint64(b[64:72], uint64(n.CreatedAt))
	binary.BigEndian.PutUint64(b[72:80], uint64(n.ExpiresAt))
	binary.BigEndian.PutUint64(b[80:88], uint64(n.BucketID))
	binary.BigEndian.PutUint32(b[88:92], n.BlobSize)
	binary.BigEndian.PutUint32(b[92:96], n.Depth)
	b[96] = n.Status
	b[97] = n.BlobType
	b[98] = n.Version
	// b[99:116] = reserved zeros (already zero from make)
	return b
}

func decodeNode(b []byte) chainstore.Node {
	var n chainstore.Node
	copy(n.ID[:], b[0:20])
	copy(n.ParentID[:], b[20:40])
	copy(n.BlobID[:], b[40:56])
	n.LastAccessUnix = int64(binary.BigEndian.Uint64(b[56:64]))
	n.CreatedAt = int64(binary.BigEndian.Uint64(b[64:72]))
	n.ExpiresAt = int64(binary.BigEndian.Uint64(b[72:80]))
	n.BucketID = chainstore.BucketID(binary.BigEndian.Uint64(b[80:88]))
	n.BlobSize = binary.BigEndian.Uint32(b[88:92])
	n.Depth = binary.BigEndian.Uint32(b[92:96])
	n.Status = b[96]
	n.BlobType = b[97]
	n.Version = b[98]
	return n
}
