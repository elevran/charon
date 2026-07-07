package pebble

import (
	"encoding/binary"
	"fmt"

	"github.com/elevran/charon/internal/chainstore"
)

// Node wire layout (111 bytes, big-endian for multi-byte fields):
//
//	offset  size  field
//	  0       1    Version
//	  1      20    ID
//	 21      20    ParentID           (zero = root)
//	 41      16    RequestBlobID      (zero until request is stored)
//	 57      16    ResponseBlobID     (zero until turn is completed; interpretation depends on BlobType)
//	 73       8    LastAccessUnix
//	 81       8    CreatedAt
//	 89       8    BucketID
//	 97       4    RequestBlobSize
//	101       4    ResponseBlobSize
//	105       4    Depth
//	109       1    Status             (0=completed, 1=failed)
//	110       1    BlobType           (Phase 6 — 0=single, 1=chunked)
//
// Node.ResponseID is NOT encoded here; it is stored as a separate
// pfxResponseID key (see keys.go) to keep this record fixed-size.
//
// Big-endian is used for multi-byte numeric fields for consistency with lruKey,
// where big-endian BucketID encoding is required for correct lexicographic sort order.
//
// Backwards compatibility: nodes written by the Phase 5 layout (110 bytes) are
// decoded as if BlobType=Single (zero). Chunked nodes always carry BlobType=Chunked
// because the chunked commit path sets the value explicitly.
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
	b[110] = uint8(n.BlobType)
	return b
}

func decodeNode(b []byte) (chainstore.Node, error) {
	if len(b) < nodeSize {
		return chainstore.Node{}, fmt.Errorf("short node record: len=%d", len(b))
	}
	if b[0] != 1 {
		return chainstore.Node{}, fmt.Errorf("unsupported node version: %d", b[0])
	}
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
	n.BlobType = chainstore.BlobType(b[110])
	return n, nil
}

// encodeManifest / decodeManifest round-trip the fixed-size ManifestEntry
// record stored at pfxManifest+blobID. The wire layout is 8 bytes total:
//
//	offset  size  field
//	  0       4    ChunkCount
//	  4       4    TotalSize
//
// Note: ManifestEntry.BlobID is the key (pfxManifest+blobID) — it is NOT
// encoded in the value bytes. Decoders patch it back from the key they read.
const manifestSize = 8

func encodeManifest(m chainstore.ManifestEntry) []byte {
	b := make([]byte, manifestSize)
	binary.BigEndian.PutUint32(b[0:4], m.ChunkCount)
	binary.BigEndian.PutUint32(b[4:8], m.TotalSize)
	return b
}

func decodeManifest(b []byte) (chainstore.ManifestEntry, error) {
	if len(b) < manifestSize {
		return chainstore.ManifestEntry{}, fmt.Errorf("short manifest record: len=%d", len(b))
	}
	return chainstore.ManifestEntry{
		ChunkCount: binary.BigEndian.Uint32(b[0:4]),
		TotalSize:  binary.BigEndian.Uint32(b[4:8]),
	}, nil
}
