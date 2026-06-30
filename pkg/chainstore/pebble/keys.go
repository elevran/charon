package pebble

import (
	"encoding/binary"

	"github.com/elevran/charon/pkg/chainstore"
)

const (
	pfxMeta     = byte(0x01) // metaKey(nodeID)         → Node binary encoding
	pfxBlob     = byte(0x02) // blobKey(blobID)         → assembled blob bytes (single-blob path)
	pfxLRU      = byte(0x03) // lruKey(bucket, nodeID)  → empty value; sorted by (bucket, nodeID)
	pfxChildren = byte(0x04) // childKey(parent, child) → empty value; enables GetChildren scan
	pfxStats    = byte(0x05) // statsKey                → counters
	pfxStaging  = byte(0x06) // stagingKey(stagingID)   → blobID (tiny pointer; deleted at Store)
	pfxChunk    = byte(0x07) // chunkKey(blobID, seq)   → chunk bytes (Phase 6)
	pfxManifest = byte(0x08) // manifestKey(blobID)     → Manifest encoding (Phase 6)
)

func metaKey(id chainstore.NodeID) []byte {
	return append([]byte{pfxMeta}, id[:]...)
}

func blobKey(id chainstore.BlobID) []byte {
	return append([]byte{pfxBlob}, id[:]...)
}

func stagingKey(stagingID []byte) []byte {
	return append([]byte{pfxStaging}, stagingID...)
}

func manifestKey(id chainstore.BlobID) []byte {
	return append([]byte{pfxManifest}, id[:]...)
}

func chunkKey(id chainstore.BlobID, seq uint32) []byte {
	k := make([]byte, 1+16+4)
	k[0] = pfxChunk
	copy(k[1:17], id[:])
	binary.BigEndian.PutUint32(k[17:21], seq)
	return k
}

func lruKey(bucket chainstore.BucketID, id chainstore.NodeID) []byte {
	k := make([]byte, 1+8+20)
	k[0] = pfxLRU
	binary.BigEndian.PutUint64(k[1:9], uint64(bucket))
	copy(k[9:], id[:])
	return k
}

func childKey(parent, child chainstore.NodeID) []byte {
	k := make([]byte, 1+20+20)
	k[0] = pfxChildren
	copy(k[1:21], parent[:])
	copy(k[21:41], child[:])
	return k
}

var statsKey = []byte{pfxStats, 0x01}
