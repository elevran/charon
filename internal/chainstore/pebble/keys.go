package pebble

import (
	"encoding/binary"

	"github.com/elevran/charon/internal/chainstore"
)

const (
	pfxMeta       = byte(0x01) // metaKey(nodeID)           → Node binary encoding (fixed-size)
	pfxBlob       = byte(0x02) // blobKey(blobID)           → assembled blob bytes (single-blob path)
	pfxLRU        = byte(0x03) // lruKey(bucket, nodeID)    → empty value; sorted by (bucket, nodeID)
	pfxChildren   = byte(0x04) // childKey(parent, child)   → empty value; enables GetChildren scan
	pfxStats      = byte(0x05) // statsKey                  → counters
	pfxResponseID = byte(0x06) // responseIDKey(nodeID)     → caller-supplied responseID string
	pfxStaging    = byte(0x07) // stagingKey(stagingID)     → partial Node (invisible to chain walks)
)

func metaKey(id chainstore.NodeID) []byte {
	k := make([]byte, 1+20)
	k[0] = pfxMeta
	copy(k[1:], id[:])
	return k
}

func blobKey(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxBlob
	copy(k[1:], id[:])
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

func responseIDKey(id chainstore.NodeID) []byte {
	k := make([]byte, 1+20)
	k[0] = pfxResponseID
	copy(k[1:], id[:])
	return k
}

func stagingKey(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxStaging
	copy(k[1:], id[:])
	return k
}

var statsKey = []byte{pfxStats, 0x01}
