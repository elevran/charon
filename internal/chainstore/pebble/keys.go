package pebble

import (
	"encoding/binary"

	"github.com/elevran/charon/internal/chainstore"
)

const (
	pfxMeta        = byte(0x01) // metaKey(nodeID)                 → Node binary encoding (fixed-size)
	pfxBlob        = byte(0x02) // blobKey(blobID)                 → assembled blob bytes (single-blob path)
	pfxLRU         = byte(0x03) // lruKey(bucket, nodeID)          → empty value; sorted by (bucket, nodeID)
	pfxChildren    = byte(0x04) // childKey(parent, child)         → empty value; enables GetChildren scan
	pfxStats       = byte(0x05) // statsKey                        → counters
	pfxResponseID  = byte(0x06) // responseIDKey(nodeID)           → caller-supplied responseID string
	pfxStaging     = byte(0x07) // stagingKey(stagingID)           → partial Node (invisible to chain walks)
	pfxChunk       = byte(0x08) // chunkKey(blobID, offset)        → one batch of a chunked response
	pfxManifest    = byte(0x09) // manifestKey(blobID)             → fixed-size ManifestEntry (atomic commit point)
	pfxStagingRID  = byte(0x0a) // stagingResponseIDKey(stagingID) → bound responseID for early-binding on staging records
	pfxRespIdx     = byte(0x0b) // responseIDIndexKey(responseID)  → stagingID (for /responses/{response_id} lookup)
	pfxStagingNext = byte(0x0c) // stagingNextKey(stagingID)       → uint32 big-endian next-expected chunk offset
	pfxStagingDone = byte(0x0d) // stagingDoneKey(stagingID)       → bound responseID (or empty for aborted) — set on complete/abort; GET /staging/{id} returns 410 when this exists
)

// chunkKey layout: pfxChunk (1) + blobID (16) + offset (4, big-endian).
// Big-endian offset yields offset-ordered iteration via Pebble SeekGE/Next.
func chunkKey(id chainstore.BlobID, offset uint32) []byte {
	k := make([]byte, 21)
	k[0] = pfxChunk
	copy(k[1:17], id[:])
	binary.BigEndian.PutUint32(k[17:21], offset)
	return k
}

// chunkKeyPrefix returns the pfxChunk+blobID prefix used by ListChunks scans.
func chunkKeyPrefix(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxChunk
	copy(k[1:17], id[:])
	return k
}

// manifestKey layout: pfxManifest (1) + blobID (16).
func manifestKey(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxManifest
	copy(k[1:17], id[:])
	return k
}

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

// stagingResponseIDKey is the pfxStagingRID-keyed location of the responseID
// bound to a staging record.  Written when the proxy calls BindResponseID;
// read by GetStagingNode to surface the bound value to the API layer.
func stagingResponseIDKey(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxStagingRID
	copy(k[1:], id[:])
	return k
}

// responseIDIndexKey maps a responseID to its stagingID — written when
// BindResponseID binds a responseID to a staging record, deleted when the
// staging record is deleted.  Powers the GET /responses/by-response-id/{rid}
// reverse lookup. Value is the 16-byte stagingID BlobID.
func responseIDIndexKey(responseID string) []byte {
	k := make([]byte, 1+len(responseID))
	k[0] = pfxRespIdx
	copy(k[1:], responseID)
	return k
}

// stagingNextKey stores the next-expected chunk offset for a staging
// record.  Updated atomically with each chunk write so the server can
// reject gaps (out-of-order chunks) without a full ListChunks scan.
func stagingNextKey(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxStagingNext
	copy(k[1:], id[:])
	return k
}

// stagingDoneKey marks a staging record as terminally complete or aborted.
// Set on /complete (value = bound responseID) and /abort (value = empty).
// GET /staging/{id} returns 410 Gone when this key exists; the record is
// otherwise invisible to the staging-status read path.
func stagingDoneKey(id chainstore.BlobID) []byte {
	k := make([]byte, 1+16)
	k[0] = pfxStagingDone
	copy(k[1:], id[:])
	return k
}

var statsKey = []byte{pfxStats, 0x01}
