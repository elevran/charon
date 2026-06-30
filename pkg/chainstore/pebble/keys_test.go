package pebble

import (
	"testing"

	"github.com/elevran/charon/pkg/chainstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestKeyPrefixNoCollision verifies that all prefix constants are distinct
// and that keys with different prefixes cannot collide.
func TestKeyPrefixNoCollision(t *testing.T) {
	prefixes := []byte{pfxMeta, pfxBlob, pfxLRU, pfxChildren, pfxStats, pfxStaging, pfxChunk, pfxManifest}
	seen := make(map[byte]bool)
	for _, p := range prefixes {
		require.False(t, seen[p], "duplicate prefix: 0x%02x", p)
		seen[p] = true
	}
}

func TestMetaKeyPrefix(t *testing.T) {
	var id chainstore.NodeID
	id[0] = 0xAB
	k := metaKey(id)
	assert.Equal(t, pfxMeta, k[0])
	assert.Equal(t, byte(0xAB), k[1])
	assert.Len(t, k, 1+20)
}

func TestBlobKeyPrefix(t *testing.T) {
	var id chainstore.BlobID
	id[0] = 0xCD
	k := blobKey(id)
	assert.Equal(t, pfxBlob, k[0])
	assert.Equal(t, byte(0xCD), k[1])
	assert.Len(t, k, 1+16)
}

func TestLruKeyLayout(t *testing.T) {
	bucket := chainstore.BucketID(0x0102030405060708)
	var id chainstore.NodeID
	id[0] = 0xFF
	k := lruKey(bucket, id)
	assert.Equal(t, pfxLRU, k[0])
	// Big-endian bucket bytes at [1:9].
	assert.Equal(t, []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08}, k[1:9])
	assert.Equal(t, byte(0xFF), k[9])
	assert.Len(t, k, 1+8+20)
}

func TestChildKeyLayout(t *testing.T) {
	var parent, child chainstore.NodeID
	parent[0] = 0x11
	child[0] = 0x22
	k := childKey(parent, child)
	assert.Equal(t, pfxChildren, k[0])
	assert.Equal(t, byte(0x11), k[1])
	assert.Equal(t, byte(0x22), k[21])
	assert.Len(t, k, 1+20+20)
}

func TestChunkKeyLayout(t *testing.T) {
	var id chainstore.BlobID
	id[0] = 0xAA
	k := chunkKey(id, 42)
	assert.Equal(t, pfxChunk, k[0])
	assert.Equal(t, byte(0xAA), k[1])
	// Sequence at [17:21] big-endian.
	assert.Equal(t, []byte{0x00, 0x00, 0x00, 0x2A}, k[17:21])
	assert.Len(t, k, 1+16+4)
}

func TestManifestKeyLayout(t *testing.T) {
	var id chainstore.BlobID
	id[0] = 0xBB
	k := manifestKey(id)
	assert.Equal(t, pfxManifest, k[0])
	assert.Equal(t, byte(0xBB), k[1])
	assert.Len(t, k, 1+16)
}

func TestStagingKeyLayout(t *testing.T) {
	sid := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	k := stagingKey(sid)
	assert.Equal(t, pfxStaging, k[0])
	assert.Equal(t, []byte{0xDE, 0xAD, 0xBE, 0xEF}, k[1:])
}

// TestLruKeyOrdering verifies that lruKey(bucketA, ...) < lruKey(bucketB, ...)
// when bucketA < bucketB (required for SeekToFirst to return oldest bucket).
func TestLruKeyOrdering(t *testing.T) {
	older := lruKey(chainstore.BucketID(100), chainstore.NodeID{})
	newer := lruKey(chainstore.BucketID(200), chainstore.NodeID{})
	assert.Less(t, string(older), string(newer))
}

// TestKeyNoCrossTypeCollision ensures a metaKey and a blobKey for the same
// underlying bytes cannot be equal (prefix prevents overlap).
func TestKeyNoCrossTypeCollision(t *testing.T) {
	var nodeID chainstore.NodeID
	var blobID chainstore.BlobID
	copy(nodeID[:], blobID[:]) // same underlying bytes

	mk := metaKey(nodeID)
	bk := blobKey(blobID)
	assert.NotEqual(t, mk[0], bk[0], "meta and blob keys must have different prefixes")
}
