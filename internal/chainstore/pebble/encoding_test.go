package pebble

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
)

func TestEncodeDecodeRoundTrip(t *testing.T) {
	node := chainstore.Node{
		Version:          1,
		ID:               chainstore.NodeID{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
		ParentID:         chainstore.NodeID{21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 39, 40},
		RequestBlobID:    chainstore.BlobID{41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56},
		ResponseBlobID:   chainstore.BlobID{57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 68, 69, 70, 71, 72},
		LastAccessUnix:   1700000000,
		CreatedAt:        1699000000,
		BucketID:         chainstore.BucketID(472222),
		RequestBlobSize:  65536,
		ResponseBlobSize: 131072,
		Depth:            7,
		Status:           chainstore.NodeStatusFailed,
		BlobType:         chainstore.BlobTypeSingle,
	}

	encoded := encodeNode(node)
	require.Len(t, encoded, nodeSize, "encoded size must be nodeSize")

	decoded := decodeNode(encoded)
	assert.Equal(t, node, decoded, "round-trip must produce identical node")
}

func TestEncodeZeroNode(t *testing.T) {
	var node chainstore.Node
	encoded := encodeNode(node)
	decoded := decodeNode(encoded)
	assert.Equal(t, node, decoded)
}

func TestEncodeAllFields(t *testing.T) {
	// Verify each field survives independently.
	tests := []struct {
		name string
		node chainstore.Node
	}{
		{
			name: "non-zero LastAccessUnix",
			node: chainstore.Node{LastAccessUnix: -1},
		},
		{
			name: "non-zero CreatedAt",
			node: chainstore.Node{CreatedAt: 9999999999},
		},
		{
			name: "max BucketID",
			node: chainstore.Node{BucketID: ^chainstore.BucketID(0)},
		},
		{
			name: "max RequestBlobSize",
			node: chainstore.Node{RequestBlobSize: ^uint32(0)},
		},
		{
			name: "max ResponseBlobSize",
			node: chainstore.Node{ResponseBlobSize: ^uint32(0)},
		},
		{
			name: "max Depth",
			node: chainstore.Node{Depth: ^uint32(0)},
		},
		{
			name: "status failed",
			node: chainstore.Node{Status: chainstore.NodeStatusFailed},
		},
		{
			name: "blob type chunked",
			node: chainstore.Node{BlobType: chainstore.BlobTypeChunked},
		},
		{
			name: "version 1",
			node: chainstore.Node{Version: 1},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			decoded := decodeNode(encodeNode(tc.node))
			assert.Equal(t, tc.node, decoded)
		})
	}
}
