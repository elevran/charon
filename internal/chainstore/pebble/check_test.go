package pebble

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
)

// TestConsistencyCheck_CleanStore verifies that a fresh store with a single
// root node passes the consistency check with no errors.
func TestConsistencyCheck_CleanStore(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	// One root node.
	node := chainstore.Node{
		Version:        1,
		ID:             chainstore.NodeID{0x01},
		BucketID:       42,
		LastAccessUnix: 100,
		CreatedAt:      100,
		Depth:          0,
	}
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		PutNodes: []chainstore.Node{node},
	}))

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.True(t, report.OK, "clean store must be OK; got: %+v", report)
	assert.Equal(t, 1, report.NodesScanned)
	assert.Equal(t, 1, report.LRUEntriesScanned)
	assert.Empty(t, report.DepthErrors)
	assert.Empty(t, report.DanglingLRU)
	assert.Empty(t, report.DecodeErrors)
}

// TestConsistencyCheck_DepthChain verifies a multi-depth chain passes.
func TestConsistencyCheck_DepthChain(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	parent := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x01}, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 0,
	}
	child := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x02}, ParentID: parent.ID, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 1,
	}
	grandchild := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x03}, ParentID: child.ID, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 2,
	}
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		PutNodes: []chainstore.Node{parent, child, grandchild},
	}))

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.True(t, report.OK, "clean chain must be OK; got: %+v", report)
	assert.Equal(t, 3, report.NodesScanned)
	assert.Equal(t, 3, report.LRUEntriesScanned)
}

// TestConsistencyCheck_DepthMismatch injects a node whose Depth is inconsistent
// with its parent and verifies the checker reports it.
func TestConsistencyCheck_DepthMismatch(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	parent := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x01}, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 0,
	}
	// child claims depth=5 but parent has depth=0 → should be 1.
	child := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x02}, ParentID: parent.ID, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 5,
	}
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		PutNodes: []chainstore.Node{parent, child},
	}))

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.False(t, report.OK)
	require.Len(t, report.DepthErrors, 1)
	assert.Equal(t, uint32(5), report.DepthErrors[0].Child)
	assert.Equal(t, uint32(0), report.DepthErrors[0].Parent)
}

// TestConsistencyCheck_DanglingLRU injects an LRU entry pointing to a missing
// node and verifies the checker reports it.
func TestConsistencyCheck_DanglingLRU(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	// One real root node, one dangling LRU entry.
	node := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x01}, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 0,
	}
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		PutNodes: []chainstore.Node{node},
		// BucketMove with NewBucket=1 writes an LRU entry for {0xAA}; no pfxMeta exists.
		BucketMoves: []chainstore.BucketMove{{NodeID: chainstore.NodeID{0xAA}, NewBucket: 1}},
	}))

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.False(t, report.OK)
	assert.Equal(t, 1, report.NodesScanned)
	assert.Equal(t, 2, report.LRUEntriesScanned, "real node LRU + dangling LRU")
	require.Len(t, report.DanglingLRU, 1)
	assert.Contains(t, string(report.DanglingLRU[0]), "aa")
}

// TestConsistencyCheck_EmptyStore verifies an empty store reports zero nodes.
func TestConsistencyCheck_EmptyStore(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.True(t, report.OK)
	assert.Equal(t, 0, report.NodesScanned)
	assert.Equal(t, 0, report.LRUEntriesScanned)
}

// TestConsistencyCheck_MissingParent reports a child whose parent was deleted
// (capacity-evicted) — surfaces as a depth error against parent=0 since the
// parent is absent from pfxMeta.
func TestConsistencyCheck_MissingParent(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	// Child references a parent that doesn't exist in pfxMeta.
	orphan := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x02},
		ParentID:       chainstore.NodeID{0xDE, 0xAD}, // never inserted
		BucketID:       1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 1,
	}
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		PutNodes: []chainstore.Node{orphan},
	}))

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.False(t, report.OK)
	require.Len(t, report.DepthErrors, 1)
	assert.Equal(t, uint32(0), report.DepthErrors[0].Parent, "absent parent reported as depth=0")
}
