package pebble

import (
	"context"
	"encoding/hex"
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

// TestConsistencyCheck_MissingParentAfterEviction verifies that a child whose
// parent was capacity-evicted (parent inserted, then deleted non-cascading)
// is reported under MissingParents and does NOT make OK=false. This is the
// expected steady-state for any store that has hit capacity.
func TestConsistencyCheck_MissingParentAfterEviction(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

	parent := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x01}, BucketID: 1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 0,
	}
	child := chainstore.Node{
		Version: 1, ID: chainstore.NodeID{0x02},
		ParentID:       parent.ID,
		BucketID:       1,
		LastAccessUnix: 100, CreatedAt: 100, Depth: 1,
	}
	// Insert parent + child normally.
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		PutNodes: []chainstore.Node{parent, child},
	}))
	// Then capacity-evict the parent (non-cascading delete via DeleteNodes,
	// matching appendNodeToDeleteTx in internal/chainstore/evict.go: also
	// clear the LRU entry via BucketMove with NewBucket=0).
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{
		DeleteNodes: []chainstore.NodeID{parent.ID},
		BucketMoves: []chainstore.BucketMove{
			{NodeID: parent.ID, OldBucket: parent.BucketID, NewBucket: 0},
		},
	}))

	report, err := b.ConsistencyCheck(ctx)
	require.NoError(t, err)
	assert.True(t, report.OK, "capacity-evicted parent must not fail the check; got: %+v", report)
	assert.Empty(t, report.DepthErrors)
	assert.Empty(t, report.DanglingLRU)
	assert.Empty(t, report.DecodeErrors)
	require.Len(t, report.MissingParents, 1)
	assert.Contains(t, string(report.MissingParents[0]), hex.EncodeToString(parent.ID[:]))
}

// TestConsistencyCheck_OrphanNodeWithMissingParent verifies the same
// MissingParents reporting for the simplest case: a node that references a
// parent that was never inserted. Same expected outcome — reported, not failed.
func TestConsistencyCheck_OrphanNodeWithMissingParent(t *testing.T) {
	ctx := context.Background()
	b := openMemBackend(t)

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
	assert.True(t, report.OK)
	assert.Empty(t, report.DepthErrors)
	require.Len(t, report.MissingParents, 1)
}
