package pebble

import (
	"context"
	"testing"
	"time"

	gogopebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/elevran/charon/pkg/chainstore"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// openMemBackend returns a Backend backed by an in-memory Pebble VFS.
// The returned backend must be closed by the caller.
func openMemBackend(t *testing.T) *Backend {
	t.Helper()
	db, err := gogopebble.Open("", &gogopebble.Options{
		FS:     vfs.NewMem(),
		Merger: StatsMerger,
	})
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })
	return &Backend{db: db}
}

// TestBucketArithmetic verifies that BucketFor maps:
//   - Same hour → same bucket
//   - 1 second across the hour boundary → different buckets
func TestBucketArithmetic(t *testing.T) {
	cfg := chainstore.Config{BucketDuration: time.Hour}

	// Two timestamps within the same hour.
	base := time.Date(2024, 1, 1, 12, 0, 0, 0, time.UTC)
	sameHour := base.Add(30 * time.Minute)
	assert.Equal(t, cfg.BucketFor(base), cfg.BucketFor(sameHour), "same hour must produce same bucket")

	// 1 second before boundary and 1 second after.
	beforeBoundary := time.Date(2024, 1, 1, 12, 59, 59, 0, time.UTC)
	afterBoundary := time.Date(2024, 1, 1, 13, 0, 0, 0, time.UTC)
	assert.NotEqual(t, cfg.BucketFor(beforeBoundary), cfg.BucketFor(afterBoundary),
		"timestamps 1s across hour boundary must be in different buckets")
}

// TestSmokeInsertAndRetrieve opens an in-memory Pebble VFS, inserts one node
// with a blob via Commit, and reads it back via GetNode — verifying the full
// encode → batch → commit → decode path.
func TestSmokeInsertAndRetrieve(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()

	blobID := chainstore.BlobID(uuid.New())
	node := chainstore.Node{
		ID:             chainstore.NodeID{0x01},
		BlobID:         blobID,
		LastAccessUnix: 1700000000,
		CreatedAt:      1699000000,
		BucketID:       chainstore.BucketID(472222),
		BlobSize:       uint32(len("hello chainstore")),
		Depth:          0,
		Version:        1,
	}
	blob := []byte("hello chainstore")

	tx := chainstore.Transaction{
		PutNodes: []chainstore.Node{node},
		PutBlobs: []chainstore.BlobEntry{{BlobID: blobID, Data: blob}},
		StatsDelta: chainstore.StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(len(blob)),
		},
	}
	require.NoError(t, b.Commit(ctx, tx))

	got, err := b.GetNode(ctx, node.ID)
	require.NoError(t, err)
	assert.Equal(t, node, got)

	gotBlob, err := b.GetBlob(ctx, node)
	require.NoError(t, err)
	assert.Equal(t, blob, gotBlob)
}

// TestGetNodeNotFound verifies ErrNotFound is returned for missing nodes.
func TestGetNodeNotFound(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()
	_, err := b.GetNode(ctx, chainstore.NodeID{0xFF})
	assert.ErrorIs(t, err, chainstore.ErrNotFound)
}

// TestLoadChain verifies walking a root→child chain returns nodes leaf-first.
func TestLoadChain(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()

	rootBlobID := chainstore.BlobID(uuid.New())
	childBlobID := chainstore.BlobID(uuid.New())

	root := chainstore.Node{
		ID:      chainstore.NodeID{0x01},
		BlobID:  rootBlobID,
		Version: 1,
	}
	child := chainstore.Node{
		ID:       chainstore.NodeID{0x02},
		ParentID: root.ID,
		BlobID:   childBlobID,
		Depth:    1,
		Version:  1,
	}

	tx := chainstore.Transaction{
		PutNodes: []chainstore.Node{root, child},
		PutBlobs: []chainstore.BlobEntry{
			{BlobID: rootBlobID, Data: []byte("root")},
			{BlobID: childBlobID, Data: []byte("child")},
		},
	}
	require.NoError(t, b.Commit(ctx, tx))

	nodes, err := b.LoadChain(ctx, child.ID)
	require.NoError(t, err)
	require.Len(t, nodes, 2)
	assert.Equal(t, child.ID, nodes[0].ID, "first returned node must be the leaf")
	assert.Equal(t, root.ID, nodes[1].ID, "second returned node must be root")
}

// TestLoadChainNotFound verifies ErrNotFound is returned when the leaf is absent.
func TestLoadChainNotFound(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()
	_, err := b.LoadChain(ctx, chainstore.NodeID{0xDE, 0xAD})
	assert.ErrorIs(t, err, chainstore.ErrNotFound)
}

// TestGetChildren verifies pfxChildren scan.
func TestGetChildren(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()

	parent := chainstore.NodeID{0x01}
	c1 := chainstore.NodeID{0x02}
	c2 := chainstore.NodeID{0x03}

	tx := chainstore.Transaction{
		PutChildren: []chainstore.ChildEntry{
			{Parent: parent, Child: c1},
			{Parent: parent, Child: c2},
		},
	}
	require.NoError(t, b.Commit(ctx, tx))

	children, err := b.GetChildren(ctx, parent)
	require.NoError(t, err)
	assert.ElementsMatch(t, []chainstore.NodeID{c1, c2}, children)
}

// TestOldestBucketEmpty verifies ErrNotFound when LRU index is empty.
func TestOldestBucketEmpty(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()
	_, err := b.OldestBucket(ctx)
	assert.ErrorIs(t, err, chainstore.ErrNotFound)
}

// TestOldestBucketAfterInsert verifies OldestBucket returns the correct bucket.
func TestOldestBucketAfterInsert(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()

	bucket := chainstore.BucketID(100)
	node := chainstore.Node{
		ID:       chainstore.NodeID{0x01},
		BlobID:   chainstore.BlobID(uuid.New()),
		BucketID: bucket,
		Version:  1,
	}
	tx := chainstore.Transaction{
		PutNodes: []chainstore.Node{node},
	}
	require.NoError(t, b.Commit(ctx, tx))

	got, err := b.OldestBucket(ctx)
	require.NoError(t, err)
	assert.Equal(t, bucket, got)
}

// TestStatsAccumulation verifies that multiple Commit calls accumulate stats correctly.
func TestStatsAccumulation(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()

	tx1 := chainstore.Transaction{
		StatsDelta: chainstore.StatsDelta{EntryDelta: 3, BytesDelta: 1024},
	}
	tx2 := chainstore.Transaction{
		StatsDelta: chainstore.StatsDelta{EntryDelta: 2, BytesDelta: 512},
	}
	require.NoError(t, b.Commit(ctx, tx1))
	require.NoError(t, b.Commit(ctx, tx2))

	entries, bytes, err := b.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(5), entries)
	assert.Equal(t, int64(1536), bytes)
}

// TestStatsEmptyDB verifies that Stats returns zeros for an empty database.
func TestStatsEmptyDB(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()
	entries, bytes, err := b.Stats(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(0), entries)
	assert.Equal(t, int64(0), bytes)
}

// TestBucketMoveUpdatesLRUIndex verifies that BucketMoves are reflected in OldestBucket.
func TestBucketMoveUpdatesLRUIndex(t *testing.T) {
	b := openMemBackend(t)
	ctx := context.Background()

	nodeID := chainstore.NodeID{0x01}
	oldBucket := chainstore.BucketID(10)
	newBucket := chainstore.BucketID(20)

	// Insert with oldBucket.
	node := chainstore.Node{
		ID:       nodeID,
		BlobID:   chainstore.BlobID(uuid.New()),
		BucketID: oldBucket,
		Version:  1,
	}
	require.NoError(t, b.Commit(ctx, chainstore.Transaction{PutNodes: []chainstore.Node{node}}))

	oldest, err := b.OldestBucket(ctx)
	require.NoError(t, err)
	assert.Equal(t, oldBucket, oldest)

	// Promote to newBucket.
	node.BucketID = newBucket
	moveTx := chainstore.Transaction{
		PutNodes: []chainstore.Node{node},
		BucketMoves: []chainstore.BucketMove{
			{NodeID: nodeID, OldBucket: oldBucket, NewBucket: newBucket},
		},
	}
	require.NoError(t, b.Commit(ctx, moveTx))

	oldest, err = b.OldestBucket(ctx)
	require.NoError(t, err)
	assert.Equal(t, newBucket, oldest)
}
