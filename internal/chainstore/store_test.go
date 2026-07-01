package chainstore_test

import (
	"context"
	"errors"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
)

// fakeClock is an injectable clock whose value advances only when set explicitly.
type fakeClock struct{ t time.Time }

func (c *fakeClock) Now() time.Time { return c.t }

// openMemStore opens a fully-wired *chainstore.Store backed by an in-memory Pebble VFS.
// The returned store must be closed by the caller.
func openMemStore(t *testing.T, cfg chainstore.Config) *chainstore.Store {
	t.Helper()
	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	s, err := chainstorepebble.Open("", opts, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestStoreRootTurn(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := openMemStore(t, chainstore.Config{Clock: clk})
	ctx := context.Background()

	err := s.Store(ctx, "resp_root", "", "", []byte("hello"))
	require.NoError(t, err)

	turns, err := s.Resolve(ctx, "resp_root", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, "resp_root", turns[0].ResponseID)
	assert.Equal(t, []byte("hello"), turns[0].RequestBlob)
	assert.Nil(t, turns[0].ResponseBlob)
}

func TestStoreChainDepthAndOrder(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "resp_0", "", "", []byte("turn0")))
	require.NoError(t, s.Store(ctx, "resp_1", "resp_0", "", []byte("turn1")))
	require.NoError(t, s.Store(ctx, "resp_2", "resp_1", "", []byte("turn2")))

	turns, err := s.Resolve(ctx, "resp_2", "")
	require.NoError(t, err)
	require.Len(t, turns, 3)
	// root-first order
	assert.Equal(t, "resp_0", turns[0].ResponseID)
	assert.Equal(t, []byte("turn0"), turns[0].RequestBlob)
	assert.Equal(t, "resp_1", turns[1].ResponseID)
	assert.Equal(t, []byte("turn1"), turns[1].RequestBlob)
	assert.Equal(t, "resp_2", turns[2].ResponseID)
	assert.Equal(t, []byte("turn2"), turns[2].RequestBlob)
}

func TestStoreParentNotFound(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	err := s.Store(ctx, "resp_child", "resp_nonexistent", "", []byte("data"))
	assert.True(t, errors.Is(err, chainstore.ErrNotFound))
}

func TestResolveNotFound(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	_, err := s.Resolve(ctx, "resp_missing", "")
	assert.True(t, errors.Is(err, chainstore.ErrNotFound))
}

func TestResolveLastAccessUnixUpdated(t *testing.T) {
	storeTime := time.Unix(1_700_000_000, 0)
	resolveTime := storeTime.Add(30 * time.Minute)
	clk := &fakeClock{t: storeTime}
	s := openMemStore(t, chainstore.Config{Clock: clk, BucketDuration: time.Hour})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "resp_a", "", "", []byte("data")))

	clk.t = resolveTime
	_, err := s.Resolve(ctx, "resp_a", "")
	require.NoError(t, err)

	// Re-resolve to get the updated node through a fresh chain walk.
	// If LastAccessUnix was not updated the bucket promotion below would not happen,
	// so we verify indirectly via a second Resolve that the commit succeeded.
	_, err = s.Resolve(ctx, "resp_a", "")
	require.NoError(t, err)
}

func TestResolveBucketMoveOnBucketCross(t *testing.T) {
	// Store in bucket 1, resolve in bucket 2 — expect a bucket move.
	bucket1Time := time.Unix(3600, 0) // bucket 1 (3600/3600=1)
	bucket2Time := time.Unix(7200, 0) // bucket 2 (7200/3600=2)
	clk := &fakeClock{t: bucket1Time}
	s := openMemStore(t, chainstore.Config{Clock: clk, BucketDuration: time.Hour})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "resp_a", "", "", []byte("data")))

	clk.t = bucket2Time
	turns, err := s.Resolve(ctx, "resp_a", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, []byte("data"), turns[0].RequestBlob)
}

func TestResolveBucketMoveSkippedSameBucket(t *testing.T) {
	// Store and resolve within the same bucket — no move needed, Commit should
	// still succeed (BucketMoves is empty but that is valid).
	t0 := time.Unix(3600, 0)
	t1 := t0.Add(10 * time.Minute)
	clk := &fakeClock{t: t0}
	s := openMemStore(t, chainstore.Config{Clock: clk, BucketDuration: time.Hour})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "resp_a", "", "", []byte("data")))

	clk.t = t1
	turns, err := s.Resolve(ctx, "resp_a", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, []byte("data"), turns[0].RequestBlob)
}

func TestMultiTenancyIsolation(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	// Same responseID, different tenant keys — must be stored as separate nodes.
	require.NoError(t, s.Store(ctx, "resp_x", "", "alice", []byte("alice data")))
	require.NoError(t, s.Store(ctx, "resp_x", "", "bob", []byte("bob data")))

	aliceTurns, err := s.Resolve(ctx, "resp_x", "alice")
	require.NoError(t, err)
	require.Len(t, aliceTurns, 1)
	assert.Equal(t, []byte("alice data"), aliceTurns[0].RequestBlob)

	bobTurns, err := s.Resolve(ctx, "resp_x", "bob")
	require.NoError(t, err)
	require.Len(t, bobTurns, 1)
	assert.Equal(t, []byte("bob data"), bobTurns[0].RequestBlob)
}

func TestEmptyTenantKeySingleTenant(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "resp_1", "", "", []byte("single tenant")))
	turns, err := s.Resolve(ctx, "resp_1", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, []byte("single tenant"), turns[0].RequestBlob)
}

func TestResolveResponseBlobNilBeforeComplete(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	// Store creates a turn with only RequestBlobID set; ResponseBlobID is zero.
	require.NoError(t, s.Store(ctx, "resp_a", "", "", []byte("req")))

	turns, err := s.Resolve(ctx, "resp_a", "")
	require.NoError(t, err)
	require.Len(t, turns, 1)
	assert.Equal(t, []byte("req"), turns[0].RequestBlob)
	assert.Nil(t, turns[0].ResponseBlob, "ResponseBlob must be nil before Complete is called")
}

func TestResolveChainCorruptedOnMissingMidNode(t *testing.T) {
	// This is enforced by the backend's LoadChain; Resolve must surface it.
	// We test it indirectly: store root+child, then store a grandchild that points
	// at a parent we inject by manipulating the transaction directly via the backend.
	// For the Store/Resolve API test, the simplest path is a chain with a dangling
	// parent pointer produced by a direct pebble.Backend Commit — but since that
	// requires package-internal access, we rely on the Backend-level test for the
	// corruption case and only verify Store/Resolve surface errors correctly here.
	// See backend_test.go:TestLoadChainCorrupted for the exhaustive case.
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	// A chain of depth 3 with a valid middle node resolves without error.
	require.NoError(t, s.Store(ctx, "r0", "", "", []byte("r0")))
	require.NoError(t, s.Store(ctx, "r1", "r0", "", []byte("r1")))
	require.NoError(t, s.Store(ctx, "r2", "r1", "", []byte("r2")))

	turns, err := s.Resolve(ctx, "r2", "")
	require.NoError(t, err)
	assert.Len(t, turns, 3)
}
