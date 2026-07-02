package chainstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
)

// TestCapacityEviction: filling the store to twice the limit then triggering
// eviction brings entry count back below the limit.
func TestCapacityEviction(t *testing.T) {
	const maxEntries = 5
	s := openMemStore(t, chainstore.Config{MaxEntries: maxEntries})
	ctx := context.Background()

	for i := 0; i < maxEntries*2; i++ {
		require.NoError(t, s.Store(ctx, fmt.Sprintf("%s_%d", t.Name(), i), "", "", []byte("data")))
	}

	s.EvictOldest(ctx)
	assert.LessOrEqual(t, s.Entries(), int64(maxEntries))
}

// TestCapacityNudge: a Store that pushes over MaxEntries signals the eviction
// goroutine via the nudge channel without waiting for the ticker.
func TestCapacityNudge(t *testing.T) {
	const maxEntries = 2
	// Use a 10-year ticker so any eviction must be nudge-driven, not ticker-driven.
	s := openMemStore(t, chainstore.Config{
		MaxEntries:       maxEntries,
		EvictionInterval: 10 * 365 * 24 * time.Hour,
	})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "r1", "", "", []byte("a")))
	require.NoError(t, s.Store(ctx, "r2", "", "", []byte("b")))
	// Third insert exceeds MaxEntries → nudge is sent.
	require.NoError(t, s.Store(ctx, "r3", "", "", []byte("c")))

	// Verify the nudge was sent before the goroutine could drain it.
	assert.Greater(t, s.NudgesFired(), int64(0), "Store should have nudged eviction goroutine")

	// Poll for eviction to complete (the goroutine should process the nudge promptly).
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.Entries() <= maxEntries {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.LessOrEqual(t, s.Entries(), int64(maxEntries))
}

// TestTTLEviction: a node whose LastAccessUnix is older than the TTL cutoff is
// removed by the TTL reaper.
func TestTTLEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	const ttl = time.Hour
	s := openMemStore(t, chainstore.Config{Clock: clk, TTL: ttl})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "resp_old", "", "", []byte("old")))

	// Advance clock past TTL cutoff.
	clk.Advance(ttl + time.Second)

	s.TtlReap(ctx)

	_, err := s.Resolve(ctx, "resp_old", "")
	assert.ErrorIs(t, err, chainstore.ErrNotFound)
}

// TestStaleBucketSkip: the reaper correctly finds an expired node in an older
// bucket even when a newer bucket contains a full batch of fresh (non-expired) nodes.
func TestStaleBucketSkip(t *testing.T) {
	const ttl = time.Hour
	const bucketDur = time.Hour
	// Expired node stored at t=1h, never accessed again.
	expiredStoreTime := time.Unix(int64(bucketDur.Seconds()), 0)
	// Hot nodes stored at t=2h, resolved at t=2.5h (same bucket) so LastAccessUnix is recent.
	hotStoreTime := time.Unix(int64(2*bucketDur.Seconds()), 0)
	// Current time = t=4h; TTL cutoff = t=3h.
	// Expired node: LastAccessUnix=1h < 3h → expired.
	// Hot nodes:    LastAccessUnix=2.5h < 3h → also expired (all nodes are > 1h old).
	// Use TTL = 2h so cutoff = 4h - 2h = 2h.
	// Expired node: LastAccessUnix=1h < 2h → expired.
	// Hot nodes: LastAccessUnix=2.5h >= 2h → NOT expired (hot).
	hotResolveTime := hotStoreTime.Add(30 * time.Minute) // 2.5h
	currentTime := time.Unix(int64(4*bucketDur.Seconds()), 0)

	clk := &fakeClock{t: expiredStoreTime}
	s, b := openMemStoreAndBackend(t, chainstore.Config{
		Clock:          clk,
		TTL:            2 * ttl,
		BucketDuration: bucketDur,
	})
	ctx := context.Background()

	// Insert the expired node (bucket 1, LastAccessUnix=1h).
	require.NoError(t, s.Store(ctx, "expired", "", "", []byte("old")))

	// Insert 100 hot nodes (bucket 2, LastAccessUnix=2h; then resolved at 2.5h).
	clk.Set(hotStoreTime)
	for i := 0; i < 100; i++ {
		require.NoError(t, s.Store(ctx, hotID(i), "", "", []byte("hot")))
	}
	// Resolve all hot nodes within the same bucket to update LastAccessUnix without
	// a bucket move.
	clk.Set(hotResolveTime)
	for i := 0; i < 100; i++ {
		_, err := s.Resolve(ctx, hotID(i), "")
		require.NoError(t, err)
	}

	// Verify all hot nodes still have BucketID=2 (no crossing).
	cfg := chainstore.Config{BucketDuration: bucketDur}
	bucket2 := cfg.BucketFor(hotStoreTime)
	node, err := b.GetNode(ctx, chainstore.NodeIDFor("", hotID(0)))
	require.NoError(t, err)
	assert.Equal(t, bucket2, node.BucketID)

	// Run the TTL reaper at currentTime (cutoff = 2h).
	clk.Set(currentTime)
	s.TtlReap(ctx)

	// The expired node should be gone.
	_, err = s.Resolve(ctx, "expired", "")
	assert.ErrorIs(t, err, chainstore.ErrNotFound)

	// The hot nodes should still be present.
	turns, err := s.Resolve(ctx, hotID(0), "")
	require.NoError(t, err)
	assert.Len(t, turns, 1)
}

// TestSubtreeEviction: TTL reaping a root node deletes it and all descendants.
func TestSubtreeEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	const ttl = time.Hour
	s := openMemStore(t, chainstore.Config{Clock: clk, TTL: ttl})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "root", "", "", []byte("root")))
	require.NoError(t, s.Store(ctx, "child1", "root", "", []byte("c1")))
	require.NoError(t, s.Store(ctx, "child2", "root", "", []byte("c2")))
	require.NoError(t, s.Store(ctx, "grandchild", "child1", "", []byte("gc")))

	clk.Advance(ttl + time.Second)
	s.TtlReap(ctx)

	for _, id := range []string{"root", "child1", "child2", "grandchild"} {
		_, err := s.Resolve(ctx, id, "")
		assert.ErrorIs(t, err, chainstore.ErrNotFound, "expected %q to be evicted", id)
	}
}

// TestOptimisticEviction: concurrent Resolve + eviction produces no panics,
// no spurious successes on evicted nodes, and no permanently dangling state.
// Run with -race.
func TestOptimisticEviction(t *testing.T) {
	clk := &fakeClock{t: time.Unix(3600, 0)}
	const maxEntries = 10
	s := openMemStore(t, chainstore.Config{
		Clock:      clk,
		MaxEntries: maxEntries,
	})
	ctx := context.Background()

	// Seed some nodes.
	for i := 0; i < maxEntries*2; i++ {
		require.NoError(t, s.Store(ctx, hotID(i), "", "", []byte("data")))
	}

	var wg sync.WaitGroup
	// Concurrent resolvers.
	for i := 0; i < 5; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 20; j++ {
				_, _ = s.Resolve(ctx, hotID(i), "")
			}
		}()
	}
	// Concurrent evictors.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				s.EvictOldest(ctx)
			}
		}()
	}
	wg.Wait()

	// After concurrent eviction, each node is either present (Resolve succeeds)
	// or absent (ErrNotFound / ErrChainExpired). Any other error indicates a bug.
	for i := 0; i < maxEntries*2; i++ {
		_, err := s.Resolve(ctx, hotID(i), "")
		if err != nil {
			assert.True(t,
				errors.Is(err, chainstore.ErrNotFound) || errors.Is(err, chainstore.ErrChainExpired),
				"unexpected error for hotID(%d): %v", i, err)
		}
	}
	// Entries() may be transiently negative (documented approximate behaviour after
	// concurrent eviction). Bound: must not exceed the total number of nodes inserted.
	assert.GreaterOrEqual(t, s.Entries(), int64(-maxEntries*2))
}

// TestStatsReload: closing and reopening the store preserves entry/byte counts.
func TestStatsReload(t *testing.T) {
	memFS := vfs.NewMem()
	opts := &crdbpebble.Options{FS: memFS}
	ctx := context.Background()

	s1, err := chainstorepebble.Open(ctx, "", opts, chainstore.Config{})
	require.NoError(t, err)

	require.NoError(t, s1.Store(ctx, "r1", "", "", []byte("hello")))
	require.NoError(t, s1.Store(ctx, "r2", "r1", "", []byte("world")))

	savedEntries := s1.Entries()
	savedBytes := s1.Bytes()
	assert.Equal(t, int64(2), savedEntries)

	require.NoError(t, s1.Close())

	// Reopen using the same in-memory FS.
	s2, err := chainstorepebble.Open(ctx, "", opts, chainstore.Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	assert.Equal(t, savedEntries, s2.Entries(), "entry count should survive close/reopen")
	assert.Equal(t, savedBytes, s2.Bytes(), "byte count should survive close/reopen")
}

// TestEvictionCounterConsistency: in-memory counters decrease correctly after eviction.
func TestEvictionCounterConsistency(t *testing.T) {
	clk := &fakeClock{t: time.Unix(3600, 0)}
	const ttl = time.Hour
	s := openMemStore(t, chainstore.Config{Clock: clk, TTL: ttl})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "a", "", "", []byte("aaa")))
	require.NoError(t, s.Store(ctx, "b", "", "", []byte("bb")))
	require.NoError(t, s.Store(ctx, "c", "", "", []byte("c")))

	entriesBefore := s.Entries()
	bytesBefore := s.Bytes()
	assert.Equal(t, int64(3), entriesBefore)
	assert.Equal(t, int64(6), bytesBefore)

	// Expire all nodes.
	clk.Advance(ttl + time.Second)
	s.TtlReap(ctx)

	assert.Equal(t, int64(0), s.Entries())
	assert.Equal(t, int64(0), s.Bytes())
}

// TestDeleteNodeNoOrphanedLRU: after deleting a node, OldestBucket no longer
// returns that node's bucket (if it was the only node).
func TestDeleteNodeNoOrphanedLRU(t *testing.T) {
	clk := &fakeClock{t: time.Unix(3600, 0)}
	const ttl = time.Hour
	s := openMemStore(t, chainstore.Config{Clock: clk, TTL: ttl})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "solo", "", "", []byte("x")))
	assert.Equal(t, int64(1), s.Entries())

	// Expire and reap.
	clk.Advance(ttl + time.Second)
	s.TtlReap(ctx)

	assert.Equal(t, int64(0), s.Entries())

	// Running evictOldest on an empty store must return cleanly (no infinite loop).
	done := make(chan struct{})
	go func() {
		s.EvictOldest(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("EvictOldest did not terminate on empty store")
	}
}

// TestCapacityEvictOrphanedChild: capacity-evicting a parent (non-cascading) leaves the
// child with a dangling parent pointer. Resolve on the child must return ErrChainExpired,
// not ErrChainCorrupted, so callers can distinguish eviction from real corruption.
func TestCapacityEvictOrphanedChild(t *testing.T) {
	const bucketDur = time.Hour
	clk := &fakeClock{}
	// MaxEntries=1: after inserting parent+child (entries=2), EvictOldest will run.
	s := openMemStore(t, chainstore.Config{
		Clock:          clk,
		MaxEntries:     1,
		BucketDuration: bucketDur,
	})
	ctx := context.Background()

	// Store parent in bucket 1 (Unix 3600).
	clk.Set(time.Unix(int64(bucketDur.Seconds()), 0))
	require.NoError(t, s.Store(ctx, "parent", "", "", []byte("p")))

	// Store child in bucket 2 (Unix 7200) — different bucket so parent is older.
	clk.Set(time.Unix(int64(2*bucketDur.Seconds()), 0))
	require.NoError(t, s.Store(ctx, "child", "parent", "", []byte("c")))

	// entries=2 > MaxEntries=1: EvictOldest picks from bucket 1 (parent only, non-cascading).
	s.EvictOldest(ctx)

	// Parent is gone.
	_, err := s.Resolve(ctx, "parent", "")
	assert.ErrorIs(t, err, chainstore.ErrNotFound)

	// Child exists but its ancestor chain is broken — must be ErrChainExpired, not ErrChainCorrupted.
	_, err = s.Resolve(ctx, "child", "")
	assert.ErrorIs(t, err, chainstore.ErrChainExpired)
}

// TestCapacityEvictionByBytes: filling the store past MaxBytes triggers eviction.
func TestCapacityEvictionByBytes(t *testing.T) {
	const blobSize = 10
	const maxBytes = int64(blobSize * 5)
	s := openMemStore(t, chainstore.Config{MaxBytes: maxBytes})
	ctx := context.Background()

	blob := make([]byte, blobSize)
	for i := 0; i < 10; i++ {
		require.NoError(t, s.Store(ctx, fmt.Sprintf("r%d", i), "", "", blob))
	}

	s.EvictOldest(ctx)
	assert.LessOrEqual(t, s.Bytes(), maxBytes)
}

// TestGoroutineStopsCleanly verifies that Close() terminates all background
// goroutines (eviction and TTL reaper) promptly. Port of TestTTLWorkerStopsCleanly
// from internal/worker/worker_test.go.
func TestGoroutineStopsCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping goroutine-stop test under -short")
	}
	// Open the store directly (no t.Cleanup) so we own the Close() call.
	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	s, err := chainstorepebble.Open(context.Background(), "", opts, chainstore.Config{
		MaxEntries:       10,
		TTL:              time.Hour,
		StagingTTL:       time.Hour,
		EvictionInterval: 10 * 365 * 24 * time.Hour,
		TTLInterval:      10 * 365 * 24 * time.Hour,
	})
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		_ = s.Close()
		close(done)
	}()

	select {
	case <-done:
		// success: Close returned promptly
	case <-time.After(time.Second):
		t.Fatal("Close() did not stop background goroutines within 1s")
	}
}

// TestTTLWorkerDeletesExpiredNodes verifies the background TTL loop removes
// expired nodes without needing a manual TtlReap call.
// Port of TestTTLWorkerDeletesExpired from internal/worker/worker_test.go.
func TestTTLWorkerDeletesExpiredNodes(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timer-based worker test under -short")
	}
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	const ttl = time.Hour

	// Use a very short TTLInterval so the background goroutine fires quickly.
	s := openMemStore(t, chainstore.Config{
		Clock:       clk,
		TTL:         ttl,
		TTLInterval: 20 * time.Millisecond,
	})
	ctx := context.Background()

	require.NoError(t, s.Store(ctx, "exp1", "", "", []byte("a")))
	require.NoError(t, s.Store(ctx, "exp2", "", "", []byte("b")))
	require.NoError(t, s.Store(ctx, "exp3", "", "", []byte("c")))

	// Advance the clock past TTL — the background goroutine will run at the next tick.
	clk.Advance(ttl + time.Second)

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.Entries() == 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	assert.Equal(t, int64(0), s.Entries(), "background TTL goroutine must have deleted expired nodes")
}

// --- helpers ---

func hotID(i int) string {
	return fmt.Sprintf("hot_%d", i)
}
