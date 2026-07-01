package chainstore_test

import (
	"context"
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
		require.NoError(t, s.Store(ctx, randomID(t), "", "", []byte("data")))
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
	clk.t = clk.t.Add(ttl + time.Second)

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
	clk.t = hotStoreTime
	for i := 0; i < 100; i++ {
		require.NoError(t, s.Store(ctx, hotID(i), "", "", []byte("hot")))
	}
	// Resolve all hot nodes within the same bucket to update LastAccessUnix without
	// a bucket move.
	clk.t = hotResolveTime
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
	clk.t = currentTime
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

	clk.t = clk.t.Add(ttl + time.Second)
	s.TtlReap(ctx)

	for _, id := range []string{"root", "child1", "child2", "grandchild"} {
		_, err := s.Resolve(ctx, id, "")
		assert.ErrorIs(t, err, chainstore.ErrNotFound, "expected %q to be evicted", id)
	}
}

// TestOptimisticEviction: concurrent Resolve + eviction produces no panics and
// no permanently dangling state. Run with -race.
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
	// No panic = test passes; final count must be non-negative.
	assert.GreaterOrEqual(t, s.Entries(), int64(0))
}

// TestStatsReload: closing and reopening the store preserves entry/byte counts.
func TestStatsReload(t *testing.T) {
	memFS := vfs.NewMem()
	opts := &crdbpebble.Options{FS: memFS}
	ctx := context.Background()

	s1, err := chainstorepebble.Open("", opts, chainstore.Config{})
	require.NoError(t, err)

	require.NoError(t, s1.Store(ctx, "r1", "", "", []byte("hello")))
	require.NoError(t, s1.Store(ctx, "r2", "r1", "", []byte("world")))

	savedEntries := s1.Entries()
	savedBytes := s1.Bytes()
	assert.Equal(t, int64(2), savedEntries)

	require.NoError(t, s1.Close())

	// Reopen using the same in-memory FS.
	s2, err := chainstorepebble.Open("", opts, chainstore.Config{})
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
	clk.t = clk.t.Add(ttl + time.Second)
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
	clk.t = clk.t.Add(ttl + time.Second)
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

// --- helpers ---

func randomID(t *testing.T) string {
	t.Helper()
	return t.Name() + "_" + time.Now().String()
}

func hotID(i int) string {
	return "hot_" + string(rune('a'+i%26)) + string(rune('0'+i/26))
}
