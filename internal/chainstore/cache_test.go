package chainstore_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
)

// seedChain stores `depth` turns of the given blob sizes and returns the leaf
// responseID. reqSize=0 fills with per-turn-unique bytes (so chain walks are
// observable); reqSize>0 fills with the byte 'r' repeated (deterministic size).
func seedChain(t *testing.T, s *chainstore.Store, depth, reqSize, respSize int) string {
	t.Helper()
	ctx := context.Background()
	var reqBlob, respBlob []byte
	if reqSize > 0 {
		reqBlob = make([]byte, reqSize)
		for i := range reqBlob {
			reqBlob[i] = 'r'
		}
	} else {
		reqBlob = []byte("req-" + uniqueSuffix())
	}
	if respSize > 0 {
		respBlob = make([]byte, respSize)
		for i := range respBlob {
			respBlob[i] = 'p'
		}
	} else {
		respBlob = []byte("resp-" + uniqueSuffix())
	}
	var leaf string
	for i := 0; i < depth; i++ {
		id := "c_" + uniqueSuffix()
		if i == 0 {
			require.NoError(t, s.Store(ctx, id, "", "", reqBlob))
		} else {
			stagingID, _, err := s.ResolveAndStage(ctx, leaf, "", reqBlob)
			require.NoError(t, err)
			require.NoError(t, s.StoreWithStaging(ctx, stagingID, id, leaf, "", respBlob))
		}
		leaf = id
	}
	return leaf
}

var suffixCounter atomic.Uint64

func uniqueSuffix() string {
	return fmt.Sprintf("%d", suffixCounter.Add(1))
}

// TestCache_HitOnRepeat: a second Resolve on the same leaf exercises the cache
// and returns identical turns without re-reading from Pebble.
func TestCache_HitOnRepeat(t *testing.T) {
	s := openMemStore(t, chainstore.Config{
		CacheMaxBytes: 1 * 1024 * 1024,
		CacheTTL:      time.Hour,
	})
	leaf := seedChain(t, s, 5, 0, 0)
	ctx := context.Background()

	first, err := s.Resolve(ctx, leaf, "")
	require.NoError(t, err)
	require.Len(t, first, 5)

	// Second call should observe the cache. We can't observe a Pebble read
	// directly, but we can verify the data is identical and the hit counter
	// advances via stats().
	second, err := s.Resolve(ctx, leaf, "")
	require.NoError(t, err)
	require.Equal(t, len(first), len(second))
	for i := range first {
		assert.Equal(t, first[i].ResponseID, second[i].ResponseID)
		assert.Equal(t, first[i].RequestBlob, second[i].RequestBlob)
	}

	hits, misses, _, _, _ := s.CacheStats()
	assert.GreaterOrEqual(t, hits, int64(1), "second Resolve must be a cache hit")
	assert.GreaterOrEqual(t, misses, int64(1), "first Resolve must be a cache miss")
}

// TestCache_MissAfterDelete and TestCache_InvalidateOnEviction were folded
// into TestCache_DeleteInvalidatesCorrectEntry (below) — they asserted the
// same behaviour (delete a chain node → cached entry for that chain misses;
// sibling chain's cache entry survives) with no extra coverage.

// TestCache_BoundEnforced: inserting more chains than the byte budget allows
// causes the cache to evict LRU entries; total bytes stay within (or near)
// the bound.
func TestCache_BoundEnforced(t *testing.T) {
	// Each turn carries a 1 KiB request blob + 1 KiB response blob.
	// A chain of depth 3 → 6 KiB of blob bytes per cached entry.
	// Budget 12 KiB → expect ~2 entries retained.
	const (
		depth     = 3
		blobSize  = 1024
		budgetKiB = 12
	)
	s := openMemStore(t, chainstore.Config{
		CacheMaxBytes: int64(budgetKiB * 1024),
		CacheTTL:      time.Hour,
	})
	ctx := context.Background()

	leaves := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		leaves = append(leaves, seedChain(t, s, depth, blobSize, blobSize))
	}

	// Resolve each leaf once to populate the cache.
	for _, leaf := range leaves {
		_, err := s.Resolve(ctx, leaf, "")
		require.NoError(t, err)
	}

	// Cache should be at-or-below the bound (with possible overshoot for the
	// final insert that pushed it over — the bound is enforced by LRU eviction
	// tail, so the latest entries are kept and the oldest are dropped). The
	// eviction counter must advance — that's the enforcement code path itself.
	_, _, evictions, usedBytes, entries := s.CacheStats()
	assert.LessOrEqual(t, entries, 3, "cache should not retain more entries than the budget allows")
	assert.LessOrEqual(t, usedBytes, int64(budgetKiB*1024),
		"cache bytes used should be at or below budget (one entry may exceed on insert)")
	assert.Greater(t, evictions, int64(0),
		"evictions must fire when 8x6KiB entries overflow a 12KiB budget")
}

// TestCache_TTLExpiry: advancing the clock past the TTL causes a subsequent
// Resolve to miss the cache and re-read from Pebble.
func TestCache_TTLExpiry(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_700_000_000, 0)}
	s := openMemStore(t, chainstore.Config{
		Clock:         clk,
		CacheMaxBytes: 1 * 1024 * 1024,
		CacheTTL:      time.Minute,
	})
	leaf := seedChain(t, s, 3, 0, 0)
	ctx := context.Background()

	// Prime at t0.
	_, err := s.Resolve(ctx, leaf, "")
	require.NoError(t, err)

	// Second resolve within TTL — must be a hit.
	_, err = s.Resolve(ctx, leaf, "")
	require.NoError(t, err)
	hitsBefore, missesBefore, _, _, _ := s.CacheStats()
	require.Greater(t, hitsBefore, int64(0))
	require.Greater(t, missesBefore, int64(0))

	// Advance past TTL.
	clk.Advance(2 * time.Minute)

	// Third resolve — TTL expired, must miss.
	_, err = s.Resolve(ctx, leaf, "")
	require.NoError(t, err)
	hitsAfter, missesAfter, _, _, _ := s.CacheStats()
	assert.Greater(t, missesAfter, missesBefore, "miss count must advance after TTL expiry")
	assert.Equal(t, hitsBefore, hitsAfter, "hits should not advance after TTL expiry")
}

// TestCache_DisabledByDefault: a zero-value Config disables the cache.
// CacheStats returns zeros and repeated resolves still miss.
func TestCache_DisabledByDefault(t *testing.T) {
	s := openMemStore(t, chainstore.Config{}) // no CacheMaxBytes
	leaf := seedChain(t, s, 2, 0, 0)
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		_, err := s.Resolve(ctx, leaf, "")
		require.NoError(t, err)
	}
	hits, misses, _, _, _ := s.CacheStats()
	assert.Equal(t, int64(0), hits, "no hits when cache is disabled")
	assert.Equal(t, int64(0), misses, "no miss accounting when cache is disabled")
}

// TestCache_ConfigGuardRejectsBadTTL: New must reject a Config where
// CacheTTL >= TTL, since a cache hit would skip the LRU touch/BucketMove commit
// and the background ttlReap would delete the underlying Pebble nodes while the
// cache still serves them. Equal TTLs are also rejected (a tie gives the reaper
// no grace window).
func TestCache_ConfigGuardRejectsBadTTL(t *testing.T) {
	opts := &crdbpebble.Options{FS: vfs.NewMem()}
	b, err := chainstorepebble.OpenBackend("", opts)
	require.NoError(t, err)

	_, err = chainstore.New(context.Background(), chainstore.Config{
		Backend:       b,
		TTL:           time.Hour,
		CacheMaxBytes: 1024,
		CacheTTL:      time.Hour,
	})
	require.Error(t, err, "New must reject CacheTTL == TTL")
	assert.Contains(t, err.Error(), "CacheTTL")

	_, err = chainstore.New(context.Background(), chainstore.Config{
		Backend:       b,
		TTL:           time.Minute,
		CacheMaxBytes: 1024,
		CacheTTL:      2 * time.Minute,
	})
	require.Error(t, err, "New must reject CacheTTL > TTL")
}

// TestCache_ConfigGuardAcceptsCacheTTLBelowTTL: the inverse — a CacheTTL that
// is strictly less than TTL must be accepted.
func TestCache_ConfigGuardAcceptsCacheTTLBelowTTL(t *testing.T) {
	s := openMemStore(t, chainstore.Config{
		TTL:           time.Hour,
		CacheMaxBytes: 1024,
		CacheTTL:      time.Minute,
	})
	require.NotNil(t, s)
}

// TestCache_ConcurrentResolve exercises the cache under concurrent load.
func TestCache_ConcurrentResolve(t *testing.T) {
	s := openMemStore(t, chainstore.Config{
		CacheMaxBytes: 1 * 1024 * 1024,
		CacheTTL:      time.Hour,
	})
	leaf := seedChain(t, s, 10, 0, 0)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, err := s.Resolve(ctx, leaf, "")
				assert.NoError(t, err)
			}
		}()
	}
	wg.Wait()

	hits, _, _, _, _ := s.CacheStats()
	assert.Greater(t, hits, int64(0), "concurrent resolves should hit the cache")
}

// TestCache_DeleteInvalidatesCorrectEntry verifies that Delete only invalidates
// entries that reference the deleted node, leaving other entries intact.
func TestCache_DeleteInvalidatesCorrectEntry(t *testing.T) {
	s := openMemStore(t, chainstore.Config{
		CacheMaxBytes: 1 * 1024 * 1024,
		CacheTTL:      time.Hour,
	})
	ctx := context.Background()

	// Build two chains: leafA and leafB, sharing no nodes.
	leafA := seedChain(t, s, 3, 0, 0)
	leafB := seedChain(t, s, 3, 0, 0)

	// Prime the cache for both.
	_, err := s.Resolve(ctx, leafA, "")
	require.NoError(t, err)
	_, err = s.Resolve(ctx, leafB, "")
	require.NoError(t, err)

	// Confirm both are cached (next resolve hits).
	_, err = s.Resolve(ctx, leafA, "")
	require.NoError(t, err)
	_, err = s.Resolve(ctx, leafB, "")
	require.NoError(t, err)
	hitsA, _, _, _, _ := s.CacheStats()
	require.Greater(t, hitsA, int64(0))

	// Delete a node in leafA's chain. The root is the third-from-end (turns[0]).
	turnsA, err := s.Resolve(ctx, leafA, "")
	require.NoError(t, err)
	require.NoError(t, s.Delete(ctx, turnsA[0].ResponseID, "", true))

	// The next resolve of leafA must miss (its chain is now broken at the root).
	_, missesBefore, _, _, _ := s.CacheStats()
	_, err = s.Resolve(ctx, leafA, "")
	assert.True(t, errors.Is(err, chainstore.ErrChainExpired) || errors.Is(err, chainstore.ErrNotFound),
		"leafA must surface the broken chain (got %v)", err)
	_, missesAfter, _, _, _ := s.CacheStats()
	assert.Greater(t, missesAfter, missesBefore, "leafA resolve must be a cache miss")

	// The next resolve of leafB must still hit the cache.
	hitsB1, _, _, _, _ := s.CacheStats()
	_, err = s.Resolve(ctx, leafB, "")
	require.NoError(t, err)
	hitsB2, _, _, _, _ := s.CacheStats()
	assert.Greater(t, hitsB2, hitsB1, "leafB must still hit the cache after leafA's invalidation")
}
