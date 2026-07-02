package chainstore_test

import (
	"context"
	"fmt"
	"testing"

	crdbpebble "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
)

// TestCrashRecovery stores 1000 nodes in a real Pebble directory, simulates a
// crash by closing the backend without going through Store.Close() (so goroutines
// are still "running"), then reopens the directory and verifies all 1000 nodes
// are intact. All commits use pebble.Sync so data survives WAL replay.
func TestCrashRecovery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping crash-recovery test under -short (uses real disk)")
	}

	dir := t.TempDir()
	ctx := context.Background()
	const nodeCount = 1000

	// --- Phase 1: store 1000 nodes, then close backend abruptly ---

	opts := &crdbpebble.Options{}
	b1, err := chainstorepebble.OpenBackend(dir, opts)
	require.NoError(t, err)

	// Wire the backend into a Store so we can call the domain-level Store method.
	// Use a Config with no background goroutines (no TTL, no MaxEntries) so
	// Close() only stops the backend itself and not any ticker goroutines.
	s1, err := chainstore.New(ctx, chainstore.Config{Backend: b1})
	require.NoError(t, err)

	rid := func(i int) string { return fmt.Sprintf("resp_%04d", i) }

	for i := range nodeCount {
		var prevID string
		if i > 0 {
			prevID = rid(i - 1)
		}
		require.NoError(t, s1.Store(ctx, rid(i), prevID, "", []byte(fmt.Sprintf("blob-%d", i))))
	}

	// Simulate a crash: close ONLY the backend without calling Store.Close().
	// Store.Close() would gracefully cancel the internal context and wait for
	// goroutines; here we skip that step to mimic an abrupt process exit.
	// The WAL is already synced (pebble.Sync on every Commit), so no data is lost.
	require.NoError(t, b1.Close())

	// --- Phase 2: reopen, verify all 1000 nodes are readable ---

	opts2 := &crdbpebble.Options{}
	b2, err := chainstorepebble.OpenBackend(dir, opts2)
	require.NoError(t, err)
	s2, err := chainstore.New(ctx, chainstore.Config{Backend: b2})
	require.NoError(t, err)
	t.Cleanup(func() { _ = s2.Close() })

	// Resolve from the leaf: one call returns the full chain, validating every node.
	turns, err := s2.Resolve(ctx, rid(nodeCount-1), "")
	require.NoError(t, err, "leaf must be readable after crash recovery")
	require.Len(t, turns, nodeCount, "chain must be complete after crash recovery")
	for i, turn := range turns {
		assert.Equal(t, rid(i), turn.ResponseID, "turn %d has wrong ResponseID", i)
	}
}
