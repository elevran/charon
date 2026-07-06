package chainstore_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/chainstore"
)

// chunkData builds a synthetic "output item" payload of the given index.
// The proxy will marshal real output items as JSON; tests use this simpler
// binary form so we can deterministically verify per-chunk boundaries.
func chunkData(idx int, size int) []byte {
	out := make([]byte, size)
	header := fmt.Sprintf("chunk-%05d-", idx)
	copy(out, header)
	for i := len(header); i < size; i++ {
		out[i] = byte('A' + (i % 26))
	}
	return out
}

// TestStreamStore_SmallSingleChunk verifies the small-stream happy path:
// one batch (one chunk) → manifest → Retrieve returns identical bytes.
func TestStreamStore_SmallSingleChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	one := chunkData(0, 4096)
	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, one))
	require.NoError(t, s.StreamStore(ctx, stagingID, "r_small", "", "", 1, uint32(len(one))))

	node, turn, err := s.Retrieve(ctx, "r_small", "")
	require.NoError(t, err)
	assert.Equal(t, chainstore.BlobTypeChunked, node.BlobType, "node must be marked Chunked")
	assert.Equal(t, uint32(len(one)), node.ResponseBlobSize)
	assert.Equal(t, one, turn.ResponseBlob)
}

// TestStreamStore_LargeMultiChunk partitions a 10MB payload into 2MB chunks
// and verifies that Retrieve reassembles them in offset order.
func TestStreamStore_LargeMultiChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	const chunkSize = 2 * 1024 * 1024 // 2 MB
	const numChunks = 5               // 10 MB total
	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	expected := make([]byte, 0, chunkSize*numChunks)
	for i := 0; i < numChunks; i++ {
		c := chunkData(i, chunkSize)
		expected = append(expected, c...)
		require.NoError(t, s.AppendChunk(ctx, stagingID, uint32(i), c))
	}
	require.NoError(t, s.StreamStore(ctx, stagingID, "r_big", "", "",
		uint32(numChunks), uint32(len(expected))))

	node, turn, err := s.Retrieve(ctx, "r_big", "")
	require.NoError(t, err)
	assert.Equal(t, uint32(len(expected)), node.ResponseBlobSize)
	assert.Equal(t, expected, turn.ResponseBlob, "chunks must reassemble in offset order")
}

// TestStreamStore_PartialLastChunk uses a non-uniform final segment (e.g. 1.5KB
// instead of 2MB) to verify the read path does not assume fixed-size chunks.
func TestStreamStore_PartialLastChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	c0 := chunkData(0, 2*1024*1024)
	c1 := chunkData(1, 1536) // intentionally small last segment
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, c0))
	require.NoError(t, s.AppendChunk(ctx, stagingID, 1, c1))

	total := uint32(len(c0) + len(c1))
	require.NoError(t, s.StreamStore(ctx, stagingID, "r_partial", "", "", 2, total))

	node, turn, err := s.Retrieve(ctx, "r_partial", "")
	require.NoError(t, err)
	assert.Equal(t, total, node.ResponseBlobSize)
	assert.Equal(t, append(c0, c1...), turn.ResponseBlob)
}

// TestStreamStore_CrashBeforeManifest simulates a proxy crash by writing chunks
// but never committing; the staging TTL reaper must then clean them up.
func TestStreamStore_CrashBeforeManifest(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	const stagingTTL = time.Hour
	s, b := openMemStoreAndBackend(t, chainstore.Config{Clock: clk, StagingTTL: stagingTTL})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)
	sid := parseStagingID(t, stagingID)

	// Write chunks but never commit the manifest.
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, chunkData(0, 1024)))
	require.NoError(t, s.AppendChunk(ctx, stagingID, 1, chunkData(1, 2048)))

	staged, err := b.GetStagingNode(ctx, sid)
	require.NoError(t, err)
	orphanNS := staged.ResponseBlobID

	// Before TTL elapses, orphans must remain.
	clk.Advance(stagingTTL - time.Second)
	s.ReapStaging(ctx)
	chunks, err := b.ListChunks(ctx, orphanNS)
	require.NoError(t, err)
	assert.Len(t, chunks, 2, "chunks must survive before TTL")

	// After TTL, orphans must be reaped.
	clk.Advance(2 * time.Second)
	s.ReapStaging(ctx)
	chunks, err = b.ListChunks(ctx, orphanNS)
	require.NoError(t, err)
	assert.Empty(t, chunks, "chunks must be reaped after TTL")
	_, err = b.GetStagingNode(ctx, sid)
	assert.True(t, errors.Is(err, chainstore.ErrUnknownStaging), "staging record must be gone")
}

// TestStreamStore_MixedSingleAndChunked verifies that a chain containing both
// single-blob and chunked nodes resolves correctly via Retrieve/Resolve.
func TestStreamStore_MixedSingleAndChunked(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	// Turn 0: single-blob (Store + Complete).
	require.NoError(t, s.Store(ctx, "t0", "", "", []byte("req0")))
	require.NoError(t, s.Complete(ctx, "t0", "", []byte("resp0")))

	// Turn 1: chunked.
	stagingID, _, err := s.ResolveAndStage(ctx, "t0", "", []byte("req1"))
	require.NoError(t, err)
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, []byte("chunk1-a")))
	require.NoError(t, s.AppendChunk(ctx, stagingID, 1, []byte("chunk1-b")))
	const sz = uint32(len("chunk1-a") + len("chunk1-b"))
	require.NoError(t, s.StreamStore(ctx, stagingID, "t1", "t0", "", 2, sz))

	// Turn 2: single-blob again.
	require.NoError(t, s.Store(ctx, "t2", "t1", "", []byte("req2")))
	require.NoError(t, s.Complete(ctx, "t2", "", []byte("resp2")))

	turns, err := s.Resolve(ctx, "t2", "")
	require.NoError(t, err)
	require.Len(t, turns, 3)
	assert.Equal(t, []byte("resp0"), turns[0].ResponseBlob, "single-blob turn 0")
	assert.Equal(t, append([]byte("chunk1-a"), []byte("chunk1-b")...), turns[1].ResponseBlob, "chunked turn 1")
	assert.Equal(t, []byte("resp2"), turns[2].ResponseBlob, "single-blob turn 2")
}

// TestStreamStore_DeleteChunkedNode verifies that deleting a chunked node
// removes its chunks (and manifest) — no stranded blobs left behind.
func TestStreamStore_DeleteChunkedNode(t *testing.T) {
	s, b := openMemStoreAndBackend(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, chunkData(0, 4096)))
	require.NoError(t, s.AppendChunk(ctx, stagingID, 1, chunkData(1, 4096)))
	require.NoError(t, s.StreamStore(ctx, stagingID, "r_del", "", "", 2, 8192))

	node, err := b.GetNode(ctx, chainstore.NodeIDFor("", "r_del"))
	require.NoError(t, err)
	ns := node.ResponseBlobID

	// Pre-delete sanity: chunks + manifest exist.
	_, err = b.GetManifest(ctx, ns)
	require.NoError(t, err)
	chunks, err := b.ListChunks(ctx, ns)
	require.NoError(t, err)
	assert.Len(t, chunks, 2)

	require.NoError(t, s.Delete(ctx, "r_del", "", false))

	// Post-delete: chunks and manifest gone.
	_, err = b.GetManifest(ctx, ns)
	assert.True(t, errors.Is(err, chainstore.ErrNotFound), "manifest must be deleted")
	chunks, err = b.ListChunks(ctx, ns)
	require.NoError(t, err)
	assert.Empty(t, chunks, "chunks must be deleted")
}

// TestStreamStore_IdempotentReplay writes the same (offset, data) twice and
// verifies Pebble last-write-wins — the read-back data equals the second write.
func TestStreamStore_IdempotentReplay(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	first := []byte("FIRST-chunk-bytes")
	second := []byte("SECOND-chunk-bytes")
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, first))
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, second), "replay must succeed (idempotent at same offset)")

	total := uint32(len(second))
	require.NoError(t, s.StreamStore(ctx, stagingID, "r_replay", "", "", 1, total))

	_, turn, err := s.Retrieve(ctx, "r_replay", "")
	require.NoError(t, err)
	assert.Equal(t, second, turn.ResponseBlob, "second write wins")
}

// TestStreamStore_PeakHeapBenchmark streams a 50MB blob in 2MB chunks and
// asserts that the steady-state heap usage (after GC) is bounded.
// Plan target: peak RAM ≈ one chunk buffer (2MB), NOT one response blob (50MB).
// We measure before-and-after committed heap delta; the test fails if the
// steady-state heap grows without bound across chunks.
func TestStreamStore_PeakHeapBenchmark(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping peak-heap benchmark under -short")
	}
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	const chunkSize = 2 * 1024 * 1024
	const totalSize = 50 * 1024 * 1024
	const numChunks = totalSize / chunkSize

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	// Establish a baseline after warmup + GC.
	warmup := chunkData(0, chunkSize)
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, warmup))
	runtime.GC()
	var baseline runtime.MemStats
	runtime.ReadMemStats(&baseline)
	before := int64(baseline.HeapAlloc)

	// Stream the remaining chunks; if steady-state growth were O(N*chunk),
	// the heap delta would balloon linearly. With the streaming design,
	// each chunk becomes GC-eligible immediately after the Pebble WAL
	// copy is done, so the per-chunk resident heap is O(chunkSize).
	for i := 1; i < numChunks; i++ {
		c := chunkData(i, chunkSize)
		require.NoError(t, s.AppendChunk(ctx, stagingID, uint32(i), c))
	}

	require.NoError(t, s.StreamStore(ctx, stagingID, "r_bench", "", "", uint32(numChunks), uint32(totalSize)))

	runtime.GC()
	var final runtime.MemStats
	runtime.ReadMemStats(&final)
	delta := int64(final.HeapAlloc) - before

	// Plan target: peak RAM much less than the response blob (50MB).
	// Assert a generous 40MB ceiling that allows for Pebble's memtable +
	// WAL + Go-runtime overhead — the point is regression detection, not
	// tight bounds. A full 50MB response buffered in proxy memory would
	// push this above the limit easily; the streaming path lets the Go
	// runtime reclaim each chunk after Pebble's WAL copy is done.
	t.Logf("steady-state heap delta = %d bytes (response blob = %d bytes)", delta, totalSize)
	assert.Less(t, delta, int64(40*1024*1024),
		"steady-state heap growth must stay below 40MB (got %d bytes)", delta)
}

// TestStreamStore_CommitUnknownStagingReturnsErrUnknownStaging: committing a
// bogus stagingID must propagate ErrUnknownStaging so callers can distinguish
// retry-able conditions from real errors.
func TestStreamStore_CommitUnknownStagingReturnsErrUnknownStaging(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	bogus := uuid.New().String()
	err := s.StreamStore(ctx, bogus, "r1", "", "", 1, 4)
	assert.True(t, errors.Is(err, chainstore.ErrUnknownStaging))
}

// TestStreamStoreCommit_SingleRequest verifies the combined
// write-chunk-and-commit path: StreamStoreCommit atomically writes the chunk,
// manifest, final Node, and deletes the staging record in one Pebble batch.
func TestStreamStoreCommit_SingleRequest(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	c0 := chunkData(0, 2048)
	c1 := chunkData(1, 4096)
	c2 := chunkData(2, 1536)
	expected := append(append(c0, c1...), c2...)

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	// Two append-only chunks, then a single combined commit.
	require.NoError(t, s.AppendChunk(ctx, stagingID, 0, c0))
	require.NoError(t, s.AppendChunk(ctx, stagingID, 1, c1))
	require.NoError(t, s.StreamStoreCommit(ctx, stagingID, "r_combined", "", "", 2, c2, uint32(len(expected))))

	node, turn, err := s.Retrieve(ctx, "r_combined", "")
	require.NoError(t, err)
	assert.Equal(t, chainstore.BlobTypeChunked, node.BlobType)
	assert.Equal(t, expected, turn.ResponseBlob, "chunks must reassemble in offset order")
}

// TestStreamStoreCommit_RejectsEmptyFinalChunk: an empty final chunk is a
// programming error — commit must require at least one byte of payload.
func TestStreamStoreCommit_RejectsEmptyFinalChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	err = s.StreamStoreCommit(ctx, stagingID, "r_empty", "", "", 0, nil, 0)
	assert.Error(t, err)
}

// TestStreamStoreCommit_UnknownStaging: bogus stagingID returns ErrUnknownStaging.
func TestStreamStoreCommit_UnknownStaging(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	bogus := uuid.New().String()
	err := s.StreamStoreCommit(ctx, bogus, "r_bogus", "", "", 0, []byte("x"), 1)
	assert.True(t, errors.Is(err, chainstore.ErrUnknownStaging))
}
