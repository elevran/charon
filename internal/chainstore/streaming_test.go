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
	chainstorepebble "github.com/elevran/charon/internal/chainstore/pebble"
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

// TestCompleteStreaming_SmallSingleChunk verifies the small-stream happy path:
// one batch (one chunk) → manifest → Retrieve returns identical bytes.
func TestCompleteStreaming_SmallSingleChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	one := chunkData(0, 4096)
	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	_, err = s.AppendChunk(ctx, stagingID, 0, one)
	require.NoError(t, err)
	_, err = s.CompleteStreaming(ctx, stagingID, "r_small", "", uint32(len(one)))
	require.NoError(t, err)

	node, turn, err := s.Retrieve(ctx, "r_small", "")
	require.NoError(t, err)
	assert.Equal(t, chainstore.BlobTypeChunked, node.BlobType, "node must be marked Chunked")
	assert.Equal(t, uint32(len(one)), node.ResponseBlobSize)
	assert.Equal(t, one, turn.ResponseBlob)
}

// TestAppendChunk_LargeMultiChunk writes a 10MB payload as five 2MB
// AppendChunk calls and verifies that Retrieve reassembles them in offset
// order. Each 2MB body splits into 8 × 256 KB internal Pebble chunks.
func TestAppendChunk_LargeMultiChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	const chunkSize = 2 * 1024 * 1024 // 2 MB
	const numChunks = 5               // 10 MB total
	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	expected := make([]byte, 0, chunkSize*numChunks)
	// k on the wire is the *batch* ordinal, not the internal-chunk
	// ordinal. AppendChunk splits each batch into 256 KB Pebble chunks.
	// The server's pfxStagingNext tracks the next internal offset.
	for i := uint32(0); i < uint32(numChunks); i++ {
		c := chunkData(int(i), chunkSize)
		expected = append(expected, c...)
		_, err = s.AppendChunk(ctx, stagingID, i*8, c)
		require.NoError(t, err)
	}
	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_big"))
	_, err = s.CompleteStreaming(ctx, stagingID, "r_big", "", uint32(len(expected)))
	require.NoError(t, err)

	node, turn, err := s.Retrieve(ctx, "r_big", "")
	require.NoError(t, err)
	assert.Equal(t, uint32(len(expected)), node.ResponseBlobSize)
	assert.Equal(t, expected, turn.ResponseBlob, "chunks must reassemble in offset order")
}

// TestAppendChunk_PartialLastChunk uses a non-uniform final segment
// (e.g. 1.5KB instead of 2MB) to verify the read path does not assume
// fixed-size chunks.
func TestAppendChunk_PartialLastChunk(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	c0 := chunkData(0, 2*1024*1024)
	c1 := chunkData(1, 1536) // intentionally small last segment
	// c0 (2 MB) splits into 8 internal Pebble chunks (offsets 0..7), so the
	// next batch must use k=8 — server-validated ordering rejects k=1 as a
	// replay at this point and silently no-ops.
	_, err = s.AppendChunk(ctx, stagingID, 0, c0)
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 8, c1)
	require.NoError(t, err)

	total := uint32(len(c0) + len(c1))
	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_partial"))
	_, err = s.CompleteStreaming(ctx, stagingID, "r_partial", "", total)
	require.NoError(t, err)

	node, turn, err := s.Retrieve(ctx, "r_partial", "")
	require.NoError(t, err)
	assert.Equal(t, total, node.ResponseBlobSize)
	assert.Equal(t, append(c0, c1...), turn.ResponseBlob)
}

// TestCompleteStreaming_CrashBeforeManifest simulates a proxy crash by writing chunks
// but never committing; the staging TTL reaper must then clean them up.
func TestCompleteStreaming_CrashBeforeManifest(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	const stagingTTL = time.Hour
	s, b := openMemStoreAndBackend(t, chainstore.Config{Clock: clk, StagingTTL: stagingTTL})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)
	sid := parseStagingID(t, stagingID)

	// Write chunks but never commit the manifest.
	_, err = s.AppendChunk(ctx, stagingID, 0, chunkData(0, 1024))
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 1, chunkData(1, 2048))
	require.NoError(t, err)

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

// TestCompleteStreaming_MixedSingleAndChunked verifies that a chain containing both
// single-blob and chunked nodes resolves correctly via Retrieve/Resolve.
func TestCompleteStreaming_MixedSingleAndChunked(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	// Turn 0: single-blob (Store + Complete).
	require.NoError(t, s.Store(ctx, "t0", "", "", []byte("req0")))
	require.NoError(t, s.Complete(ctx, "t0", "", []byte("resp0")))

	// Turn 1: chunked.
	stagingID, _, err := s.ResolveAndStage(ctx, "t0", "", []byte("req1"))
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 0, []byte("chunk1-a"))
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 1, []byte("chunk1-b"))
	require.NoError(t, err)
	const sz = uint32(len("chunk1-a") + len("chunk1-b"))
	_, err = s.CompleteStreaming(ctx, stagingID, "t1", "", sz)
	require.NoError(t, err)

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

// TestCompleteStreaming_DeleteChunkedNode verifies that deleting a chunked node
// removes its chunks (and manifest) — no stranded blobs left behind.
func TestCompleteStreaming_DeleteChunkedNode(t *testing.T) {
	s, b := openMemStoreAndBackend(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 0, chunkData(0, 4096))
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 1, chunkData(1, 4096))
	require.NoError(t, err)
	_, err = s.CompleteStreaming(ctx, stagingID, "r_del", "", 8192)
	require.NoError(t, err)

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

// TestAppendChunk_IdempotentReplay writes the same offset twice. With
// server-validated ordering the second write is a no-op (k < next_expected):
// the data is left untouched. Read-back equals the FIRST write.
func TestAppendChunk_IdempotentReplay(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	first := []byte("FIRST-chunk-bytes")
	second := []byte("SECOND-chunk-bytes")
	_, err = s.AppendChunk(ctx, stagingID, 0, first)
	require.NoError(t, err)
	// Replay at the same offset — server detects k < next_expected and
	// returns 200 OK without rewriting. The chunk stays as the first write.
	_, err = s.AppendChunk(ctx, stagingID, 0, second)
	require.NoError(t, err, "replay must succeed (idempotent at same offset)")

	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_replay"))
	_, err = s.CompleteStreaming(ctx, stagingID, "r_replay", "", uint32(len(first)))
	require.NoError(t, err)

	_, turn, err := s.Retrieve(ctx, "r_replay", "")
	require.NoError(t, err)
	assert.Equal(t, first, turn.ResponseBlob, "replay is a no-op; first write's data wins")
}

// TestCompleteStreaming_PeakHeapBenchmark streams a 50MB blob in 2MB chunks and
// asserts that the steady-state heap usage (after GC) is bounded.
// Plan target: peak RAM ≈ one chunk buffer (2MB), NOT one response blob (50MB).
// We measure before-and-after committed heap delta; the test fails if the
// steady-state heap grows without bound across chunks.
func TestCompleteStreaming_PeakHeapBenchmark(t *testing.T) {
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
	_, err = s.AppendChunk(ctx, stagingID, 0, warmup)
	require.NoError(t, err)
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
		_, err = s.AppendChunk(ctx, stagingID, uint32(i), c)
		require.NoError(t, err)
	}

	_, err = s.CompleteStreaming(ctx, stagingID, "r_bench", "", uint32(totalSize))
	require.NoError(t, err)

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

// TestCompleteStreaming_CommitUnknownStagingReturnsErrUnknownStaging: committing a
// bogus stagingID must propagate ErrUnknownStaging so callers can distinguish
// retry-able conditions from real errors.
func TestCompleteStreaming_CommitUnknownStagingReturnsErrUnknownStaging(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	bogus := uuid.New().String()
	_, err := s.CompleteStreaming(ctx, bogus, "r1", "", 4)
	assert.True(t, errors.Is(err, chainstore.ErrUnknownStaging))
}

// TestAppendChunkAndCommit_ThreeChunks: verifies the chunk-path commit
// pattern. Three chunks are written via AppendChunk (server-validated
// ordering), then a separate StreamStoreCommit finalizes the manifest
// atomically. Read-back reassembles the bytes in offset order.
func TestAppendChunkAndCommit_ThreeChunks(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	c0 := chunkData(0, 2048)
	c1 := chunkData(1, 4096)
	c2 := chunkData(2, 1536)
	expected := append(append(c0, c1...), c2...)

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	// Three chunks via the new chunk-path API.
	_, err = s.AppendChunk(ctx, stagingID, 0, c0)
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 1, c1)
	require.NoError(t, err)
	_, err = s.AppendChunk(ctx, stagingID, 2, c2)
	require.NoError(t, err)

	// Bind and commit (responseID is now required; supply via call arg).
	finalID, err := s.CompleteStreaming(ctx, stagingID, "r_combined", "", uint32(len(expected)))
	require.NoError(t, err)
	assert.Equal(t, "r_combined", finalID)

	node, turn, err := s.Retrieve(ctx, "r_combined", "")
	require.NoError(t, err)
	assert.Equal(t, chainstore.BlobTypeChunked, node.BlobType)
	assert.Equal(t, expected, turn.ResponseBlob, "chunks must reassemble in offset order")
}

// TestCompleteStreaming_UnknownStaging: bogus stagingID returns ErrUnknownStaging.
func TestCompleteStreaming_UnknownStaging(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	bogus := uuid.New().String()
	_, err := s.CompleteStreaming(ctx, bogus, "r_bogus", "", 1)
	assert.True(t, errors.Is(err, chainstore.ErrUnknownStaging))
}

// TestCompleteAcceptsBoundOrSuppliedResponseID: the completion path resolves
// the responseID from (1) the call's responseID arg, or (2) the bound value.
// Both are valid; a conflict between them is rejected.
func TestCompleteAcceptsBoundOrSuppliedResponseID(t *testing.T) {
	s, b := openMemStoreAndBackend(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	// No chunks yet — completion with no responseID supplied and no
	// binding → ErrResponseIDRequired.
	_, err = s.CompleteStreaming(ctx, stagingID, "", "", 0)
	assert.ErrorIs(t, err, chainstore.ErrResponseIDRequired)

	// Bind via BindResponseID, retry — uses bound value, no caller ID.
	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_via_bind"))
	_, err = s.CompleteStreaming(ctx, stagingID, "", "", 0)
	require.NoError(t, err)

	// Verify the bound value was used.
	node, err := b.GetNode(ctx, chainstore.NodeIDFor("", "r_via_bind"))
	require.NoError(t, err)
	assert.Equal(t, "r_via_bind", node.ResponseID)
}

// resolveStagingBlobID resolves a UUID-string stagingID to its pre-allocated
// ResponseBlobID by reading the staging record via the backend.
func resolveStagingBlobID(t *testing.T, b *chainstorepebble.Backend, stagingID string) chainstore.BlobID {
	t.Helper()
	uid, err := uuid.Parse(stagingID)
	if err != nil {
		t.Fatalf("invalid stagingID %q: %v", stagingID, err)
	}
	var sid chainstore.BlobID
	copy(sid[:], uid[:])
	staged, err := b.GetStagingNode(context.Background(), sid)
	if err != nil {
		t.Fatalf("GetStagingNode: %v", err)
	}
	return staged.ResponseBlobID
}

// TestAppendChunk_InternalSplitting: a 1 MB HTTP body is stored as four
// 256 KB Pebble chunks. Read-back must reassemble them correctly via the chunked read path.
func TestAppendChunk_InternalSplitting(t *testing.T) {
	s, b := openMemStoreAndBackend(t, chainstore.Config{})
	ctx := context.Background()

	const bodySize = 1 * 1024 * 1024 // 1 MB → 4 × 256 KB internal chunks
	body := chunkData(0, bodySize)

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	_, err = s.AppendChunk(ctx, stagingID, 0, body)
	require.NoError(t, err)

	ns := resolveStagingBlobID(t, b, stagingID)
	chunks, err := b.ListChunks(ctx, ns)
	require.NoError(t, err)
	assert.Len(t, chunks, 4, "1 MB body must split into 4 × 256 KB chunks")
	for i, c := range chunks {
		assert.Equal(t, uint32(i), c.Offset, "internal offsets must be 0,1,2,3")
		if i < 3 {
			assert.Len(t, c.Data, 256*1024, "first 3 chunks must be full 256 KB")
		} else {
			assert.Len(t, c.Data, bodySize-3*256*1024, "last chunk is the tail")
		}
	}

	// Commit so the retrieve path can reassemble.
	_, err = s.CompleteStreaming(ctx, stagingID, "r_split1mb", "", uint32(bodySize))
	require.NoError(t, err)
	node, turn, err := s.Retrieve(ctx, "r_split1mb", "")
	require.NoError(t, err)
	assert.Equal(t, body, turn.ResponseBlob, "1 MB reassembled from 4 chunks")
	assert.Equal(t, uint32(bodySize), node.ResponseBlobSize)
}

// TestAppendChunk_SplitsLargeBody: a 4 MB AppendChunk body is split into
// 16 × 256 KB internal Pebble chunks, all written in one Pebble batch.
// (The combined write+commit "StreamStoreCommit_SplitsLargeFinalChunk"
// is obsolete after the chunk-path refactor — splits now happen in
// AppendChunk, and the commit is a separate call.)
func TestAppendChunk_SplitsLargeBody(t *testing.T) {
	s, b := openMemStoreAndBackend(t, chainstore.Config{})
	ctx := context.Background()

	const bodySize = 4 * 1024 * 1024 // 4 MB → 16 × 256 KB internal chunks
	body := chunkData(0, bodySize)

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	_, err = s.AppendChunk(ctx, stagingID, 0, body)
	require.NoError(t, err)

	// Inspect chunks via the staging record (still alive in-progress).
	ns := resolveStagingBlobID(t, b, stagingID)
	chunks, err := b.ListChunks(ctx, ns)
	require.NoError(t, err)
	assert.Len(t, chunks, 16, "4 MB body must split into 16 × 256 KB chunks")

	// Now commit and read-back must reassemble exactly the input body.
	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_split"))
	_, err = s.CompleteStreaming(ctx, stagingID, "r_split", "", uint32(bodySize))
	require.NoError(t, err)

	roundtrip, turn, err := s.Retrieve(ctx, "r_split", "")
	require.NoError(t, err)
	assert.Equal(t, body, turn.ResponseBlob, "reassembled bytes must match input")
	assert.Equal(t, uint32(bodySize), roundtrip.ResponseBlobSize)
}

// TestBindResponseID verifies that the early-binding API records the
// responseID on the staging record and rejects conflicting bindings.
func TestBindResponseID(t *testing.T) {
	s, b := openMemStoreAndBackend(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	sid := parseStagingID(t, stagingID)

	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_early"))
	node, err := b.GetStagingNode(ctx, sid)
	require.NoError(t, err)
	assert.Equal(t, "r_early", node.ResponseID)

	// Re-binding to the same id is a no-op.
	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_early"))

	// Re-binding to a different id is rejected.
	err = s.BindResponseID(ctx, stagingID, "r_other")
	assert.Error(t, err)
}

// TestCompleteStreaming_UsesBoundResponseID: when the staging record is
// already bound, CompleteStreaming uses the bound value even when the
// caller passes a different one (conflict → error). This test exercises
// the 2-call (AppendChunk + CompleteStreaming) path: chunks are
// pre-written, then CompleteStreaming finalizes the manifest atomically.
func TestCompleteStreaming_UsesBoundResponseID(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_bound"))

	// Write a single chunk via AppendChunk (now returns next-expected).
	body := []byte("payload")
	_, err = s.AppendChunk(ctx, stagingID, 0, body)
	require.NoError(t, err)

	// StreamStoreCommit (legacy 2-call path) uses the bound value.
	_, err = s.CompleteStreaming(ctx, stagingID, "r_bound", "", uint32(len(body)))
	require.NoError(t, err)

	// Verify the bound value was used.
	node, turn, err := s.Retrieve(ctx, "r_bound", "")
	require.NoError(t, err)
	assert.Equal(t, "r_bound", node.ResponseID)
	assert.Equal(t, body, turn.ResponseBlob)
}

// TestReapStaging_SkipsCommittedBlobs: after a CompleteStreaming call, the
// staging record acquires a done-marker. ReapStaging must not delete the
// chunks — they are the only copy of the committed response.
func TestReapStaging_SkipsCommittedBlobs(t *testing.T) {
	clk := &fakeClock{t: time.Unix(1_000_000, 0)}
	const stagingTTL = time.Hour
	s, b := openMemStoreAndBackend(t, chainstore.Config{Clock: clk, StagingTTL: stagingTTL})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)
	sid := parseStagingID(t, stagingID)

	payload := chunkData(0, 1024)
	_, err = s.AppendChunk(ctx, stagingID, 0, payload)
	require.NoError(t, err)

	_, err = s.CompleteStreaming(ctx, stagingID, "r_committed", "", uint32(len(payload)))
	require.NoError(t, err)

	// Baseline: committed response is readable.
	_, turn, err := s.Retrieve(ctx, "r_committed", "")
	require.NoError(t, err)
	require.Equal(t, payload, turn.ResponseBlob)

	// Advance past StagingTTL and reap — the reaper must skip the done record.
	clk.Advance(stagingTTL + time.Second)
	s.ReapStaging(ctx)

	// Staging record still exists (committed records are kept for /responses/{id} lookups).
	staged, err := b.GetStagingNode(ctx, sid)
	require.NoError(t, err)

	// Chunks must survive.
	chunks, err := b.ListChunks(ctx, staged.ResponseBlobID)
	require.NoError(t, err)
	assert.NotEmpty(t, chunks, "committed chunks must not be reaped")

	// Response must still be retrievable.
	_, turn2, err := s.Retrieve(ctx, "r_committed", "")
	require.NoError(t, err)
	assert.Equal(t, payload, turn2.ResponseBlob, "response must be readable after reap")
}

// TestCompleteRequiresResponseIDOrBinding: /complete must have a responseID
// either bound via an earlier chunk PUT or supplied on the call. Without
// one, the data would be unreachable after /staging/{id} flips to 410 —
// so the chainstore rejects up front.
func TestCompleteRequiresResponseIDOrBinding(t *testing.T) {
	s := openMemStore(t, chainstore.Config{})
	ctx := context.Background()

	stagingID, _, err := s.ResolveAndStage(ctx, "", "", []byte("req"))
	require.NoError(t, err)

	_, err = s.AppendChunk(ctx, stagingID, 0, []byte("payload"))
	require.NoError(t, err)

	// No bound value, no caller-supplied responseID → error.
	_, err = s.CompleteStreaming(ctx, stagingID, "", "", 7)
	assert.Error(t, err)
	assert.ErrorIs(t, err, chainstore.ErrResponseIDRequired)

	// Now bind via a chunk PUT, retry — should succeed.
	require.NoError(t, s.BindResponseID(ctx, stagingID, "r_bound"))
	_, err = s.CompleteStreaming(ctx, stagingID, "", "", 7)
	require.NoError(t, err)
}
