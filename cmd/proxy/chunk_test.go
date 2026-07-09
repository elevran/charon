package main

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/pkg/charon"
)

// ---------------------------------------------------------------------------
// recordingBackend — minimal Backend mock for chunkedResponseWriter tests
// ---------------------------------------------------------------------------

type chunkCall struct {
	k    uint32
	body []byte
}

type completeCall struct {
	stagingID  string
	responseID string
	tenantKey  string
	total      uint32
}

type recordingBackend struct {
	mu          sync.Mutex
	chunks      []chunkCall
	completes   []completeCall
	aborts      []string
	chunkErr    error
	completeErr error
}

func (r *recordingBackend) AppendChunk(_ context.Context, _ string, k uint32, _ string, body []byte) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.chunkErr != nil {
		return r.chunkErr
	}
	cp := make([]byte, len(body))
	copy(cp, body)
	r.chunks = append(r.chunks, chunkCall{k: k, body: cp})
	return nil
}

func (r *recordingBackend) Complete(_ context.Context, stagingID, responseID, tenantKey string, total uint32) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.completeErr != nil {
		return "", r.completeErr
	}
	r.completes = append(r.completes, completeCall{stagingID: stagingID, responseID: responseID, tenantKey: tenantKey, total: total})
	return responseID, nil
}

func (r *recordingBackend) Abort(_ context.Context, stagingID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.aborts = append(r.aborts, stagingID)
	return nil
}

func (r *recordingBackend) snapshot() (chunks []chunkCall, completes []completeCall, aborts []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]chunkCall(nil), r.chunks...), append([]completeCall(nil), r.completes...), append([]string(nil), r.aborts...)
}

// Unused interface methods — must exist to satisfy charon.Backend.
func (r *recordingBackend) Resolve(_ context.Context, _, _ string, _ []byte) (string, []charon.ResolveTurn, error) {
	return "", nil, nil
}
func (r *recordingBackend) GetChain(_ context.Context, _, _ string) ([]charon.ResolveTurn, error) {
	return nil, nil
}
func (r *recordingBackend) Store(_ context.Context, _, _, _ string, _ []byte) error { return nil }
func (r *recordingBackend) Retrieve(_ context.Context, _, _ string) (*charon.RetrieveResponse, error) {
	return nil, nil
}
func (r *recordingBackend) Delete(_ context.Context, _, _ string) error { return nil }

var _ charon.Backend = (*recordingBackend)(nil)

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func newWriter(b *recordingBackend, maxChunkBytes int64) *chunkedResponseWriter {
	return newChunkedResponseWriter(context.Background(), b, "sid1", "resp1", "tenant1", maxChunkBytes)
}

// ---------------------------------------------------------------------------
// tests
// ---------------------------------------------------------------------------

func TestWriterSingleSmallChunk(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 1024)
	payload := []byte(`{"id":"r1","output":[]}`)
	require.NoError(t, w.Add(payload))
	require.NoError(t, w.Close())

	chunks, completes, aborts := b.snapshot()
	assert.Len(t, chunks, 1)
	assert.Equal(t, uint32(0), chunks[0].k)
	assert.Equal(t, payload, chunks[0].body)
	assert.Len(t, completes, 1)
	assert.Equal(t, uint32(len(payload)), completes[0].total)
	assert.Empty(t, aborts)
}

func TestWriterCapFlush(t *testing.T) {
	// cap = 10 bytes; 20 bytes total → 2 full chunks (intra-Add splitting fills
	// each chunk to cap before moving on, so bytes span chunk boundaries freely).
	b := &recordingBackend{}
	w := newWriter(b, 10)
	p1 := []byte("12345678") // 8 bytes
	p2 := []byte("abcdefgh") // 8 bytes
	p3 := []byte("ABCD")     // 4 bytes → 20 total → 2 full chunks of 10, no remainder at Close
	require.NoError(t, w.Add(p1))
	require.NoError(t, w.Add(p2))
	require.NoError(t, w.Add(p3))
	require.NoError(t, w.Close())

	chunks, completes, aborts := b.snapshot()
	assert.GreaterOrEqual(t, len(chunks), 2, "20 bytes at 10-byte cap must produce ≥2 chunks")
	assert.Len(t, completes, 1)
	assert.Empty(t, aborts)

	// chunk indices must be 0,1,2
	for i, c := range chunks {
		assert.Equal(t, uint32(i), c.k)
	}

	// total must equal sum of chunk body lengths
	var expectedTotal uint32
	for _, c := range chunks {
		expectedTotal += uint32(len(c.body))
	}
	assert.Equal(t, expectedTotal, completes[0].total)

	// concatenation must reproduce the original bytes in order
	var concat []byte
	for _, c := range chunks {
		concat = append(concat, c.body...)
	}
	assert.Equal(t, append(append(p1, p2...), p3...), concat)
}

func TestWriterZeroBytesAborts(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 1024)
	require.NoError(t, w.Close())

	chunks, completes, aborts := b.snapshot()
	assert.Empty(t, chunks)
	assert.Empty(t, completes)
	assert.Len(t, aborts, 1)
	assert.Equal(t, "sid1", aborts[0])
}

func TestWriterZeroItemAborts(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 1024)
	require.NoError(t, w.Add(nil))
	require.NoError(t, w.Add([]byte{}))
	require.NoError(t, w.Close())

	chunks, completes, aborts := b.snapshot()
	assert.Empty(t, chunks)
	assert.Empty(t, completes)
	assert.Len(t, aborts, 1)
}

func TestWriterNonZeroCloses(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 1024)
	require.NoError(t, w.Add([]byte(`{"id":"r1"}`)))
	require.NoError(t, w.Close())

	_, completes, aborts := b.snapshot()
	assert.Len(t, completes, 1)
	assert.Empty(t, aborts)
}

func TestWriterCloseIdempotent(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 1024)
	require.NoError(t, w.Add([]byte(`{"id":"r1"}`)))

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range 2 {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = w.Close()
		}(i)
	}
	wg.Wait()

	assert.Equal(t, errs[0], errs[1], "both Close calls must return the same error")

	_, completes, _ := b.snapshot()
	assert.Len(t, completes, 1, "terminal flush must execute exactly once")
}

func TestWriterAbortSkipsComplete(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 1024)
	require.NoError(t, w.Add([]byte(`{"id":"r1"}`)))
	require.NoError(t, w.Abort())

	_, completes, aborts := b.snapshot()
	assert.Empty(t, completes)
	assert.Len(t, aborts, 1)
}

func TestWriterCancelDuringFlush(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	b := &recordingBackend{}
	w := newChunkedResponseWriter(ctx, b, "sid1", "resp1", "tenant1", 4)

	cancel()
	b.chunkErr = context.Canceled

	// Two adds: first buffers (4 bytes exactly == cap, no flush yet since not exceeding),
	// second Add triggers flush because combined would exceed cap.
	err := w.Add([]byte("abcd")) // 4 bytes == cap, no flush (not > cap)
	if err == nil {
		err = w.Add([]byte("ef")) // 4+2=6 > 4, triggers flush → error
	}
	if err == nil {
		err = w.Close()
	}
	assert.ErrorIs(t, err, context.Canceled)

	// Subsequent Add returns the same error.
	err2 := w.Add([]byte("z"))
	assert.ErrorIs(t, err2, context.Canceled)
}

func TestWriterFlushFailurePropagates(t *testing.T) {
	sentinel := errors.New("chunk write failed")
	b := &recordingBackend{chunkErr: sentinel}
	w := newWriter(b, 5)

	// cap=5: first Add (4 bytes) buffers without flushing (4 < 5).
	// Second Add (2 bytes) fills pending to 6 > 5, triggers flush → error.
	require.NoError(t, w.Add([]byte("abcd")))
	err := w.Add([]byte("ef"))
	if err == nil {
		err = w.Close()
	}
	assert.ErrorIs(t, err, sentinel)

	// Subsequent calls return the same error.
	assert.ErrorIs(t, w.Add([]byte("x")), sentinel)
}

func TestWriterBodyShape(t *testing.T) {
	b := &recordingBackend{}
	w := newWriter(b, 10) // small cap to force multiple chunks

	// Simulate storing a large blob in two parts.
	part1 := []byte(`{"id":"r1","output":[`)
	part2 := []byte(`{"type":"msg"}]}`)
	require.NoError(t, w.Add(part1))
	require.NoError(t, w.Add(part2))
	require.NoError(t, w.Close())

	chunks, _, _ := b.snapshot()
	require.NotEmpty(t, chunks)

	// Concatenated chunks must reproduce the original bytes.
	var concat []byte
	for _, c := range chunks {
		concat = append(concat, c.body...)
	}
	assert.Equal(t, append(part1, part2...), concat)
}
