package main

import (
	"context"
	"sync"

	"github.com/elevran/charon/pkg/charon"
)

// chunkedResponseWriter buffers response payload up to maxChunkBytes and
// flushes each chunk to Charon via PUT /staging/{sid}/chunks/{k}. The
// terminal Close() issues PUT /staging/{sid}/complete with the running
// total, OR — when total is 0 — PUT /staging/{sid}/abort to drop the
// staging record without committing an empty turn.
//
// Chunks are raw bytes; Charon concatenates them in order to produce the
// final stored blob. maxChunkBytes controls flush frequency only — there is
// no JSON framing applied by this type.
//
// Concurrency: Add and Close may be called from separate goroutines.
// The mutex serialises all state mutations.
type chunkedResponseWriter struct {
	ctx           context.Context
	backend       charon.Backend
	stagingID     string
	responseID    string
	tenantKey     string
	maxChunkBytes int64

	mu        sync.Mutex
	pending   []byte // bytes awaiting next flush
	k         uint32 // next chunk index
	total     uint64 // running byte sum of flushed chunk bodies
	closed    bool
	closedCh  chan struct{}
	closedErr error
}

// newChunkedResponseWriter constructs a writer targeting the given staging record.
func newChunkedResponseWriter(
	ctx context.Context, b charon.Backend,
	stagingID, responseID, tenantKey string, maxChunkBytes int64,
) *chunkedResponseWriter {
	return &chunkedResponseWriter{
		ctx:           ctx,
		backend:       b,
		stagingID:     stagingID,
		responseID:    responseID,
		tenantKey:     tenantKey,
		maxChunkBytes: maxChunkBytes,
		closedCh:      make(chan struct{}),
	}
}

// Add appends p to the pending buffer. If appending p would push the pending
// byte count past maxChunkBytes (and pending is non-empty), the existing
// pending is flushed first as chunk k, then p starts a fresh chunk.
//
// Empty or nil p is a no-op. When maxChunkBytes <= 0, all bytes accumulate
// without flushing until Close. When len(p) alone exceeds maxChunkBytes, p is
// split across as many maxChunkBytes-sized flushes as needed.
func (w *chunkedResponseWriter) Add(p []byte) error {
	if len(p) == 0 {
		return nil
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return w.closedErr
	}

	if w.maxChunkBytes <= 0 {
		w.pending = append(w.pending, p...)
		return nil
	}

	for len(p) > 0 {
		room := w.maxChunkBytes - int64(len(w.pending))
		if room <= 0 {
			if err := w.flush(); err != nil {
				return err
			}
			room = w.maxChunkBytes
		}
		take := int64(len(p))
		if take > room {
			take = room
		}
		w.pending = append(w.pending, p[:take]...)
		p = p[take:]
		if int64(len(w.pending)) >= w.maxChunkBytes {
			if err := w.flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

// Close flushes any remaining bytes and signals terminal completion:
//   - total > 0: PUT /staging/{sid}/complete
//   - total == 0: PUT /staging/{sid}/abort
//
// Subsequent calls block on closedCh and return the same error. Idempotent.
func (w *chunkedResponseWriter) Close() error {
	w.mu.Lock()

	if w.closed {
		ch := w.closedCh
		err := w.closedErr
		w.mu.Unlock()
		<-ch
		return err
	}
	w.closed = true
	defer close(w.closedCh)

	// Flush remaining bytes.
	if len(w.pending) > 0 {
		if err := w.flush(); err != nil {
			w.closedErr = err
			w.mu.Unlock()
			return err
		}
	}

	total := w.total
	w.mu.Unlock()

	if total == 0 {
		return w.backend.Abort(w.ctx, w.stagingID)
	}
	_, err := w.backend.Complete(w.ctx, w.stagingID, w.responseID, w.tenantKey, uint32(total))
	w.mu.Lock()
	w.closedErr = err
	w.mu.Unlock()
	return err
}

// Abort issues PUT /staging/{sid}/abort unconditionally (failure path).
// Subsequent Add and Close calls return the abort error.
func (w *chunkedResponseWriter) Abort() error {
	w.mu.Lock()

	if w.closed {
		ch := w.closedCh
		w.mu.Unlock()
		<-ch
		return w.closedErr
	}
	w.closed = true
	defer close(w.closedCh)
	w.mu.Unlock()

	err := w.backend.Abort(w.ctx, w.stagingID)
	w.mu.Lock()
	w.closedErr = err
	w.mu.Unlock()
	return err
}

// flush sends pending bytes as chunk k.
// Must be called with w.mu held. Releases and reacquires the lock around
// the network call.
func (w *chunkedResponseWriter) flush() error {
	body := make([]byte, len(w.pending))
	copy(body, w.pending)
	k := w.k

	// Release lock during I/O.
	w.mu.Unlock()
	err := w.backend.AppendChunk(w.ctx, w.stagingID, k, w.responseID, body)
	w.mu.Lock()

	if err != nil {
		w.closedErr = err
		return err
	}
	w.k++
	w.total += uint64(len(body))
	w.pending = w.pending[:0]
	return nil
}
