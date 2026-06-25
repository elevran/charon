package store_test

// Disruptive tests exercise failure modes that the normal conformance suite
// does not reach: error injection at each write-intent phase, concurrent
// access, and chain-depth enforcement.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
	"github.com/elevran/charon/internal/testutil"
)

var errInject = errors.New("injected failure")

// allIntents returns every in-flight (pending/file_written) intent by using a
// negative stale threshold, which makes every intent appear "older than" the
// threshold.
func allIntents(t *testing.T, idx storage.IndexStore) []model.WriteIntent {
	t.Helper()
	intents, err := idx.ListStaleWriteIntents(context.Background(), -time.Hour)
	require.NoError(t, err)
	return intents
}

// newChainPayloadKey is the expected PayloadStore key for a new-chain response
// (position 0): mirrors the internal payloadKey function.
func newChainPayloadKey(responseID string) string {
	return fmt.Sprintf("%s/00000000_%s.json", responseID, responseID)
}

func disruptiveLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func disruptiveStoreReq() model.StoreRequest {
	var inp responses.ResponseInputItemUnionParam
	return model.StoreRequest{
		Input:  responses.ResponseInputParam{inp},
		Output: []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant"}`)},
		Status: "completed",
		Model:  "test",
	}
}

// --- Payload write failure ---

// TestStorePayloadWriteFailure verifies that a payload-write error:
//   - propagates back to the Store caller (wrapping "write payload")
//   - leaves the response inaccessible (index row never committed)
//   - leaves a pending write-intent (no payload on disk for the Reconciler to recover)
func TestStorePayloadWriteFailure(t *testing.T) {
	realPay := memory.NewPayloadStore()
	idx := memory.NewIndexStore()
	pay := &testutil.HookPayloadStore{
		PayloadStore: realPay,
		OnPut:        func(context.Context, string, []byte) error { return errInject },
	}
	svc := store.New(idx, pay, store.Config{}, disruptiveLogger())

	const id = "resp_pay_fail"
	err := svc.Store(context.Background(), id, disruptiveStoreReq())
	require.Error(t, err)
	assert.ErrorContains(t, err, "write payload")

	_, _, getErr := svc.Retrieve(context.Background(), id)
	assert.ErrorIs(t, getErr, storage.ErrNotFound,
		"response must not be accessible after payload write failure")

	// A single write-intent must be present and still in pending state.
	intents := allIntents(t, idx)
	require.Len(t, intents, 1)
	assert.Equal(t, model.WriteIntentPending, intents[0].Phase,
		"intent must still be pending: UpdateWriteIntent was never reached")
}

// --- Index commit failure ---

// TestStoreIndexPutFailure verifies that an index-commit error:
//   - propagates back to the Store caller (wrapping "commit index")
//   - leaves the response inaccessible (no index row)
//   - leaves a file_written write-intent with the payload already on disk,
//     which the Reconciler can recover by re-running index.Put
func TestStoreIndexPutFailure(t *testing.T) {
	realIdx := memory.NewIndexStore()
	idx := &testutil.HookIndexStore{
		IndexStore: realIdx,
		OnPut:      func(context.Context, model.ResponseMeta) error { return errInject },
	}
	pay := memory.NewPayloadStore()
	svc := store.New(idx, pay, store.Config{}, disruptiveLogger())

	const id = "resp_idx_fail"
	err := svc.Store(context.Background(), id, disruptiveStoreReq())
	require.Error(t, err)
	assert.ErrorContains(t, err, "commit index")

	// Response must not be accessible — index row was never committed.
	_, _, getErr := svc.Retrieve(context.Background(), id)
	assert.ErrorIs(t, getErr, storage.ErrNotFound)

	// Payload must be on disk — the Reconciler can recover from file_written state.
	_, payErr := pay.Get(context.Background(), newChainPayloadKey(id))
	assert.NoError(t, payErr, "payload must be persisted even though index.Put failed")

	// Intent must be in file_written state (UpdateWriteIntent to file_written succeeded;
	// UpdateWriteIntent to committed was never reached).
	intents := allIntents(t, realIdx)
	require.Len(t, intents, 1)
	assert.Equal(t, model.WriteIntentFileWritten, intents[0].Phase)
}

// --- Committed-update failure ---

// TestCommittedUpdateFailureDataAccessible verifies that a failure when marking
// an intent "committed" does NOT make the response inaccessible: index.Put
// (step 5) already succeeded, so Retrieve returns the full record.
//
// The stuck file_written intent is a Reconciler concern, not a data-loss concern.
// This test documents the important invariant: "Store returning an error does NOT
// guarantee the response is inaccessible."
func TestCommittedUpdateFailureDataAccessible(t *testing.T) {
	realIdx := memory.NewIndexStore()
	idx := &testutil.HookIndexStore{
		IndexStore: realIdx,
		OnUpdateWriteIntent: func(ctx context.Context, intentID string, phase model.WriteIntentPhase) error {
			if phase == model.WriteIntentCommitted {
				return errInject
			}
			return realIdx.UpdateWriteIntent(ctx, intentID, phase)
		},
	}
	svc := store.New(idx, memory.NewPayloadStore(), store.Config{}, disruptiveLogger())

	const id = "resp_committed_fail"
	err := svc.Store(context.Background(), id, disruptiveStoreReq())
	// Store reports an error because the intent-cleanup step failed…
	require.Error(t, err)
	assert.ErrorContains(t, err, "update write intent to committed")

	// …but the response is fully committed and must be retrievable.
	meta, _, getErr := svc.Retrieve(context.Background(), id)
	require.NoError(t, getErr, "response must be accessible despite committed-update failure")
	assert.Equal(t, id, meta.ID)

	// Intent is stuck in file_written — Reconciler will re-run index.Put (idempotent).
	intents := allIntents(t, realIdx)
	require.Len(t, intents, 1)
	assert.Equal(t, model.WriteIntentFileWritten, intents[0].Phase)
}

// --- Retrieve during in-flight store ---

// TestRetrieveDuringInflightStore verifies that a GET while a Store is blocked
// at the payload-write step returns ErrNotFound — the response must not be
// visible until the index row is committed (step 5 of the write-intent protocol).
func TestRetrieveDuringInflightStore(t *testing.T) {
	realPay := memory.NewPayloadStore()
	blockPut := make(chan struct{})
	atPut := make(chan struct{}) // closed the first time OnPut is entered

	pay := &testutil.HookPayloadStore{
		PayloadStore: realPay,
		OnPut: func(ctx context.Context, key string, data []byte) error {
			select {
			case <-atPut: // already closed — subsequent Puts just delegate
			default:
				close(atPut)
				<-blockPut
			}
			return realPay.Put(ctx, key, data)
		},
	}
	svc := store.New(memory.NewIndexStore(), pay, store.Config{}, disruptiveLogger())

	const id = "resp_inflight"
	var storeErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		storeErr = svc.Store(context.Background(), id, disruptiveStoreReq())
	}()

	// Wait until the goroutine is blocked at payload write.
	<-atPut

	// Retrieve must return not-found — no index row exists yet.
	_, _, getErr := svc.Retrieve(context.Background(), id)
	assert.ErrorIs(t, getErr, storage.ErrNotFound,
		"Retrieve must return not-found while Store is blocked at payload write")

	// Unblock the store and wait for completion.
	close(blockPut)
	wg.Wait()
	require.NoError(t, storeErr)

	// Now the response must be accessible.
	meta, _, getErr2 := svc.Retrieve(context.Background(), id)
	require.NoError(t, getErr2)
	assert.Equal(t, id, meta.ID)
}

// --- Concurrent stores for the same response ID ---

// TestConcurrentStoreSameID verifies that two simultaneous Store calls for the
// same response ID do not panic, do not corrupt state, and leave the response
// consistently retrievable (last writer wins).
func TestConcurrentStoreSameID(t *testing.T) {
	svc := store.New(memory.NewIndexStore(), memory.NewPayloadStore(), store.Config{}, disruptiveLogger())

	const id = "resp_concurrent"

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			errs[slot] = svc.Store(context.Background(), id, disruptiveStoreReq())
		}(i)
	}
	wg.Wait()

	// At least one must succeed.
	successCount := 0
	for _, e := range errs {
		if e == nil {
			successCount++
		}
	}
	assert.GreaterOrEqual(t, successCount, 1, "at least one concurrent Store must succeed")

	// Response must be retrievable with consistent data.
	meta, _, getErr := svc.Retrieve(context.Background(), id)
	require.NoError(t, getErr)
	assert.Equal(t, id, meta.ID)
}

// --- Chain depth enforcement ---

// TestChainDepthLimitExceeded verifies that Resolve returns ErrChainCorrupted
// when the backward chain walk exceeds maxChainDepth (1000 nodes), preventing
// unbounded memory use during context assembly.
func TestChainDepthLimitExceeded(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping chain-depth test (inserts 1001 index records) under -short")
	}

	idx := memory.NewIndexStore()
	svc := store.New(idx, memory.NewPayloadStore(), store.Config{}, disruptiveLogger())

	const depth = 1001 // one more than maxChainDepth (1000)

	var prevID *string
	var lastID string
	for i := 0; i < depth; i++ {
		id := fmt.Sprintf("resp_%04d", i)
		meta := model.ResponseMeta{
			ID:                 id,
			PreviousResponseID: prevID,
			ChainRootID:        "resp_0000",
			Position:           i,
			Status:             model.StatusCompleted,
			PayloadKey:         fmt.Sprintf("resp_0000/%08d_%s.json", i, id),
		}
		require.NoError(t, idx.Put(context.Background(), meta))
		p := id
		prevID = &p
		lastID = id
	}

	_, _, err := svc.Resolve(context.Background(), lastID, 0)
	assert.ErrorIs(t, err, storage.ErrChainCorrupted,
		"Resolve must return ErrChainCorrupted when chain exceeds the 1000-node depth limit")
}
