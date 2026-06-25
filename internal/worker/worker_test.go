package worker_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/worker"
)

var (
	ctx    = context.Background()
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
)

func pastTime() int64 {
	return time.Now().Add(-2 * time.Hour).Unix()
}

// --- TTL Worker ---

func TestTTLWorkerDeletesExpired(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timer-based worker test under -short")
	}
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	past := time.Now().Add(-time.Hour).Unix()
	future := time.Now().Add(time.Hour).Unix()

	for _, id := range []string{"exp1", "exp2", "exp3"} {
		require.NoError(t, pay.Put(ctx, id+"_key", []byte("data")))
		require.NoError(t, idx.Put(ctx, model.ResponseMeta{
			ID:         id,
			PayloadKey: id + "_key",
			ExpiresAt:  &past,
			Status:     model.StatusCompleted,
			CreatedAt:  pastTime(),
		}))
	}
	require.NoError(t, idx.Put(ctx, model.ResponseMeta{
		ID:         "live1",
		PayloadKey: "live1_key",
		ExpiresAt:  &future,
		Status:     model.StatusCompleted,
		CreatedAt:  pastTime(),
	}))

	w := worker.NewCleaner(idx, pay, logger, 50*time.Millisecond)
	ctx2, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	w.Run(ctx2)

	for _, id := range []string{"exp1", "exp2", "exp3"} {
		_, err := idx.Get(ctx, id)
		assert.ErrorIs(t, err, storage.ErrNotFound, "expected %q deleted", id)
	}
	_, err := idx.Get(ctx, "live1")
	assert.NoError(t, err, "live1 should survive TTL")
}

func TestTTLWorkerStopsCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timer-based worker test under -short")
	}
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	w := worker.NewCleaner(idx, pay, logger, 10*time.Second)

	ctx2, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		w.Run(ctx2)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("TTLWorker did not stop within 1s after cancel")
	}
}

// --- Recovery Worker ---

func TestRecoveryWorkerPendingMarkedFailed(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timer-based worker test under -short")
	}
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	old := pastTime()
	intent := model.WriteIntent{
		IntentID:   "i_pending",
		ResponseID: "resp_p",
		PayloadKey: "root/00000000_resp_p.json",
		Phase:      model.WriteIntentPending,
		CreatedAt:  old,
		UpdatedAt:  old,
	}
	require.NoError(t, idx.InsertWriteIntent(ctx, intent))

	w := worker.NewReconciler(idx, pay, logger, time.Minute, 10*time.Second)
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	w.Run(ctx2)

	stale, err := idx.ListStaleWriteIntents(ctx, time.Minute)
	require.NoError(t, err)
	for _, si := range stale {
		assert.NotEqual(t, "i_pending", si.IntentID, "pending intent should have been resolved")
	}
}

func TestRecoveryWorkerFileWrittenCommitted(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timer-based worker test under -short")
	}
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	responseID := "resp_fw"
	payloadKey := "root/00000001_resp_fw.json"
	payload := model.ResponsePayload{
		ID:          responseID,
		InputItems:  []json.RawMessage{json.RawMessage(`{"type":"message"}`)},
		OutputItems: []json.RawMessage{json.RawMessage(`{"type":"message"}`)},
	}
	payloadBytes, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NoError(t, pay.Put(ctx, payloadKey, payloadBytes))

	old := pastTime()
	intent := model.WriteIntent{
		IntentID:   "i_fw",
		ResponseID: responseID,
		PayloadKey: payloadKey,
		Phase:      model.WriteIntentFileWritten,
		CreatedAt:  old,
		UpdatedAt:  old,
	}
	require.NoError(t, idx.InsertWriteIntent(ctx, intent))

	w := worker.NewReconciler(idx, pay, logger, time.Minute, 10*time.Second)
	ctx2, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	w.Run(ctx2)

	_, err = idx.Get(ctx, responseID)
	assert.NoError(t, err, "expected committed index row")

	stale, err := idx.ListStaleWriteIntents(ctx, time.Minute)
	require.NoError(t, err)
	for _, si := range stale {
		assert.NotEqual(t, "i_fw", si.IntentID, "file_written intent should be committed")
	}
}

func TestRecoveryWorkerStopsCleanly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping timer-based worker test under -short")
	}
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	w := worker.NewReconciler(idx, pay, logger, 5*time.Minute, 10*time.Second)

	ctx2, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		w.Run(ctx2)
		close(done)
	}()
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RecoveryWorker did not stop within 1s after cancel")
	}
}

// --- Eviction ---

// storeChain stores n responses as a chain (each references the previous) and
// returns the root ID. All entries are created at baseTime + i seconds so the
// chains can be ordered by age.
func storeChain(t *testing.T, idx *memory.IndexStore, pay *memory.PayloadStore, rootID string, n int, baseTime int64) {
	t.Helper()
	var prevID *string
	for i := 0; i < n; i++ {
		id := rootID
		if i > 0 {
			id = rootID + "_" + string(rune('a'+i))
		}
		payKey := id + "_pay"
		require.NoError(t, pay.Put(ctx, payKey, []byte("data")))
		require.NoError(t, idx.Put(ctx, model.ResponseMeta{
			ID:                 id,
			PreviousResponseID: prevID,
			ChainRootID:        rootID,
			Position:           i,
			PayloadKey:         payKey,
			Status:             model.StatusCompleted,
			CreatedAt:          baseTime + int64(i),
		}))
		s := id
		prevID = &s
	}
}

func TestEvictionDeletesOldestChains(t *testing.T) {
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	// Create 3 chains of 2 entries each (6 total).
	// Chains are aged: chain A is oldest, chain C is newest.
	now := time.Now().Unix()
	storeChain(t, idx, pay, "chainA", 2, now-300)
	storeChain(t, idx, pay, "chainB", 2, now-200)
	storeChain(t, idx, pay, "chainC", 2, now-100)

	count, err := idx.Count(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(6), count)

	// maxResponses=6, watermark=90% → triggers at 6; evict down to 80% (4 entries → target 4).
	// But with a small store, 80% of 6 = 4.8 → 4. We need to evict at least chainA (2 entries)
	// to get below 5.4 (high watermark).
	w := worker.NewCleanerWithEviction(idx, pay, logger, time.Hour, 6, 0.9)
	w.EvictOnce(ctx)

	afterCount, err := idx.Count(ctx)
	require.NoError(t, err)
	// After eviction we should be at or below 80% of 6 (4.8 → 4 entries).
	assert.LessOrEqual(t, afterCount, int64(4), "expected count to drop to ≤4 after eviction")

	// chainA (oldest) must be gone.
	_, errA := idx.Get(ctx, "chainA")
	assert.ErrorIs(t, errA, storage.ErrNotFound, "chainA root should be evicted")
	_, errA2 := idx.Get(ctx, "chainA_b")
	assert.ErrorIs(t, errA2, storage.ErrNotFound, "chainA second entry should be evicted")

	// chainC (newest) must still be present.
	_, errC := idx.Get(ctx, "chainC")
	assert.NoError(t, errC, "chainC (newest) should survive eviction")
}

func TestEvictionDisabledWhenMaxResponsesZero(t *testing.T) {
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	now := time.Now().Unix()
	storeChain(t, idx, pay, "chainX", 3, now-100)

	w := worker.NewCleanerWithEviction(idx, pay, logger, time.Hour, 0, 0.9)
	w.EvictOnce(ctx)

	count, err := idx.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(3), count, "no eviction when maxResponses=0")
}

func TestEvictionNoOpBelowWatermark(t *testing.T) {
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()

	now := time.Now().Unix()
	storeChain(t, idx, pay, "chainY", 2, now-100) // 2 entries

	// maxResponses=10, watermark=90% → triggers at 9; we only have 2.
	w := worker.NewCleanerWithEviction(idx, pay, logger, time.Hour, 10, 0.9)
	w.EvictOnce(ctx)

	count, err := idx.Count(ctx)
	require.NoError(t, err)
	assert.Equal(t, int64(2), count, "no eviction when below watermark")
}
