package sqlite_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/sqlite"
)

var ctx = context.Background()

func openDB(t *testing.T) *sqlite.IndexStore {
	t.Helper()
	db, err := sqlite.Open(":memory:", sqlite.Config{})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { sqlite.Close(db) })
	idx, err := sqlite.NewIndexStore(db)
	if err != nil {
		t.Fatalf("NewIndexStore: %v", err)
	}
	return idx
}

func sampleMeta(id string) model.ResponseMeta {
	return model.ResponseMeta{
		ID:          id,
		ChainRootID: id,
		Position:    0,
		Status:      model.StatusCompleted,
		PayloadKey:  id + "/00000000_" + id + ".json",
		CreatedAt:   time.Now().Unix(),
	}
}

// --- Put / Get round-trip ---

func TestPutGet(t *testing.T) {
	idx := openDB(t)
	meta := sampleMeta("resp_a")
	meta.Model = "gpt-4o"

	if err := idx.Put(ctx, meta); err != nil {
		t.Fatalf("Put: %v", err)
	}
	got, err := idx.Get(ctx, "resp_a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != "resp_a" || got.Model != "gpt-4o" {
		t.Errorf("unexpected meta: %+v", got)
	}
}

func TestGetNotFound(t *testing.T) {
	idx := openDB(t)
	_, err := idx.Get(ctx, "resp_missing")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestPutUpsert(t *testing.T) {
	idx := openDB(t)
	meta := sampleMeta("resp_upsert")
	_ = idx.Put(ctx, meta)

	meta.Model = "updated-model"
	if err := idx.Put(ctx, meta); err != nil {
		t.Fatalf("second Put: %v", err)
	}
	got, _ := idx.Get(ctx, "resp_upsert")
	if got.Model != "updated-model" {
		t.Errorf("expected updated-model, got %q", got.Model)
	}
}

// --- Delete ---

func TestDeleteIdempotent(t *testing.T) {
	idx := openDB(t)
	_ = idx.Put(ctx, sampleMeta("resp_del"))

	if err := idx.Delete(ctx, "resp_del"); err != nil {
		t.Fatalf("first Delete: %v", err)
	}
	// Deleting an absent record must not error.
	if err := idx.Delete(ctx, "resp_del"); err != nil {
		t.Fatalf("second Delete: %v", err)
	}
	_, err := idx.Get(ctx, "resp_del")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// --- List pagination ---

func TestListPagination(t *testing.T) {
	idx := openDB(t)
	for i := range 5 {
		m := sampleMeta([]string{"resp_a", "resp_b", "resp_c", "resp_d", "resp_e"}[i])
		m.CreatedAt = int64(i + 1)
		_ = idx.Put(ctx, m)
	}

	// First page: 2 items.
	page1, err := idx.List(ctx, storage.ListOptions{Limit: 2})
	if err != nil {
		t.Fatalf("List page1: %v", err)
	}
	if len(page1) != 2 {
		t.Fatalf("expected 2, got %d", len(page1))
	}

	// Second page using cursor.
	page2, err := idx.List(ctx, storage.ListOptions{Cursor: page1[len(page1)-1].ID, Limit: 2})
	if err != nil {
		t.Fatalf("List page2: %v", err)
	}
	if len(page2) != 2 {
		t.Fatalf("expected 2, got %d", len(page2))
	}
	if page2[0].ID == page1[1].ID {
		t.Errorf("page2 overlaps page1")
	}
}

// --- WriteIntent ---

func TestInsertWriteIntentDuplicate(t *testing.T) {
	idx := openDB(t)
	now := time.Now().Unix()
	intent := model.WriteIntent{
		IntentID:   "wi_1",
		ResponseID: "resp_1",
		PayloadKey: "k",
		Phase:      model.WriteIntentPending,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	if err := idx.InsertWriteIntent(ctx, intent); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	if err := idx.InsertWriteIntent(ctx, intent); err != storage.ErrAlreadyExists {
		t.Fatalf("expected ErrAlreadyExists, got %v", err)
	}
}

func TestUpdateWriteIntentNotFound(t *testing.T) {
	idx := openDB(t)
	err := idx.UpdateWriteIntent(ctx, "wi_missing", model.WriteIntentCommitted)
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestListStaleWriteIntents(t *testing.T) {
	idx := openDB(t)
	old := time.Now().Add(-10 * time.Minute).Unix()
	recent := time.Now().Unix()

	intents := []model.WriteIntent{
		{IntentID: "wi_pending_old", ResponseID: "r1", PayloadKey: "k1", Phase: model.WriteIntentPending, CreatedAt: old, UpdatedAt: old},
		{IntentID: "wi_fw_old", ResponseID: "r2", PayloadKey: "k2", Phase: model.WriteIntentFileWritten, CreatedAt: old, UpdatedAt: old},
		{IntentID: "wi_committed_old", ResponseID: "r3", PayloadKey: "k3", Phase: model.WriteIntentCommitted, CreatedAt: old, UpdatedAt: old},
		{IntentID: "wi_pending_recent", ResponseID: "r4", PayloadKey: "k4", Phase: model.WriteIntentPending, CreatedAt: recent, UpdatedAt: recent},
	}
	for _, i := range intents {
		_ = idx.InsertWriteIntent(ctx, i)
	}

	stale, err := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	if err != nil {
		t.Fatalf("ListStale: %v", err)
	}
	if len(stale) != 2 {
		t.Fatalf("expected 2 stale intents, got %d", len(stale))
	}
	for _, s := range stale {
		if s.Phase == model.WriteIntentCommitted {
			t.Errorf("committed intent should not be stale")
		}
		if s.IntentID == "wi_pending_recent" {
			t.Errorf("recent intent should not be stale")
		}
	}
}

// --- ListExpired ---

func TestListExpired(t *testing.T) {
	idx := openDB(t)
	past := time.Now().Add(-time.Hour).Unix()
	future := time.Now().Add(time.Hour).Unix()

	expired := sampleMeta("resp_expired")
	expired.ExpiresAt = &past
	_ = idx.Put(ctx, expired)

	notExpired := sampleMeta("resp_not_expired")
	notExpired.ExpiresAt = &future
	_ = idx.Put(ctx, notExpired)

	noExpiry := sampleMeta("resp_no_expiry")
	_ = idx.Put(ctx, noExpiry)

	results, err := idx.ListExpired(ctx, time.Now().Unix())
	if err != nil {
		t.Fatalf("ListExpired: %v", err)
	}
	if len(results) != 1 || results[0].ID != "resp_expired" {
		t.Errorf("expected only resp_expired, got %v", results)
	}
}

// --- Concurrent Put under -race ---

func TestConcurrentPut(t *testing.T) {
	idx := openDB(t)
	var wg sync.WaitGroup
	for i := range 20 {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			m := sampleMeta([]string{
				"resp_c0", "resp_c1", "resp_c2", "resp_c3", "resp_c4",
				"resp_c5", "resp_c6", "resp_c7", "resp_c8", "resp_c9",
				"resp_c10", "resp_c11", "resp_c12", "resp_c13", "resp_c14",
				"resp_c15", "resp_c16", "resp_c17", "resp_c18", "resp_c19",
			}[n])
			_ = idx.Put(ctx, m)
		}(i)
	}
	wg.Wait()
}
