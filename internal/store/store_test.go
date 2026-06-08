package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/require"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/filesystem"
	"github.com/elevran/charon/internal/storage/memory"
	sqlitestore "github.com/elevran/charon/internal/storage/sqlite"
	"github.com/elevran/charon/internal/store"
)

var ctx = context.Background()

// storeFactory creates a fresh ContextStore and its underlying stores.
// Cleanup (closing databases, removing temp files) must be registered via
// t.Cleanup inside the factory so each sub-test is self-contained.
type storeFactory func(cfg store.Config) (svc *store.ContextStore, idx storage.IndexStore, pay storage.PayloadStore)

// runConformanceSuite runs all 12 conformance scenarios against any backend
// provided by factory.
func runConformanceSuite(t *testing.T, factory storeFactory) {
	t.Helper()
	t.Run("resolve-new-chain", func(t *testing.T) { testResolveNewChainNotFound(t, factory) })
	t.Run("resolve-multi-turn", func(t *testing.T) { testResolveMultiTurn(t, factory) })
	t.Run("resolve-with-checkpoint", func(t *testing.T) { testResolveWithCheckpoint(t, factory) })
	t.Run("store-and-retrieve", func(t *testing.T) { testStoreAndRetrieve(t, factory) })
	t.Run("write-intent-pending", func(t *testing.T) { testWriteIntentPendingRecovery(t, factory) })
	t.Run("write-intent-file-written", func(t *testing.T) { testWriteIntentFileWrittenRecovery(t, factory) })
	t.Run("ttl-expiry", func(t *testing.T) { testTTLExpiry(t, factory) })
	t.Run("delete-no-cascade", func(t *testing.T) { testDeleteNoCascade(t, factory) })
	t.Run("encrypted-content-roundtrip", func(t *testing.T) { testEncryptedContentRoundtrip(t, factory) })
	t.Run("instructions-not-in-context", func(t *testing.T) { testInstructionsNotInContext(t, factory) })
	t.Run("phase-field-preserved", func(t *testing.T) { testPhaseFieldPreserved(t, factory) })
	t.Run("resolve-efficiency", func(t *testing.T) { testResolveEfficiency(t, factory) })
}

func memoryFactory(cfg store.Config) (*store.ContextStore, storage.IndexStore, storage.PayloadStore) {
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return store.New(idx, pay, cfg, log), idx, pay
}

// TestMemoryBackendConformance runs all scenarios against the in-memory backend.
func TestMemoryBackendConformance(t *testing.T) {
	runConformanceSuite(t, memoryFactory)
}

// TestSQLiteBackendConformance runs the identical suite against SQLite + filesystem.
// Each sub-test gets its own isolated temp directory via t.TempDir() inside the factory.
func TestSQLiteBackendConformance(t *testing.T) {
	runConformanceSuite(t, func(cfg store.Config) (*store.ContextStore, storage.IndexStore, storage.PayloadStore) {
		dataDir := t.TempDir()
		db, err := sqlitestore.Open(filepath.Join(dataDir, "responses.db"), sqlitestore.Config{WALMode: false})
		require.NoError(t, err)
		t.Cleanup(func() { sqlitestore.Close(db) })

		pay, err := filesystem.New(filepath.Join(dataDir, "payloads"))
		require.NoError(t, err)

		idx, err := sqlitestore.NewIndexStore(db)
		require.NoError(t, err)
		log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
		return store.New(idx, pay, cfg, log), idx, pay
	})
}

// --- helpers ---

func rawJSON(v any) json.RawMessage {
	b, _ := json.Marshal(v)
	return b
}

func toInputParam(items []json.RawMessage) responses.ResponseInputParam {
	params := make(responses.ResponseInputParam, len(items))
	for i, raw := range items {
		_ = json.Unmarshal(raw, &params[i])
	}
	return params
}

func storeRequest(prevID *string, input, output []json.RawMessage) model.StoreRequest {
	return model.StoreRequest{
		PreviousResponseID: prevID,
		Input:              toInputParam(input),
		Output:             output,
		Status:             "completed",
		Model:              "test-model",
	}
}

// --- Scenario 1: resolve-new-chain ---

func testResolveNewChainNotFound(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{})
	_, _, err := svc.Resolve(ctx, "resp_doesnotexist")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// --- Scenario 2: resolve-multi-turn ---

func testResolveMultiTurn(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{CheckpointInterval: 100})

	var prevID *string
	var lastID string

	for i := range 5 {
		inp := rawJSON(map[string]any{"type": "message", "role": "user", "content": fmt.Sprintf("turn %d", i)})
		out := rawJSON(map[string]any{"type": "message", "role": "assistant", "content": fmt.Sprintf("turn %d", i)})

		responseID := fmt.Sprintf("resp_%05d", i)
		req := storeRequest(prevID, []json.RawMessage{inp}, []json.RawMessage{out})
		if err := svc.Store(ctx, responseID, req); err != nil {
			t.Fatalf("turn %d Store: %v", i, err)
		}
		prevID = &responseID
		lastID = responseID
	}

	_, flatContext, err := svc.Resolve(ctx, lastID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(flatContext) != 10 {
		t.Fatalf("expected 10 items, got %d", len(flatContext))
	}
	var first map[string]any
	_ = json.Unmarshal(flatContext[0], &first)
	if first["content"] != "turn 0" {
		t.Fatalf("first item should be turn 0, got content=%v", first["content"])
	}
}

// --- Scenario 3: resolve-with-checkpoint ---

func testResolveWithCheckpoint(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{CheckpointInterval: 10})

	var prevID *string
	for i := range 12 {
		responseID := fmt.Sprintf("resp_%05d", i)
		inp := rawJSON(map[string]any{"type": "message", "role": "user", "turn": i})
		out := rawJSON(map[string]any{"type": "message", "role": "assistant", "turn": i})
		req := storeRequest(prevID, []json.RawMessage{inp}, []json.RawMessage{out})
		if err := svc.Store(ctx, responseID, req); err != nil {
			t.Fatalf("turn %d Store: %v", i, err)
		}
		prevID = &responseID
	}

	_, flatContext, err := svc.Resolve(ctx, "resp_00011")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if len(flatContext) != 24 {
		t.Fatalf("expected 24 items, got %d", len(flatContext))
	}
}

// --- Scenario 4: store-and-retrieve ---

func testStoreAndRetrieve(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{})

	inp := rawJSON(map[string]string{"type": "message", "role": "user", "text": "hello"})
	out := rawJSON(map[string]string{"type": "message", "role": "assistant", "text": "hi"})
	req := storeRequest(nil, []json.RawMessage{inp}, []json.RawMessage{out})
	req.Model = "gpt-4o"
	req.Status = "completed"

	if err := svc.Store(ctx, "resp_test1", req); err != nil {
		t.Fatal(err)
	}
	meta, payload, err := svc.Retrieve(ctx, "resp_test1")
	if err != nil {
		t.Fatal(err)
	}
	if meta.ID != "resp_test1" {
		t.Errorf("meta.ID = %q, want resp_test1", meta.ID)
	}
	if meta.Model != "gpt-4o" {
		t.Errorf("meta.Model = %q, want gpt-4o", meta.Model)
	}
	if string(meta.Status) != "completed" {
		t.Errorf("meta.Status = %q, want completed", meta.Status)
	}
	if len(payload.InputItems) != 1 || len(payload.OutputItems) != 1 {
		t.Errorf("unexpected payload items: in=%d out=%d", len(payload.InputItems), len(payload.OutputItems))
	}
}

// --- Scenario 5: write-intent-pending-recovery ---

func testWriteIntentPendingRecovery(t *testing.T, factory storeFactory) {
	t.Helper()
	_, idx, _ := factory(store.Config{})

	oldTime := time.Now().Add(-10 * time.Minute).Unix()
	intent := model.WriteIntent{
		IntentID:   "intent_crash1",
		ResponseID: "resp_crashed",
		PayloadKey: "root/00000000_resp_crashed.json",
		Phase:      model.WriteIntentPending,
		CreatedAt:  oldTime,
		UpdatedAt:  oldTime,
	}
	if err := idx.InsertWriteIntent(ctx, intent); err != nil {
		t.Fatal(err)
	}

	stale, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range stale {
		if si.Phase == model.WriteIntentPending {
			if err := idx.UpdateWriteIntent(ctx, si.IntentID, model.WriteIntentFailed); err != nil {
				t.Fatal(err)
			}
		}
	}

	staleAfter, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range staleAfter {
		if si.IntentID == "intent_crash1" {
			t.Fatalf("intent_crash1 should not be stale after recovery, got phase %q", si.Phase)
		}
	}
}

// --- Scenario 6: write-intent-file-written-recovery ---

func testWriteIntentFileWrittenRecovery(t *testing.T, factory storeFactory) {
	t.Helper()
	_, idx, pay := factory(store.Config{})

	responseID := "resp_filewritten"
	payloadKey := fmt.Sprintf("root/%08d_%s.json", 1, responseID)

	payload := model.ResponsePayload{
		ID:          responseID,
		InputItems:  []json.RawMessage{rawJSON(map[string]string{"type": "message"})},
		OutputItems: []json.RawMessage{rawJSON(map[string]string{"type": "message"})},
	}
	payloadBytes, _ := json.Marshal(payload)
	_ = pay.Put(ctx, payloadKey, payloadBytes)

	oldTime := time.Now().Add(-10 * time.Minute).Unix()
	intent := model.WriteIntent{
		IntentID:   "intent_fw1",
		ResponseID: responseID,
		PayloadKey: payloadKey,
		Phase:      model.WriteIntentFileWritten,
		CreatedAt:  oldTime,
		UpdatedAt:  oldTime,
	}
	_ = idx.InsertWriteIntent(ctx, intent)

	stale, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range stale {
		if si.Phase == model.WriteIntentFileWritten {
			data, err := pay.Get(ctx, si.PayloadKey)
			if err == storage.ErrNotFound {
				_ = idx.UpdateWriteIntent(ctx, si.IntentID, model.WriteIntentFailed)
				continue
			}
			var p model.ResponsePayload
			_ = json.Unmarshal(data, &p)
			meta := model.ResponseMeta{
				ID:         si.ResponseID,
				PayloadKey: si.PayloadKey,
				Status:     "completed",
				CreatedAt:  si.CreatedAt,
			}
			_ = idx.Put(ctx, meta)
			_ = idx.UpdateWriteIntent(ctx, si.IntentID, model.WriteIntentCommitted)
		}
	}

	if _, err := idx.Get(ctx, responseID); err != nil {
		t.Fatalf("expected index row after recovery, got %v", err)
	}

	staleAfter, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range staleAfter {
		if si.IntentID == "intent_fw1" {
			t.Fatalf("intent_fw1 should not be stale after recovery")
		}
	}
}

// --- Scenario 7: ttl-expiry ---

func testTTLExpiry(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, idx, _ := factory(store.Config{TTLDays: 1})

	inp := rawJSON(map[string]string{"type": "message"})
	req := storeRequest(nil, []json.RawMessage{inp}, nil)
	if err := svc.Store(ctx, "resp_ttl1", req); err != nil {
		t.Fatal(err)
	}

	meta, _ := idx.Get(ctx, "resp_ttl1")
	pastExp := time.Now().Add(-time.Hour).Unix()
	meta.ExpiresAt = &pastExp
	_ = idx.Put(ctx, meta)

	expired, _ := idx.ListExpired(ctx, time.Now().Unix())
	for _, m := range expired {
		_ = idx.Delete(ctx, m.ID)
	}

	if _, err := idx.Get(ctx, "resp_ttl1"); err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound after TTL expiry, got %v", err)
	}
}

// --- Scenario 8: delete-no-cascade ---

func testDeleteNoCascade(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{CheckpointInterval: 100})

	var prevID *string
	for i := range 3 {
		responseID := fmt.Sprintf("resp_%05d", i)
		inp := rawJSON(map[string]any{"type": "message", "turn": i})
		req := storeRequest(prevID, []json.RawMessage{inp}, nil)
		if err := svc.Store(ctx, responseID, req); err != nil {
			t.Fatalf("Store turn %d: %v", i, err)
		}
		prevID = &responseID
	}

	if err := svc.Delete(ctx, "resp_00001"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	if _, _, err := svc.Retrieve(ctx, "resp_00000"); err != nil {
		t.Fatalf("turn 0 still retrievable: %v", err)
	}
	if _, _, err := svc.Retrieve(ctx, "resp_00002"); err != nil {
		t.Fatalf("turn 2 still retrievable: %v", err)
	}

	_, _, err := svc.Resolve(ctx, "resp_00002")
	if err != storage.ErrChainCorrupted {
		t.Fatalf("expected ErrChainCorrupted due to gap, got %v", err)
	}
}

// --- Scenario 9: encrypted-content-roundtrip ---

func testEncryptedContentRoundtrip(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{CheckpointInterval: 100})

	encryptedBlob := `{"type":"reasoning","encrypted_content":"OPAQUE_BLOB_12345==","summary":[]}`
	reasoningItem := json.RawMessage(encryptedBlob)

	inp0 := rawJSON(map[string]string{"type": "message", "role": "user"})
	req0 := storeRequest(nil, []json.RawMessage{inp0}, []json.RawMessage{reasoningItem})
	if err := svc.Store(ctx, "resp_enc0", req0); err != nil {
		t.Fatal(err)
	}

	prevID := "resp_enc0"
	inp1 := rawJSON(map[string]string{"type": "message", "role": "user"})
	req1 := storeRequest(&prevID, []json.RawMessage{inp1}, nil)
	if err := svc.Store(ctx, "resp_enc1", req1); err != nil {
		t.Fatal(err)
	}

	_, flatContext, err := svc.Resolve(ctx, "resp_enc1")
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, item := range flatContext {
		if string(item) == encryptedBlob {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("reasoning item with encrypted_content not found verbatim in flat_context")
	}
}

// --- Scenario 10: instructions-not-in-context ---

func testInstructionsNotInContext(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{CheckpointInterval: 100})

	inp := rawJSON(map[string]string{"type": "message", "role": "user", "text": "hello"})
	out := rawJSON(map[string]string{"type": "message", "role": "assistant", "text": "hi"})
	req := storeRequest(nil, []json.RawMessage{inp}, []json.RawMessage{out})
	if err := svc.Store(ctx, "resp_noinstr0", req); err != nil {
		t.Fatal(err)
	}

	prevID := "resp_noinstr0"
	inp1 := rawJSON(map[string]string{"type": "message", "role": "user"})
	req1 := storeRequest(&prevID, []json.RawMessage{inp1}, nil)
	if err := svc.Store(ctx, "resp_noinstr1", req1); err != nil {
		t.Fatal(err)
	}

	_, flatContext, err := svc.Resolve(ctx, "resp_noinstr1")
	if err != nil {
		t.Fatal(err)
	}

	for _, item := range flatContext {
		var m map[string]any
		_ = json.Unmarshal(item, &m)
		if role, ok := m["role"]; ok && role == "system" {
			t.Fatalf("instructions (system role) found in flat_context: %s", item)
		}
		if typ, ok := m["type"]; ok && typ == "instructions" {
			t.Fatalf("instructions item found in flat_context: %s", item)
		}
	}
}

// --- Scenario 11: phase-field-preserved ---

func testPhaseFieldPreserved(t *testing.T, factory storeFactory) {
	t.Helper()
	svc, _, _ := factory(store.Config{CheckpointInterval: 100})

	msgWithPhase := `{"type":"message","role":"assistant","content":[],"phase":"final_answer"}`
	out := json.RawMessage(msgWithPhase)

	inp := rawJSON(map[string]string{"type": "message", "role": "user"})
	req := storeRequest(nil, []json.RawMessage{inp}, []json.RawMessage{out})
	if err := svc.Store(ctx, "resp_phase0", req); err != nil {
		t.Fatal(err)
	}

	prevID := "resp_phase0"
	inp1 := rawJSON(map[string]string{"type": "message", "role": "user"})
	req1 := storeRequest(&prevID, []json.RawMessage{inp1}, nil)
	if err := svc.Store(ctx, "resp_phase1", req1); err != nil {
		t.Fatal(err)
	}

	_, flatContext, err := svc.Resolve(ctx, "resp_phase1")
	if err != nil {
		t.Fatal(err)
	}

	var found bool
	for _, item := range flatContext {
		if string(item) == msgWithPhase {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("message with phase=final_answer not found verbatim in flat_context")
	}
}

// --- Scenario 12: resolve-efficiency ---
// Wraps the payload store with a counter so we can assert ≤10 Get calls
// for a 100-turn chain with K=10.

type countingPayloadStore struct {
	storage.PayloadStore
	gets atomic.Int64
}

func (c *countingPayloadStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.gets.Add(1)
	return c.PayloadStore.Get(ctx, key)
}

func testResolveEfficiency(t *testing.T, factory storeFactory) {
	t.Helper()
	// Get the raw stores from the factory so we can wrap the payload store.
	_, idx, basePay := factory(store.Config{CheckpointInterval: 10})
	counter := &countingPayloadStore{PayloadStore: basePay}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	svc := store.New(idx, counter, store.Config{CheckpointInterval: 10}, log)

	var prevID *string
	for i := range 100 {
		responseID := fmt.Sprintf("resp_%05d", i)
		inp := rawJSON(map[string]any{"type": "message", "turn": i})
		out := rawJSON(map[string]any{"type": "message", "turn": i})
		req := storeRequest(prevID, []json.RawMessage{inp}, []json.RawMessage{out})
		if err := svc.Store(ctx, responseID, req); err != nil {
			t.Fatalf("Store turn %d: %v", i, err)
		}
		prevID = &responseID
	}

	counter.gets.Store(0)
	_, _, err := svc.Resolve(ctx, "resp_00099")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	if gets := counter.gets.Load(); gets > 10 {
		t.Fatalf("Resolve on 100-turn chain made %d PayloadStore.Get calls, want ≤10", gets)
	}
}
