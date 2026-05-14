package store_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/openai/openai-go/responses"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

var ctx = context.Background()

func newSvc(cfg store.Config) (*store.ContextStore, *memory.IndexStore, *memory.PayloadStore) {
	idx := memory.NewIndexStore()
	pay := memory.NewPayloadStore()
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	return store.New(idx, pay, cfg, log), idx, pay
}

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

func TestResolveNewChainNotFound(t *testing.T) {
	svc, _, _ := newSvc(store.Config{})
	_, _, err := svc.Resolve(ctx, "resp_doesnotexist")
	if err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

// --- Scenario 2: resolve-multi-turn ---

func TestResolveMultiTurn(t *testing.T) {
	svc, _, _ := newSvc(store.Config{CheckpointInterval: 100}) // disable checkpoints

	var prevID *string
	var lastID string
	items := make([]json.RawMessage, 0, 10)

	for i := range 5 {
		inp := rawJSON(map[string]any{"type": "message", "role": "user", "content": fmt.Sprintf("turn %d", i)})
		out := rawJSON(map[string]any{"type": "message", "role": "assistant", "content": fmt.Sprintf("turn %d", i)})
		items = append(items, inp, out)

		// For test simplicity, use Store directly and track IDs manually.
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
	// 5 turns × (1 input + 1 output) = 10 items
	if len(flatContext) != 10 {
		t.Fatalf("expected 10 items, got %d", len(flatContext))
	}
	// Verify chronological order: first item is turn 0 input (role=user, content="turn 0").
	var first map[string]any
	_ = json.Unmarshal(flatContext[0], &first)
	if first["content"] != "turn 0" {
		t.Fatalf("first item should be turn 0, got content=%v", first["content"])
	}
}

// --- Scenario 3: resolve-with-checkpoint ---

func TestResolveWithCheckpoint(t *testing.T) {
	svc, _, _ := newSvc(store.Config{CheckpointInterval: 10})

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

	lastID := "resp_00011"
	_, flatContext, err := svc.Resolve(ctx, lastID)
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	// 12 turns × 2 items = 24 items
	if len(flatContext) != 24 {
		t.Fatalf("expected 24 items, got %d", len(flatContext))
	}
}

// --- Scenario 4: store-and-retrieve ---

func TestStoreAndRetrieve(t *testing.T) {
	svc, _, _ := newSvc(store.Config{})

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
	if meta.Status != "completed" {
		t.Errorf("meta.Status = %q, want completed", meta.Status)
	}
	if len(payload.InputItems) != 1 || len(payload.OutputItems) != 1 {
		t.Errorf("unexpected payload items: in=%d out=%d", len(payload.InputItems), len(payload.OutputItems))
	}
}

// --- Scenario 5: write-intent-pending-recovery ---

func TestWriteIntentPendingRecovery(t *testing.T) {
	_, idx, _ := newSvc(store.Config{})

	// Simulate a crash after InsertWriteIntent but before payload write.
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

	// Recovery: pending intent means payload was never written — mark failed.
	stale, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range stale {
		if si.Phase == model.WriteIntentPending {
			if err := idx.UpdateWriteIntent(ctx, si.IntentID, model.WriteIntentFailed); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Verify it's now marked failed (not in stale list anymore).
	staleAfter, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range staleAfter {
		if si.IntentID == "intent_crash1" {
			t.Fatalf("intent_crash1 should not be stale after recovery, got phase %q", si.Phase)
		}
	}
}

// --- Scenario 6: write-intent-file-written-recovery ---

func TestWriteIntentFileWrittenRecovery(t *testing.T) {
	_, idx, pay := newSvc(store.Config{})

	// Simulate a crash after payload write but before index commit.
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

	// Recovery: file_written intent — payload exists, commit the index row.
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
			// Reconstruct meta (simplified for memory-only recovery test).
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

	// Verify index row now exists.
	if _, err := idx.Get(ctx, responseID); err != nil {
		t.Fatalf("expected index row after recovery, got %v", err)
	}

	// Verify intent is now committed (not stale).
	staleAfter, _ := idx.ListStaleWriteIntents(ctx, 5*time.Minute)
	for _, si := range staleAfter {
		if si.IntentID == "intent_fw1" {
			t.Fatalf("intent_fw1 should not be stale after recovery")
		}
	}
}

// --- Scenario 7: ttl-expiry ---

func TestTTLExpiry(t *testing.T) {
	svc, idx, _ := newSvc(store.Config{TTLDays: 1})

	inp := rawJSON(map[string]string{"type": "message"})
	req := storeRequest(nil, []json.RawMessage{inp}, nil)
	if err := svc.Store(ctx, "resp_ttl1", req); err != nil {
		t.Fatal(err)
	}

	// Manually expire it.
	meta, _ := idx.Get(ctx, "resp_ttl1")
	pastExp := time.Now().Add(-time.Hour).Unix()
	meta.ExpiresAt = &pastExp
	_ = idx.Put(ctx, meta)

	// Simulate TTL worker: list expired, delete them.
	expired, _ := idx.ListExpired(ctx, time.Now().Unix())
	for _, m := range expired {
		_ = idx.Delete(ctx, m.ID)
	}

	if _, err := idx.Get(ctx, "resp_ttl1"); err != storage.ErrNotFound {
		t.Fatalf("expected ErrNotFound after TTL expiry, got %v", err)
	}
}

// --- Scenario 8: delete-no-cascade ---

func TestDeleteNoCascade(t *testing.T) {
	svc, _, _ := newSvc(store.Config{CheckpointInterval: 100})

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

	// Delete the middle response (turn 1).
	if err := svc.Delete(ctx, "resp_00001"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	// Adjacent responses (turn 0, turn 2) should still be retrievable.
	if _, _, err := svc.Retrieve(ctx, "resp_00000"); err != nil {
		t.Fatalf("turn 0 still retrievable: %v", err)
	}
	if _, _, err := svc.Retrieve(ctx, "resp_00002"); err != nil {
		t.Fatalf("turn 2 still retrievable: %v", err)
	}

	// Resolve from turn 2 (the head) should fail: chain walks through deleted turn 1.
	_, _, err := svc.Resolve(ctx, "resp_00002")
	if err != storage.ErrChainCorrupted {
		t.Fatalf("expected ErrChainCorrupted due to gap, got %v", err)
	}
}

// --- Scenario 9: encrypted-content-roundtrip ---

func TestEncryptedContentRoundtrip(t *testing.T) {
	svc, _, _ := newSvc(store.Config{CheckpointInterval: 100})

	encryptedBlob := `{"type":"reasoning","encrypted_content":"OPAQUE_BLOB_12345==","summary":[]}`
	reasoningItem := json.RawMessage(encryptedBlob)

	// Store turn 0 with a reasoning item in output.
	inp0 := rawJSON(map[string]string{"type": "message", "role": "user"})
	req0 := storeRequest(nil, []json.RawMessage{inp0}, []json.RawMessage{reasoningItem})
	if err := svc.Store(ctx, "resp_enc0", req0); err != nil {
		t.Fatal(err)
	}

	// Store turn 1 referencing turn 0.
	prevID := "resp_enc0"
	inp1 := rawJSON(map[string]string{"type": "message", "role": "user"})
	req1 := storeRequest(&prevID, []json.RawMessage{inp1}, nil)
	if err := svc.Store(ctx, "resp_enc1", req1); err != nil {
		t.Fatal(err)
	}

	// Resolve from turn 1 should include the reasoning item verbatim.
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

func TestInstructionsNotInContext(t *testing.T) {
	svc, _, _ := newSvc(store.Config{CheckpointInterval: 100})

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
		if t2, ok := m["type"]; ok && t2 == "instructions" {
			t.Fatalf("instructions item found in flat_context: %s", item)
		}
	}
}

// --- Scenario 11: phase-field-preserved ---

func TestPhaseFieldPreserved(t *testing.T) {
	svc, _, _ := newSvc(store.Config{CheckpointInterval: 100})

	// Assistant message with phase field.
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

// --- Efficiency: 100-turn chain with K=10 makes ≤10 PayloadStore.Get calls ---

type countingPayloadStore struct {
	storage.PayloadStore
	gets atomic.Int64
}

func (c *countingPayloadStore) Get(ctx context.Context, key string) ([]byte, error) {
	c.gets.Add(1)
	return c.PayloadStore.Get(ctx, key)
}

func TestResolveEfficiency(t *testing.T) {
	idx := memory.NewIndexStore()
	base := memory.NewPayloadStore()
	counter := &countingPayloadStore{PayloadStore: base}
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

	// Reset counter before Resolve.
	counter.gets.Store(0)

	_, _, err := svc.Resolve(ctx, "resp_00099")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}

	gets := counter.gets.Load()
	if gets > 10 {
		t.Fatalf("Resolve on 100-turn chain made %d PayloadStore.Get calls, want ≤10", gets)
	}
}
