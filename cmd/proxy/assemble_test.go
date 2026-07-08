package main

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/elevran/charon/internal/inference"
)

// helper: decode json.RawMessage value for a key from map.
func rawString(m map[string]json.RawMessage, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	return string(v)
}

func TestInputToItems_String(t *testing.T) {
	raw := json.RawMessage(`"hello world"`)
	items, err := inputToItems(raw)
	if err != nil {
		t.Fatalf("inputToItems: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	var m map[string]interface{}
	_ = json.Unmarshal(items[0], &m)
	if m["type"] != "message" || m["role"] != "user" {
		t.Errorf("unexpected item: %s", items[0])
	}
}

func TestInputToItems_Array(t *testing.T) {
	raw := json.RawMessage(`[{"type":"message","role":"user"},{"type":"message","role":"assistant"}]`)
	items, err := inputToItems(raw)
	if err != nil {
		t.Fatalf("inputToItems: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
}

func TestInputToItems_Empty(t *testing.T) {
	items, err := inputToItems(nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Errorf("expected empty, got %d items", len(items))
	}
}

func TestBuildInferenceMap(t *testing.T) {
	flatCtx := []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"hi"}`)}
	inputItems := []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"follow up"}`)}

	rawReq := map[string]json.RawMessage{
		"model":                json.RawMessage(`"test"`),
		"temperature":          json.RawMessage(`0.7`),
		"previous_response_id": json.RawMessage(`"resp_old"`),
		"background":           json.RawMessage(`false`),
	}

	infMap := buildInferenceMap(rawReq, flatCtx, inputItems)

	// Gateway-consumed fields must be stripped.
	if _, ok := infMap["previous_response_id"]; ok {
		t.Error("previous_response_id must be stripped")
	}
	if _, ok := infMap["background"]; ok {
		t.Error("background must be stripped")
	}

	// store must be false.
	if rawString(infMap, "store") != "false" {
		t.Errorf("store must be false, got %s", rawString(infMap, "store"))
	}

	// stream must be false (default; streaming callers override).
	if rawString(infMap, "stream") != "false" {
		t.Errorf("stream must be false, got %s", rawString(infMap, "stream"))
	}

	// Pass-through fields must survive.
	if rawString(infMap, "temperature") != "0.7" {
		t.Errorf("temperature must pass through, got %s", rawString(infMap, "temperature"))
	}

	// input must contain flatCtx + inputItems (2 items).
	var items []json.RawMessage
	if err := json.Unmarshal(infMap["input"], &items); err != nil {
		t.Fatalf("decode input: %v", err)
	}
	if len(items) != 2 {
		t.Errorf("expected 2 input items, got %d", len(items))
	}
}

func TestBuildInferenceMap_DoesNotMutateInput(t *testing.T) {
	rawReq := map[string]json.RawMessage{
		"model": json.RawMessage(`"test"`),
	}
	buildInferenceMap(rawReq, nil, nil)
	// original must not have been modified.
	if _, ok := rawReq["store"]; ok {
		t.Error("buildInferenceMap must not mutate the caller's map")
	}
}

func TestBuildResponseResource(t *testing.T) {
	output := json.RawMessage(`{"type":"message","role":"assistant"}`)
	prevID := "resp_prev"
	infResp := &inference.Response{
		ID:     "resp_abc",
		Status: "completed",
		Model:  "test",
		Output: []json.RawMessage{output},
		Usage:  &inference.UsageInfo{InputTokens: 5, OutputTokens: 3, TotalTokens: 8},
	}
	now := time.Now()
	r := buildResponseResource(infResp, &prevID, true, false, now, &now)
	if r.ID != "resp_abc" {
		t.Errorf("ID mismatch: %q", r.ID)
	}
	if r.Object != "response" {
		t.Errorf("Object: %q", r.Object)
	}
	if r.Status != "completed" {
		t.Errorf("Status: %q", r.Status)
	}
	if *r.PreviousResponseID != "resp_prev" {
		t.Errorf("PreviousResponseID: %v", r.PreviousResponseID)
	}
	if r.Usage == nil || r.Usage.TotalTokens != 8 {
		t.Errorf("Usage: %v", r.Usage)
	}
	if r.Background {
		t.Error("Background should be false when not set")
	}
	if len(r.Tools) == 0 {
		// Tools should be an empty slice, not nil (for JSON serialisation)
		if r.Tools == nil {
			t.Error("Tools should be empty slice, not nil")
		}
	}
}

func TestBuildResponseResource_Background(t *testing.T) {
	infResp := &inference.Response{ID: "resp_bg", Status: "completed", Model: "test"}
	now := time.Now()
	r := buildResponseResource(infResp, nil, true, true, now, &now)
	if !r.Background {
		t.Error("Background should be true when set")
	}
}
