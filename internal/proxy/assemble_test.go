package proxy

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/elevran/charon/internal/inference"
)

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

func TestBuildInferenceRequest(t *testing.T) {
	flatCtx := []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"hi"}`)}
	inputItems := []json.RawMessage{json.RawMessage(`{"type":"message","role":"user","content":"follow up"}`)}
	req := CreateRequest{Model: "test", Stream: false}

	infReq := buildInferenceRequest(req, flatCtx, inputItems)
	if infReq.Store {
		t.Error("inference request must have store:false")
	}
	if len(infReq.Input) != 2 {
		t.Errorf("expected 2 input items, got %d", len(infReq.Input))
	}
	if infReq.Stream {
		t.Error("stream flag should match request (false)")
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
	r := buildResponseResource(infResp, &prevID, true, time.Now())
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
	if len(r.Tools) == 0 {
		// Tools should be an empty slice, not nil (for JSON serialisation)
		if r.Tools == nil {
			t.Error("Tools should be empty slice, not nil")
		}
	}
}
