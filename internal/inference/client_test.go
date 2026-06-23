package inference_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/elevran/charon/internal/inference"
)

var ctx = context.Background()

func TestMockComplete(t *testing.T) {
	mock := inference.NewMockServer()
	defer mock.Close()

	client := inference.New(mock.URL, "", 0)
	req := map[string]json.RawMessage{
		"model": json.RawMessage(`"test"`),
		"input": json.RawMessage(`[]`),
	}
	resp, err := client.Complete(ctx, req)
	if err != nil {
		t.Fatalf("Complete: %v", err)
	}
	if !strings.HasPrefix(resp.ID, "resp_") {
		t.Errorf("expected resp_-prefixed ID, got %q", resp.ID)
	}
	if resp.Status != "completed" {
		t.Errorf("expected status completed, got %q", resp.Status)
	}
	if len(resp.Output) == 0 {
		t.Error("expected non-empty output")
	}
}

func TestMockIDsAreUnique(t *testing.T) {
	mock := inference.NewMockServer()
	defer mock.Close()
	client := inference.New(mock.URL, "", 0)

	req := map[string]json.RawMessage{"model": json.RawMessage(`"test"`)}
	r1, _ := client.Complete(ctx, req)
	r2, _ := client.Complete(ctx, req)
	if r1.ID == r2.ID {
		t.Errorf("expected unique IDs, both got %q", r1.ID)
	}
}

func TestMockStream(t *testing.T) {
	mock := inference.NewMockServer()
	defer mock.Close()
	client := inference.New(mock.URL, "", 0)

	ch, err := client.Stream(ctx, map[string]json.RawMessage{
		"model":  json.RawMessage(`"test"`),
		"stream": json.RawMessage(`true`),
	})
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}

	var events []inference.SSEEvent
	for evt := range ch {
		events = append(events, evt)
	}

	if len(events) == 0 {
		t.Fatal("expected SSE events")
	}
	last := events[len(events)-1]
	if last.Type != "response.completed" {
		t.Errorf("expected last event response.completed, got %q", last.Type)
	}
	if last.Response == nil || last.Response.Status != "completed" {
		t.Errorf("expected completed response in final event")
	}
	if len(last.Response.Output) == 0 {
		t.Error("expected non-empty output in completed event")
	}
}
