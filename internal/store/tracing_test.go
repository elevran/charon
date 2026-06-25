package store_test

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"

	"github.com/openai/openai-go/responses"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage/memory"
	"github.com/elevran/charon/internal/store"
)

func newTracingStore(t *testing.T) (*store.ContextStore, *tracetest.SpanRecorder) {
	t.Helper()
	rec := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(rec))
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := store.New(memory.NewIndexStore(), memory.NewPayloadStore(), store.Config{TracerProvider: tp}, log)
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })
	return svc, rec
}

func TestStoreSpan(t *testing.T) {
	svc, rec := newTracingStore(t)
	var inp responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(json.RawMessage(`{"type":"message","role":"user"}`), &inp)
	req := model.StoreRequest{
		Input:  responses.ResponseInputParam{inp},
		Output: []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant"}`)},
		Status: "completed",
		Model:  "test",
	}
	require.NoError(t, svc.Store(context.Background(), "resp_trace1", req))

	spans := rec.Ended()
	require.NotEmpty(t, spans, "expected at least one span")
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name()
	}
	assert.Contains(t, names, "store.Store")
}

func TestResolveSpan(t *testing.T) {
	svc, rec := newTracingStore(t)
	var inp responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(json.RawMessage(`{"type":"message","role":"user"}`), &inp)
	req := model.StoreRequest{
		Input:  responses.ResponseInputParam{inp},
		Output: []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant"}`)},
		Status: "completed",
		Model:  "test",
	}
	require.NoError(t, svc.Store(context.Background(), "resp_trace2", req))
	rec.Reset()

	_, _, err := svc.Resolve(context.Background(), "resp_trace2")
	require.NoError(t, err)

	spans := rec.Ended()
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name()
	}
	assert.Contains(t, names, "store.Resolve")
}

func TestRetrieveSpan(t *testing.T) {
	svc, rec := newTracingStore(t)
	var inp responses.ResponseInputItemUnionParam
	_ = json.Unmarshal(json.RawMessage(`{"type":"message","role":"user"}`), &inp)
	req := model.StoreRequest{
		Input:  responses.ResponseInputParam{inp},
		Output: []json.RawMessage{json.RawMessage(`{"type":"message","role":"assistant"}`)},
		Status: "completed",
	}
	require.NoError(t, svc.Store(context.Background(), "resp_trace3", req))
	rec.Reset()

	_, _, err := svc.Retrieve(context.Background(), "resp_trace3")
	require.NoError(t, err)

	spans := rec.Ended()
	names := make([]string, len(spans))
	for i, s := range spans {
		names[i] = s.Name()
	}
	assert.Contains(t, names, "store.Retrieve")
}
