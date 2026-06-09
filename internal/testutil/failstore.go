// Package testutil provides error-injecting storage wrappers for disruptive tests.
package testutil

import (
	"context"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// HookPayloadStore wraps a PayloadStore. OnPut, when non-nil, replaces Put entirely —
// giving the test full control (fail, block, delegate to the wrapped store, etc.).
type HookPayloadStore struct {
	storage.PayloadStore
	OnPut func(ctx context.Context, key string, data []byte) error
}

func (h *HookPayloadStore) Put(ctx context.Context, key string, data []byte) error {
	if h.OnPut != nil {
		return h.OnPut(ctx, key, data)
	}
	return h.PayloadStore.Put(ctx, key, data)
}

// HookIndexStore wraps an IndexStore. OnPut and OnUpdateWriteIntent replace the
// corresponding methods when non-nil.
type HookIndexStore struct {
	storage.IndexStore
	OnPut               func(ctx context.Context, meta model.ResponseMeta) error
	OnUpdateWriteIntent func(ctx context.Context, intentID string, phase model.WriteIntentPhase) error
}

func (h *HookIndexStore) Put(ctx context.Context, meta model.ResponseMeta) error {
	if h.OnPut != nil {
		return h.OnPut(ctx, meta)
	}
	return h.IndexStore.Put(ctx, meta)
}

func (h *HookIndexStore) UpdateWriteIntent(ctx context.Context, intentID string, phase model.WriteIntentPhase) error {
	if h.OnUpdateWriteIntent != nil {
		return h.OnUpdateWriteIntent(ctx, intentID, phase)
	}
	return h.IndexStore.UpdateWriteIntent(ctx, intentID, phase)
}
