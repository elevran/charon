package storage

import (
	"context"
	"time"

	"github.com/elevran/charon/internal/model"
)

// ListOptions controls filtering and pagination for IndexStore.List.
type ListOptions struct {
	Owner  string
	Cursor string
	Limit  int
}

// IndexStore manages response metadata and write-intent tracking.
type IndexStore interface {
	// Response metadata
	Put(ctx context.Context, meta model.ResponseMeta) error
	Get(ctx context.Context, id string) (model.ResponseMeta, error)
	// Delete removes the record. Returns nil if the record did not exist (idempotent).
	Delete(ctx context.Context, id string) error
	List(ctx context.Context, opts ListOptions) ([]model.ResponseMeta, error)

	// Write-intent tracking
	InsertWriteIntent(ctx context.Context, intent model.WriteIntent) error
	UpdateWriteIntent(ctx context.Context, intentID string, phase model.WriteIntentPhase) error
	ListStaleWriteIntents(ctx context.Context, olderThan time.Duration) ([]model.WriteIntent, error)
	DeleteWriteIntent(ctx context.Context, intentID string) error

	// TTL
	ListExpired(ctx context.Context, before int64) ([]model.ResponseMeta, error)

	// Count returns the total number of response records. Used to enforce MaxResponses caps.
	Count(ctx context.Context) (int64, error)

	// ListOldest returns up to limit response records ordered by CreatedAt ascending.
	// Used by the capacity-based eviction sweep to find chains to evict first.
	ListOldest(ctx context.Context, limit int) ([]model.ResponseMeta, error)
}

// PayloadStore manages serialised response content blobs.
// Get returns ErrNotFound if the key does not exist.
type PayloadStore interface {
	Put(ctx context.Context, key string, data []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}
