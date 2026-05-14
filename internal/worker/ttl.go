package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/elevran/charon/internal/storage"
)

// Cleaner periodically deletes responses whose ExpiresAt is in the past.
type Cleaner struct {
	index    storage.IndexStore
	payloads storage.PayloadStore
	log      *slog.Logger
	interval time.Duration
}

// NewCleaner creates a Cleaner.
func NewCleaner(index storage.IndexStore, payloads storage.PayloadStore, log *slog.Logger, interval time.Duration) *Cleaner {
	return &Cleaner{index: index, payloads: payloads, log: log, interval: interval}
}

// Run loops until ctx is cancelled. On each tick it deletes expired records.
func (w *Cleaner) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

func (w *Cleaner) sweep(ctx context.Context) {
	expired, err := w.index.ListExpired(ctx, time.Now().Unix())
	if err != nil {
		w.log.Error("ttl: list expired", "err", err)
		return
	}
	for _, meta := range expired {
		if meta.PayloadKey != "" {
			if err := w.payloads.Delete(ctx, meta.PayloadKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
				w.log.Error("ttl: delete payload", "id", meta.ID, "err", err)
			}
		}
		if meta.CheckpointKey != nil {
			if err := w.payloads.Delete(ctx, *meta.CheckpointKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
				w.log.Error("ttl: delete checkpoint", "id", meta.ID, "err", err)
			}
		}
		if err := w.index.Delete(ctx, meta.ID); err != nil {
			w.log.Error("ttl: delete index", "id", meta.ID, "err", err)
		}
	}
}
