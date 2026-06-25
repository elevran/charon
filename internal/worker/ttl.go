package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/storage"
)

// Cleaner periodically deletes responses whose ExpiresAt is in the past,
// and evicts oldest chains when the store approaches its MaxResponses cap.
type Cleaner struct {
	index             storage.IndexStore
	payloads          storage.PayloadStore
	log               *slog.Logger
	interval          time.Duration
	maxResponses      int64
	evictionWatermark int64
}

// NewCleaner creates a Cleaner.
// maxResponses is the cap on total stored responses (0 = unlimited).
// evictionHighWatermark is the fraction of maxResponses at which eviction
// triggers (e.g. 0.9). When maxResponses is 0 the eviction sweep is disabled.
func NewCleaner(index storage.IndexStore, payloads storage.PayloadStore, log *slog.Logger, interval time.Duration) *Cleaner {
	return &Cleaner{index: index, payloads: payloads, log: log, interval: interval}
}

// NewCleanerWithEviction creates a Cleaner with capacity-based eviction enabled.
func NewCleanerWithEviction(index storage.IndexStore, payloads storage.PayloadStore, log *slog.Logger, interval time.Duration, maxResponses int64, evictionHighWatermark float64) *Cleaner {
	c := NewCleaner(index, payloads, log, interval)
	c.maxResponses = maxResponses
	if maxResponses > 0 && evictionHighWatermark > 0 {
		c.evictionWatermark = int64(float64(maxResponses) * evictionHighWatermark)
	}
	return c
}

// Run loops until ctx is cancelled. On each tick it deletes expired records
// and evicts old chains if the store is over its high watermark.
func (w *Cleaner) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sweep(ctx)
			w.evict(ctx)
		}
	}
}

func (w *Cleaner) sweep(ctx context.Context) {
	start := time.Now()
	defer func() { metrics.WorkerSweepDuration.WithLabelValues("cleaner").Observe(time.Since(start).Seconds()) }()

	expired, err := w.index.ListExpired(ctx, time.Now().Unix())
	if err != nil {
		w.log.Error("ttl: list expired", "err", err)
		return
	}
	for _, meta := range expired {
		if meta.PayloadKey != "" {
			if err := w.payloads.Delete(ctx, meta.PayloadKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
				w.log.Error("ttl: delete payload", "id", meta.ID, "err", err)
				continue // payload blob still exists; skip index delete to keep the record findable
			}
		}
		if meta.CheckpointKey != nil {
			if err := w.payloads.Delete(ctx, *meta.CheckpointKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
				w.log.Error("ttl: delete checkpoint", "id", meta.ID, "err", err)
				continue // checkpoint blob still exists; skip index delete
			}
		}
		if err := w.index.Delete(ctx, meta.ID); err != nil {
			w.log.Error("ttl: delete index", "id", meta.ID, "err", err)
		} else {
			metrics.TTLExpirationsTotal.Inc()
		}
	}
}

// EvictOnce runs a single eviction sweep. Exposed for testing.
func (w *Cleaner) EvictOnce(ctx context.Context) {
	w.evict(ctx)
}

// evict performs capacity-based eviction: deletes the oldest whole chains
// until the store count drops below 80% of maxResponses (to avoid thrashing).
// It is a no-op when maxResponses == 0 or evictionWatermark == 0.
func (w *Cleaner) evict(ctx context.Context) {
	if w.maxResponses == 0 || w.evictionWatermark == 0 {
		return
	}

	count, err := w.index.Count(ctx)
	if err != nil {
		w.log.Error("eviction: count", "err", err)
		return
	}
	if count < w.evictionWatermark {
		return
	}

	// Target: evict down to 80% of maxResponses.
	targetCount := int64(float64(w.maxResponses) * 0.8)

	// Fetch a batch of the oldest entries. We ask for more than strictly
	// needed so we can group full chains without a second query in the
	// common case.
	batchSize := int(count - targetCount)
	if batchSize < 1 {
		batchSize = 1
	}
	// Fetch extra to cover whole chains that straddle the boundary.
	batchSize += int(w.maxResponses / 10)

	entries, err := w.index.ListOldest(ctx, batchSize)
	if err != nil {
		w.log.Error("eviction: list oldest", "err", err)
		return
	}

	// Group entries by chain root, preserving oldest-first order.
	type metaEntry struct {
		PayloadKey    string
		CheckpointKey *string
		ID            string
	}
	chainMap := make(map[string][]metaEntry)
	var chainOrder []string
	seen := make(map[string]bool)
	for _, e := range entries {
		me := metaEntry{PayloadKey: e.PayloadKey, CheckpointKey: e.CheckpointKey, ID: e.ID}
		if !seen[e.ChainRootID] {
			seen[e.ChainRootID] = true
			chainOrder = append(chainOrder, e.ChainRootID)
		}
		chainMap[e.ChainRootID] = append(chainMap[e.ChainRootID], me)
	}

	currentCount := count
	for _, rootID := range chainOrder {
		if currentCount <= targetCount {
			break
		}
		entries := chainMap[rootID]
		evicted := 0
		for _, e := range entries {
			if e.PayloadKey != "" {
				if err := w.payloads.Delete(ctx, e.PayloadKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
					w.log.Error("eviction: delete payload", "id", e.ID, "err", err)
					continue
				}
			}
			if e.CheckpointKey != nil {
				if err := w.payloads.Delete(ctx, *e.CheckpointKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
					w.log.Error("eviction: delete checkpoint", "id", e.ID, "err", err)
					continue
				}
			}
			if err := w.index.Delete(ctx, e.ID); err != nil {
				w.log.Error("eviction: delete index", "id", e.ID, "err", err)
				continue
			}
			metrics.EvictionsTotal.Inc()
			evicted++
			currentCount--
		}
		if evicted > 0 {
			w.log.Info("eviction: deleted chain", "chain_root", rootID, "entries", evicted)
		}
	}
}
