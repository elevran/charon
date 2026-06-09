package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// Reconciler periodically finds stale write intents and attempts to
// complete or fail them.
type Reconciler struct {
	index    storage.IndexStore
	payloads storage.PayloadStore
	log      *slog.Logger
	stale    time.Duration
	interval time.Duration
}

// NewReconciler creates a Reconciler.
func NewReconciler(index storage.IndexStore, payloads storage.PayloadStore, log *slog.Logger, stale, interval time.Duration) *Reconciler {
	return &Reconciler{index: index, payloads: payloads, log: log, stale: stale, interval: interval}
}

// Run loops until ctx is cancelled. It runs an immediate sweep on startup to
// recover intents that went stale during a prior crash.
func (w *Reconciler) Run(ctx context.Context) {
	w.sweep(ctx)
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

// RunOnce performs a single recovery sweep and returns.
// Useful in tests and for the charon reconcile subcommand.
func (w *Reconciler) RunOnce(ctx context.Context) {
	w.sweep(ctx)
}

func (w *Reconciler) sweep(ctx context.Context) {
	intents, err := w.index.ListStaleWriteIntents(ctx, w.stale)
	if err != nil {
		w.log.Error("recovery: list stale intents", "err", err)
		return
	}
	for _, intent := range intents {
		w.recover(ctx, intent)
	}
}

func (w *Reconciler) recover(ctx context.Context, intent model.WriteIntent) {
	switch intent.Phase {
	case model.WriteIntentCommitted, model.WriteIntentFailed:
		// Already in a terminal state; ListStaleWriteIntents should not return these,
		// but guard against any race with the Reconciler's own UpdateWriteIntent calls.

	case model.WriteIntentStreamOpen:
		// Proxy crashed mid-stream; in-memory staged chunks are lost.
		w.log.Warn("recovery: stream_open intent lost (proxy crashed mid-stream)", "response_id", intent.ResponseID)
		_ = w.index.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentFailed)
		metrics.WriteIntentFailuresTotal.Inc()

	case model.WriteIntentPending:
		w.log.Warn("recovery: pending intent lost", "response_id", intent.ResponseID)
		_ = w.index.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentFailed)
		metrics.WriteIntentFailuresTotal.Inc()

	case model.WriteIntentFileWritten:
		data, err := w.payloads.Get(ctx, intent.PayloadKey)
		if errors.Is(err, storage.ErrNotFound) {
			w.log.Warn("recovery: file_written intent but payload missing", "response_id", intent.ResponseID)
			_ = w.index.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentFailed)
			metrics.WriteIntentFailuresTotal.Inc()
			return
		}
		if err != nil {
			w.log.Error("recovery: get payload", "response_id", intent.ResponseID, "err", err)
			return
		}
		var payload model.ResponsePayload
		if err := json.Unmarshal(data, &payload); err != nil {
			w.log.Error("recovery: unmarshal payload", "response_id", intent.ResponseID, "err", err)
			_ = w.index.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentFailed)
			metrics.WriteIntentFailuresTotal.Inc()
			return
		}
		chainRootID, position, err := parsePayloadKey(intent.PayloadKey)
		if err != nil {
			w.log.Error("recovery: parse payload key", "response_id", intent.ResponseID, "err", err)
			_ = w.index.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentFailed)
			metrics.WriteIntentFailuresTotal.Inc()
			return
		}
		meta := model.ResponseMeta{
			ID:                 payload.ID,
			PreviousResponseID: payload.PreviousResponseID,
			ChainRootID:        chainRootID,
			Position:           position,
			PayloadKey:         intent.PayloadKey,
			Status:             model.StatusCompleted,
			CreatedAt:          intent.CreatedAt,
		}
		if err := w.index.Put(ctx, meta); err != nil {
			w.log.Error("recovery: commit index", "response_id", intent.ResponseID, "err", err)
			return
		}
		_ = w.index.UpdateWriteIntent(ctx, intent.IntentID, model.WriteIntentCommitted)
		w.log.Info("recovery: committed file_written intent", "response_id", intent.ResponseID)
	}
}

// parsePayloadKey extracts ChainRootID and Position from a payload key.
// Key format: chainRootID/XXXXXXXX_responseID.json
func parsePayloadKey(key string) (chainRootID string, position int, err error) {
	parts := strings.SplitN(key, "/", 2)
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid payload key: %q", key)
	}
	chainRootID = parts[0]
	if len(parts[1]) < 8 {
		return "", 0, fmt.Errorf("invalid payload key filename: %q", parts[1])
	}
	pos64, parseErr := strconv.ParseInt(parts[1][:8], 10, 64)
	if parseErr != nil {
		return "", 0, fmt.Errorf("parse position in payload key: %w", parseErr)
	}
	return chainRootID, int(pos64), nil
}
