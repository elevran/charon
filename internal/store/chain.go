package store

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

const maxChainDepth = 1000

// buildContext walks the chain from prevID back to the nearest checkpoint (or root),
// then assembles the flat []json.RawMessage context in chronological order.
//
// Walk algorithm:
//  1. Walk backward collecting ResponseMeta records until a checkpoint or chain root.
//  2. If checkpoint found: load the pre-built checkpoint blob as the base context.
//  3. Append delta items (positions after checkpoint, in forward order).
//
// Storage note: each checkpoint stores the full context from the chain root up to
// that position. This means total checkpoint storage grows as O(N^2 / interval)
// where N is chain length. The tradeoff is O(1) reads at resolve time from any
// checkpoint. For the in-memory backend this is acceptable. For persistent backends
// consider incremental checkpoints (each referencing the prior one) which reduce
// storage to O(N) at the cost of O(log N) reads at resolve time.
//
// Returns ErrChainCorrupted if any payload key resolves to ErrNotFound.
func (s *ContextStore) buildContext(ctx context.Context, prevID string) ([]json.RawMessage, error) {
	var chain []model.ResponseMeta
	currentID := prevID

	for {
		meta, err := s.index.Get(ctx, currentID)
		if err != nil {
			return nil, storage.ErrChainCorrupted
		}
		chain = append(chain, meta)

		if len(chain) > maxChainDepth {
			return nil, storage.ErrChainCorrupted
		}

		if meta.CheckpointKey != nil {
			break
		}
		if meta.PreviousResponseID == nil {
			break
		}
		currentID = *meta.PreviousResponseID
	}

	// Reverse so we iterate from oldest to newest.
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}

	var flatContext []json.RawMessage

	for i, meta := range chain {
		if i == 0 && meta.CheckpointKey != nil {
			data, err := s.payloads.Get(ctx, *meta.CheckpointKey)
			if err != nil {
				return nil, storage.ErrChainCorrupted
			}
			var ckItems []json.RawMessage
			if err := json.Unmarshal(data, &ckItems); err != nil {
				return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
			}
			flatContext = append(flatContext, ckItems...)
			continue
		}

		data, err := s.payloads.Get(ctx, meta.PayloadKey)
		if err != nil {
			return nil, storage.ErrChainCorrupted
		}
		var payload model.ResponsePayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, fmt.Errorf("unmarshal payload: %w", err)
		}
		flatContext = append(flatContext, payload.InputItems...)
		flatContext = append(flatContext, payload.OutputItems...)
	}

	return flatContext, nil
}
