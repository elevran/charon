package store

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// buildContext walks the chain from prevID back to the nearest checkpoint (or root),
// then assembles the flat []json.RawMessage context in chronological order.
//
// maxDepth caps the backward walk (returns ErrChainTooDeep when exceeded).
// maxBytes caps the assembled context size in bytes (returns ErrContextTooLarge when exceeded).
// Pass maxBytes=0 to skip the size check.
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
func (s *ContextStore) buildContext(ctx context.Context, prevID string, maxDepth int, maxBytes int64) ([]json.RawMessage, error) {
	var chain []model.ResponseMeta
	currentID := prevID

	for {
		meta, err := s.index.Get(ctx, currentID)
		if err != nil {
			return nil, storage.ErrChainCorrupted
		}
		chain = append(chain, meta)

		if len(chain) > maxDepth {
			return nil, storage.ErrChainTooDeep
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

	var (
		flatContext []json.RawMessage
		totalBytes  int64
	)

	appendItems := func(items []json.RawMessage) error {
		for _, item := range items {
			totalBytes += int64(len(item))
			if maxBytes > 0 && totalBytes > maxBytes {
				return storage.ErrContextTooLarge
			}
			flatContext = append(flatContext, item)
		}
		return nil
	}

	for i, meta := range chain {
		if i == 0 && meta.CheckpointKey != nil {
			data, err := s.payloads.Get(ctx, *meta.CheckpointKey)
			if err != nil {
				return nil, storage.ErrChainCorrupted
			}
			ckItems, err := parseCheckpoint(data)
			if err != nil {
				return nil, fmt.Errorf("parse checkpoint: %w", err)
			}
			if err := appendItems(ckItems); err != nil {
				return nil, err
			}
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
		if err := appendItems(payload.InputItems); err != nil {
			return nil, err
		}
		if err := appendItems(payload.OutputItems); err != nil {
			return nil, err
		}
	}

	return flatContext, nil
}

// parseCheckpoint reads a checkpoint blob written by marshalNDJSON (ndjson) or,
// for backward compatibility, a legacy JSON array written by json.Marshal.
// The ndjson path copies lines directly without re-validating JSON — safe
// because items were validated on ingestion.
func parseCheckpoint(data []byte) ([]json.RawMessage, error) {
	if len(data) == 0 {
		return nil, nil
	}
	// Legacy format: starts with '['.
	if data[0] == '[' {
		var items []json.RawMessage
		if err := json.Unmarshal(data, &items); err != nil {
			return nil, err
		}
		return items, nil
	}
	// ndjson format: one JSON value per line, no re-validation needed.
	scanner := bufio.NewScanner(bytes.NewReader(data))
	scanner.Buffer(make([]byte, 4096), 1<<20) // grow up to 1 MB if a single item is large
	var items []json.RawMessage
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		cp := make([]byte, len(line))
		copy(cp, line)
		items = append(items, json.RawMessage(cp))
	}
	return items, scanner.Err()
}
