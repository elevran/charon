package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/openai/openai-go/responses"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// Config holds store-level configuration.
type Config struct {
	CheckpointInterval int // create checkpoint every N turns; default 10
	TTLDays            int // response TTL; default 30
}

func (c *Config) applyDefaults() {
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = 10
	}
	if c.TTLDays <= 0 {
		c.TTLDays = 30
	}
}

// streamStage holds in-memory state for a streaming store in progress.
type streamStage struct {
	intentID           string
	reservationID      string
	previousResponseID *string
	chunks             []json.RawMessage // accumulated output items from AppendChunk calls
}

// ContextStore owns all business logic: chain construction,
// checkpoint decisions, write-intent sequencing, and ID minting.
type ContextStore struct {
	index    storage.IndexStore
	payloads storage.PayloadStore
	cfg      Config
	log      *slog.Logger

	mu      sync.Mutex
	streams map[string]*streamStage // key: canonical response ID
}

// New creates a ContextStore.
func New(index storage.IndexStore, payloads storage.PayloadStore, cfg Config, log *slog.Logger) *ContextStore {
	cfg.applyDefaults()
	return &ContextStore{
		index:    index,
		payloads: payloads,
		cfg:      cfg,
		log:      log,
		streams:  make(map[string]*streamStage),
	}
}

// resolveChainPosition derives ChainRootID and Position for a new response.
// newID is only used when prevID is nil (new chain root).
func (s *ContextStore) resolveChainPosition(ctx context.Context, prevID *string, newID string) (chainRootID string, position int, err error) {
	if prevID == nil {
		return newID, 0, nil
	}
	prevMeta, err := s.index.Get(ctx, *prevID)
	if err != nil {
		return "", 0, fmt.Errorf("previous response: %w", err)
	}
	return prevMeta.ChainRootID, prevMeta.Position + 1, nil
}

// computeExpiresAt returns the expiry timestamp based on TTLDays config.
func (s *ContextStore) computeExpiresAt() *int64 {
	exp := time.Now().AddDate(0, 0, s.cfg.TTLDays).Unix()
	return &exp
}

// Resolve assembles flat_context from previousID and mints a new responseID.
func (s *ContextStore) Resolve(ctx context.Context, previousID string) (string, []json.RawMessage, error) {
	if _, err := s.index.Get(ctx, previousID); err != nil {
		return "", nil, err
	}

	flatContext, err := s.buildContext(ctx, previousID)
	if err != nil {
		return "", nil, err
	}

	reservationID := mintID("rsrv")
	return reservationID, flatContext, nil
}

// Store commits a completed inference response using the two-phase write-intent protocol.
func (s *ContextStore) Store(ctx context.Context, responseID string, req model.StoreRequest) error {
	chainRootID, position, err := s.resolveChainPosition(ctx, req.PreviousResponseID, responseID)
	if err != nil {
		return err
	}

	// Failed responses: record without payload write.
	if req.Status == responses.ResponseStatusFailed {
		meta := model.ResponseMeta{
			ID:                 responseID,
			PreviousResponseID: req.PreviousResponseID,
			ChainRootID:        chainRootID,
			Position:           position,
			Status:             model.StatusFailed,
			Model:              req.Model,
			CreatedAt:          time.Now().Unix(),
			ExpiresAt:          s.computeExpiresAt(),
		}
		return s.index.Put(ctx, meta)
	}

	pKey := payloadKey(chainRootID, position, responseID)
	intentID := mintID("intent")
	now := time.Now().Unix()

	intent := model.WriteIntent{
		IntentID:      intentID,
		ResponseID:    responseID,
		ReservationID: req.ReservationID,
		PayloadKey:    pKey,
		Phase:         model.WriteIntentPending,
		CreatedAt:     now,
		UpdatedAt:     now,
	}

	if err := s.index.InsertWriteIntent(ctx, intent); err != nil {
		return fmt.Errorf("insert write intent: %w", err)
	}

	rawInput, err := marshalInputItems(req.Input)
	if err != nil {
		return fmt.Errorf("marshal input items: %w", err)
	}

	usageRaw := req.Usage // already json.RawMessage; nil/empty if not provided

	payload := model.ResponsePayload{
		ID:                 responseID,
		PreviousResponseID: req.PreviousResponseID,
		InputItems:         rawInput,
		OutputItems:        req.Output,
		Usage:              usageRaw,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	isCheckpoint := position > 0 && position%s.cfg.CheckpointInterval == 0
	var ckKey *string

	if isCheckpoint {
		var flatCtx []json.RawMessage
		if req.PreviousResponseID != nil {
			flatCtx, err = s.buildContext(ctx, *req.PreviousResponseID)
			if err != nil {
				return fmt.Errorf("build checkpoint context: %w", err)
			}
		}
		flatCtx = append(flatCtx, rawInput...)
		flatCtx = append(flatCtx, req.Output...)

		ckBytes := marshalNDJSON(flatCtx)
		ck := checkpointKey(chainRootID, position, responseID)
		ckKey = &ck
		if err := s.payloads.Put(ctx, ck, ckBytes); err != nil {
			return fmt.Errorf("write checkpoint: %w", err)
		}
	}

	if err := s.payloads.Put(ctx, pKey, payloadBytes); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	if err := s.index.UpdateWriteIntent(ctx, intentID, model.WriteIntentFileWritten); err != nil {
		return fmt.Errorf("update write intent to file_written: %w", err)
	}

	meta := model.ResponseMeta{
		ID:                 responseID,
		PreviousResponseID: req.PreviousResponseID,
		ChainRootID:        chainRootID,
		Position:           position,
		PayloadKey:         pKey,
		CheckpointKey:      ckKey,
		Status:             model.StatusCompleted,
		Model:              req.Model,
		CreatedAt:          now,
		ExpiresAt:          s.computeExpiresAt(),
	}
	if err := s.index.Put(ctx, meta); err != nil {
		return fmt.Errorf("commit index: %w", err)
	}

	if err := s.index.UpdateWriteIntent(ctx, intentID, model.WriteIntentCommitted); err != nil {
		return fmt.Errorf("update write intent to committed: %w", err)
	}

	return nil
}

// Retrieve fetches a single stored response record by ID.
func (s *ContextStore) Retrieve(ctx context.Context, responseID string) (model.ResponseMeta, model.ResponsePayload, error) {
	meta, err := s.index.Get(ctx, responseID)
	if err != nil {
		return model.ResponseMeta{}, model.ResponsePayload{}, err
	}
	// Failed responses have no payload — return meta only.
	if meta.PayloadKey == "" {
		return meta, model.ResponsePayload{ID: responseID}, nil
	}

	data, err := s.payloads.Get(ctx, meta.PayloadKey)
	if err != nil {
		return model.ResponseMeta{}, model.ResponsePayload{}, err
	}
	var payload model.ResponsePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		return model.ResponseMeta{}, model.ResponsePayload{}, fmt.Errorf("unmarshal payload: %w", err)
	}
	return meta, payload, nil
}

// Delete removes a single response by ID (point delete, no cascade).
func (s *ContextStore) Delete(ctx context.Context, responseID string) error {
	meta, err := s.index.Get(ctx, responseID)
	if err != nil {
		return err
	}
	if err := s.payloads.Delete(ctx, meta.PayloadKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if meta.CheckpointKey != nil {
		if err := s.payloads.Delete(ctx, *meta.CheckpointKey); err != nil && !errors.Is(err, storage.ErrNotFound) {
			return err
		}
	}
	return s.index.Delete(ctx, responseID)
}

// AppendChunk adds output items to the in-memory stream stage for responseID.
// On the first call a write-intent is created at WriteIntentStreamOpen.
// Items accumulate in memory until CommitStream is called.
func (s *ContextStore) AppendChunk(ctx context.Context, responseID string, items []json.RawMessage) error {
	if len(items) == 0 {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	stage, exists := s.streams[responseID]
	if !exists {
		intentID := mintID("intent")
		now := time.Now().Unix()
		intent := model.WriteIntent{
			IntentID:   intentID,
			ResponseID: responseID,
			PayloadKey: "", // not known yet; set at commit time
			Phase:      model.WriteIntentStreamOpen,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := s.index.InsertWriteIntent(ctx, intent); err != nil {
			return fmt.Errorf("insert stream intent: %w", err)
		}
		stage = &streamStage{intentID: intentID}
		s.streams[responseID] = stage
	}
	stage.chunks = append(stage.chunks, items...)
	return nil
}

// CommitStream finalises a streaming store: merges all staged chunks with the
// final batch in req, writes the payload, and commits the index record.
// If no prior AppendChunk calls were made, it behaves like Store() using
// req.Items as the full output.
func (s *ContextStore) CommitStream(ctx context.Context, responseID string, req model.ChunkRequest) error {
	s.mu.Lock()
	stage := s.streams[responseID]
	delete(s.streams, responseID)
	s.mu.Unlock()

	// Assemble all output items: staged chunks + final batch.
	var allOutput []json.RawMessage
	if stage != nil {
		allOutput = append(allOutput, stage.chunks...)
	}
	allOutput = append(allOutput, req.Items...)

	// Derive chain position.
	chainRootID, position, err := s.resolveChainPosition(ctx, req.PreviousResponseID, responseID)
	if err != nil {
		return err
	}

	pKey := payloadKey(chainRootID, position, responseID)
	now := time.Now().Unix()

	// If we have a staged intent (from AppendChunk), advance it.
	// Otherwise create a fresh one — this handles the case where CommitStream
	// is called directly without prior AppendChunk calls.
	var intentID string
	if stage != nil {
		intentID = stage.intentID
		// Update payload_key now that we know it, and advance phase.
		if err := s.index.UpdateWriteIntent(ctx, intentID, model.WriteIntentPending); err != nil {
			return fmt.Errorf("advance stream intent to pending: %w", err)
		}
	} else {
		intentID = mintID("intent")
		intent := model.WriteIntent{
			IntentID:      intentID,
			ResponseID:    responseID,
			ReservationID: req.ReservationID,
			PayloadKey:    pKey,
			Phase:         model.WriteIntentPending,
			CreatedAt:     now,
			UpdatedAt:     now,
		}
		if err := s.index.InsertWriteIntent(ctx, intent); err != nil {
			return fmt.Errorf("insert commit intent: %w", err)
		}
	}

	rawInput, err := inputItemsToRaw(req.Input)
	if err != nil {
		return fmt.Errorf("marshal input items: %w", err)
	}

	payload := model.ResponsePayload{
		ID:                 responseID,
		PreviousResponseID: req.PreviousResponseID,
		InputItems:         rawInput,
		OutputItems:        allOutput,
		Usage:              req.Usage,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	isCheckpoint := position > 0 && position%s.cfg.CheckpointInterval == 0
	var ckKey *string
	if isCheckpoint {
		var flatCtx []json.RawMessage
		if req.PreviousResponseID != nil {
			flatCtx, err = s.buildContext(ctx, *req.PreviousResponseID)
			if err != nil {
				return fmt.Errorf("build checkpoint context: %w", err)
			}
		}
		flatCtx = append(flatCtx, rawInput...)
		flatCtx = append(flatCtx, allOutput...)
		ckBytes := marshalNDJSON(flatCtx)
		ck := checkpointKey(chainRootID, position, responseID)
		ckKey = &ck
		if err := s.payloads.Put(ctx, ck, ckBytes); err != nil {
			return fmt.Errorf("write checkpoint: %w", err)
		}
	}

	if err := s.payloads.Put(ctx, pKey, payloadBytes); err != nil {
		return fmt.Errorf("write payload: %w", err)
	}

	if err := s.index.UpdateWriteIntent(ctx, intentID, model.WriteIntentFileWritten); err != nil {
		return fmt.Errorf("update intent to file_written: %w", err)
	}

	status := model.ResponseStatus(req.Status)
	if status == "" {
		status = model.StatusCompleted
	}
	meta := model.ResponseMeta{
		ID:                 responseID,
		PreviousResponseID: req.PreviousResponseID,
		ChainRootID:        chainRootID,
		Position:           position,
		PayloadKey:         pKey,
		CheckpointKey:      ckKey,
		Status:             status,
		Model:              req.Model,
		CreatedAt:          now,
		ExpiresAt:          s.computeExpiresAt(),
	}
	if err := s.index.Put(ctx, meta); err != nil {
		return fmt.Errorf("commit index: %w", err)
	}

	return s.index.UpdateWriteIntent(ctx, intentID, model.WriteIntentCommitted)
}

// inputItemsToRaw converts []json.RawMessage input items (used by CommitStream)
// into the canonical []json.RawMessage form for payload storage.
func inputItemsToRaw(items []json.RawMessage) ([]json.RawMessage, error) {
	return items, nil
}

func marshalInputItems(items responses.ResponseInputParam) ([]json.RawMessage, error) {
	raw := make([]json.RawMessage, len(items))
	for i, item := range items {
		b, err := json.Marshal(item)
		if err != nil {
			return nil, err
		}
		raw[i] = b
	}
	return raw, nil
}

func mintID(prefix string) string {
	id := uuid.New().String()
	return prefix + "_" + strings.ReplaceAll(id, "-", "")
}

func payloadKey(chainRootID string, position int, responseID string) string {
	return fmt.Sprintf("%s/%08d_%s.json", chainRootID, position, responseID)
}

func checkpointKey(chainRootID string, position int, responseID string) string {
	return fmt.Sprintf("%s/checkpoint_%08d_%s.json", chainRootID, position, responseID)
}

// marshalNDJSON serialises a []json.RawMessage as newline-delimited JSON.
// Each item occupies one line. This avoids the json.checkValid + array-skip
// cost that json.Marshal([]json.RawMessage{…}) pays when the slice is large.
func marshalNDJSON(items []json.RawMessage) []byte {
	var buf bytes.Buffer
	buf.Grow(len(items) * 128)
	for _, item := range items {
		buf.Write(item)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}
