package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/openai/openai-go/responses"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/elevran/charon/internal/metrics"
	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

// Config holds store-level configuration.
type Config struct {
	CheckpointInterval int                  // create checkpoint every N turns; default 10
	TTLDays            int                  // response TTL; default 30
	MaxResponses       int64                // max total responses in index; 0 = unbounded
	MaxPayloadBytes    int64                // max size of a single payload blob in bytes; 0 = unbounded
	TracerProvider     trace.TracerProvider // nil = use otel.GetTracerProvider() (no-op when not configured)
}

func (c *Config) applyDefaults() {
	if c.CheckpointInterval <= 0 {
		c.CheckpointInterval = 10
	}
	if c.TTLDays <= 0 {
		c.TTLDays = 30
	}
}

// seqChunk is one sequenced batch of output items from a single AppendChunk call.
type seqChunk struct {
	seq   int
	items []json.RawMessage
}

// streamStage holds in-memory state for a streaming store in progress.
// Chunks are indexed by their sequence number so concurrent PATCH requests
// can arrive out of order and still be reassembled correctly at commit time.
type streamStage struct {
	intentID string
	chunks   []seqChunk // unsorted; sorted by seq at commit
	mu       sync.Mutex // per-stage lock for concurrent AppendChunk calls
}

// ContextStore owns all business logic: chain construction,
// checkpoint decisions, write-intent sequencing, and ID minting.
type ContextStore struct {
	index    storage.IndexStore
	payloads storage.PayloadStore
	cfg      Config
	log      *slog.Logger
	tracer   trace.Tracer

	mu      sync.Mutex
	streams map[string]*streamStage // key: canonical response ID
}

// New creates a ContextStore.
func New(index storage.IndexStore, payloads storage.PayloadStore, cfg Config, log *slog.Logger) *ContextStore {
	cfg.applyDefaults()
	tp := cfg.TracerProvider
	if tp == nil {
		tp = otel.GetTracerProvider()
	}
	return &ContextStore{
		index:    index,
		payloads: payloads,
		cfg:      cfg,
		log:      log,
		tracer:   tp.Tracer("charon/store"),
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

// Ping performs a lightweight read against the index to verify storage is reachable.
// Returns nil when the store is healthy.
func (s *ContextStore) Ping(ctx context.Context) error {
	_, err := s.index.List(ctx, storage.ListOptions{Limit: 1})
	return err
}

// computeExpiresAt returns the expiry timestamp based on TTLDays config.
func (s *ContextStore) computeExpiresAt() *int64 {
	exp := time.Now().AddDate(0, 0, s.cfg.TTLDays).Unix()
	return &exp
}

// Resolve assembles flat_context from previousID and mints a new responseID.
func (s *ContextStore) Resolve(ctx context.Context, previousID string) (string, []json.RawMessage, error) {
	ctx, span := s.tracer.Start(ctx, "store.Resolve",
		trace.WithAttributes(attribute.String("response.id", previousID)))
	defer span.End()

	if _, err := s.index.Get(ctx, previousID); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", nil, err
	}

	flatContext, err := s.buildContext(ctx, previousID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", nil, err
	}

	span.SetAttributes(attribute.Int("flat_context.item_count", len(flatContext)))
	reservationID := mintID("rsrv")
	return reservationID, flatContext, nil
}

// Store commits a completed inference response using the two-phase write-intent protocol.
func (s *ContextStore) Store(ctx context.Context, responseID string, req model.StoreRequest) (retErr error) {
	ctx, span := s.tracer.Start(ctx, "store.Store",
		trace.WithAttributes(attribute.String("response.id", responseID)))
	defer func() {
		if retErr != nil {
			span.RecordError(retErr)
			span.SetStatus(codes.Error, retErr.Error())
		}
		span.End()
	}()

	// Enforce MaxResponses cap before accepting new entries.
	if s.cfg.MaxResponses > 0 {
		n, err := s.index.Count(ctx)
		if err != nil {
			return fmt.Errorf("count index: %w", err)
		}
		if n >= s.cfg.MaxResponses {
			return fmt.Errorf("%w: index holds %d of %d responses", storage.ErrStoreFull, n, s.cfg.MaxResponses)
		}
	}

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
		Instructions:       req.Instructions,
		InputItems:         rawInput,
		OutputItems:        req.Output,
		Usage:              usageRaw,
	}
	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}
	// Enforce MaxPayloadBytes cap after marshaling so we know the actual size.
	if s.cfg.MaxPayloadBytes > 0 && int64(len(payloadBytes)) > s.cfg.MaxPayloadBytes {
		return fmt.Errorf("%w: payload %d bytes exceeds limit %d", storage.ErrStoreFull, len(payloadBytes), s.cfg.MaxPayloadBytes)
	}

	isCheckpoint := position > 0 && position%s.cfg.CheckpointInterval == 0
	var ckKey *string

	if isCheckpoint {
		flatCtx := make([]json.RawMessage, 0, len(rawInput)+len(req.Output))
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
		metrics.CheckpointWritesTotal.Inc()
		metrics.CheckpointSizeBytes.Observe(float64(len(ckBytes)))
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
	ctx, span := s.tracer.Start(ctx, "store.Retrieve",
		trace.WithAttributes(attribute.String("response.id", responseID)))
	defer span.End()

	meta, err := s.index.Get(ctx, responseID)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
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

// AppendChunk adds a sequenced batch of output items to the in-memory stream
// stage for responseID. seq is a 0-based sequence number assigned by the
// caller before the request is sent; concurrent calls with different seq
// values may arrive in any order.
//
// On the first call a write-intent is created at WriteIntentStreamOpen.
// Items accumulate in memory until CommitStream is called.
func (s *ContextStore) AppendChunk(ctx context.Context, responseID string, seq int, items []json.RawMessage) error {
	if len(items) == 0 {
		return nil
	}

	// Ensure the stage exists (create write-intent on first call).
	s.mu.Lock()
	stage, exists := s.streams[responseID]
	if !exists {
		intentID := mintID("intent")
		now := time.Now().Unix()
		intent := model.WriteIntent{
			IntentID:   intentID,
			ResponseID: responseID,
			PayloadKey: "",
			Phase:      model.WriteIntentStreamOpen,
			CreatedAt:  now,
			UpdatedAt:  now,
		}
		if err := s.index.InsertWriteIntent(ctx, intent); err != nil {
			s.mu.Unlock()
			return fmt.Errorf("insert stream intent: %w", err)
		}
		stage = &streamStage{intentID: intentID}
		s.streams[responseID] = stage
	}
	s.mu.Unlock()

	// Append this batch to the per-stage chunk list (separate lock allows
	// concurrent appends to different stages, and concurrent appends to the
	// same stage from parallel PATCH goroutines).
	stage.mu.Lock()
	stage.chunks = append(stage.chunks, seqChunk{seq: seq, items: items})
	stage.mu.Unlock()
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

	// Assemble all output items: sort staged chunks by seq, then append the
	// final batch at req.Seq (which equals the number of preceding batches).
	var allOutput []json.RawMessage
	if stage != nil {
		stage.mu.Lock()
		sort.Slice(stage.chunks, func(i, j int) bool {
			return stage.chunks[i].seq < stage.chunks[j].seq
		})
		for _, c := range stage.chunks {
			allOutput = append(allOutput, c.items...)
		}
		stage.mu.Unlock()
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
		Instructions:       req.Instructions,
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
		flatCtx2 := make([]json.RawMessage, 0, len(rawInput)+len(allOutput))
		if req.PreviousResponseID != nil {
			flatCtx2, err = s.buildContext(ctx, *req.PreviousResponseID)
			if err != nil {
				return fmt.Errorf("build checkpoint context: %w", err)
			}
		}
		flatCtx2 = append(flatCtx2, rawInput...)
		flatCtx2 = append(flatCtx2, allOutput...)
		ckBytes := marshalNDJSON(flatCtx2)
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
