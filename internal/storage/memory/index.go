package memory

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/elevran/charon/internal/model"
	"github.com/elevran/charon/internal/storage"
)

var _ storage.IndexStore = (*IndexStore)(nil)

type IndexStore struct {
	mu      sync.RWMutex
	records map[string]model.ResponseMeta
	intents map[string]model.WriteIntent
}

func NewIndexStore() *IndexStore {
	return &IndexStore{
		records: make(map[string]model.ResponseMeta),
		intents: make(map[string]model.WriteIntent),
	}
}

func (s *IndexStore) Put(_ context.Context, meta model.ResponseMeta) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[meta.ID] = meta
	return nil
}

func (s *IndexStore) Get(_ context.Context, id string) (model.ResponseMeta, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.records[id]
	if !ok {
		return model.ResponseMeta{}, storage.ErrNotFound
	}
	return m, nil
}

func (s *IndexStore) Delete(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.records, id)
	return nil
}

func (s *IndexStore) List(_ context.Context, opts storage.ListOptions) ([]model.ResponseMeta, error) {
	s.mu.RLock()
	var results []model.ResponseMeta
	for _, m := range s.records {
		if opts.Owner != "" && m.OwnerPrincipal != opts.Owner {
			continue
		}
		results = append(results, m)
	}
	s.mu.RUnlock()

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].CreatedAt != results[j].CreatedAt {
			return results[i].CreatedAt < results[j].CreatedAt
		}
		return results[i].ID < results[j].ID
	})

	if opts.Cursor != "" {
		found := false
		for i, m := range results {
			if m.ID == opts.Cursor {
				results = results[i+1:]
				found = true
				break
			}
		}
		if !found {
			return []model.ResponseMeta{}, nil
		}
	}

	if opts.Limit > 0 && len(results) > opts.Limit {
		results = results[:opts.Limit]
	}

	return results, nil
}

func (s *IndexStore) InsertWriteIntent(_ context.Context, intent model.WriteIntent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.intents[intent.IntentID]; exists {
		return storage.ErrAlreadyExists
	}
	s.intents[intent.IntentID] = intent
	return nil
}

func (s *IndexStore) UpdateWriteIntent(_ context.Context, intentID string, phase model.WriteIntentPhase) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	intent, ok := s.intents[intentID]
	if !ok {
		return storage.ErrNotFound
	}
	intent.Phase = phase
	intent.UpdatedAt = time.Now().Unix()
	s.intents[intentID] = intent
	return nil
}

func (s *IndexStore) ListStaleWriteIntents(_ context.Context, olderThan time.Duration) ([]model.WriteIntent, error) {
	s.mu.RLock()
	threshold := time.Now().Add(-olderThan).Unix()
	var stale []model.WriteIntent
	for _, intent := range s.intents {
		if intent.Phase == model.WriteIntentCommitted || intent.Phase == model.WriteIntentFailed {
			continue
		}
		if intent.UpdatedAt < threshold {
			stale = append(stale, intent)
		}
	}
	s.mu.RUnlock()
	return stale, nil
}

func (s *IndexStore) DeleteWriteIntent(_ context.Context, intentID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.intents, intentID)
	return nil
}

func (s *IndexStore) ListExpired(_ context.Context, before int64) ([]model.ResponseMeta, error) {
	s.mu.RLock()
	var expired []model.ResponseMeta
	for _, m := range s.records {
		if m.ExpiresAt != nil && *m.ExpiresAt < before {
			expired = append(expired, m)
		}
	}
	s.mu.RUnlock()
	return expired, nil
}

func (s *IndexStore) Count(_ context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return int64(len(s.records)), nil
}
