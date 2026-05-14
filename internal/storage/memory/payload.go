package memory

import (
	"context"
	"sync"

	"github.com/elevran/charon/internal/storage"
)

var _ storage.PayloadStore = (*PayloadStore)(nil)

type PayloadStore struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func NewPayloadStore() *PayloadStore {
	return &PayloadStore{data: make(map[string][]byte)}
}

func (s *PayloadStore) Put(_ context.Context, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp := make([]byte, len(data))
	copy(cp, data)
	s.data[key] = cp
	return nil
}

func (s *PayloadStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, ok := s.data[key]
	if !ok {
		return nil, storage.ErrNotFound
	}
	cp := make([]byte, len(d))
	copy(cp, d)
	return cp, nil
}

func (s *PayloadStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)
	return nil
}
