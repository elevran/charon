package memory

import "github.com/elevran/charon/internal/storage"

// Open returns a new in-memory IndexStore and PayloadStore.
// Both stores are empty and independent; no cleanup is required.
func Open() (storage.IndexStore, storage.PayloadStore) {
	return NewIndexStore(), NewPayloadStore()
}
