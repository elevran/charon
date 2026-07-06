package chainstore

import "context"

// NodeIDFor exposes the internal nodeID derivation for use in tests that need
// to inspect backend state (e.g. calling backend.GetNode to verify fields).
var NodeIDFor = nodeID

// TtlReap exposes the internal ttlReap method for testing.
func (s *Store) TtlReap(ctx context.Context) { s.ttlReap(ctx) }

// EvictOldest exposes the internal evictOldest method for testing.
func (s *Store) EvictOldest(ctx context.Context) { s.evictOldest(ctx) }

// NudgesFired returns the number of successful nudge sends since the store was opened.
func (s *Store) NudgesFired() int64 { return s.nudgeCount.Load() }

func (s *Store) ReapStaging(ctx context.Context) { s.reapStaging(ctx) }

// CacheStats returns the cache hit, miss, used-bytes, and entry-count snapshot
// (for tests). Returns zeros when caching is disabled.
func (s *Store) CacheStats() (hits, misses int64, bytes int64, entries int) {
	h, m, _, b, e := s.cache.stats()
	return h, m, b, e
}
