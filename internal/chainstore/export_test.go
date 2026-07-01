package chainstore

import "context"

// NodeIDFor exposes the internal nodeID derivation for use in tests that need
// to inspect backend state (e.g. calling backend.GetNode to verify fields).
var NodeIDFor = nodeID

// TtlReap exposes the internal ttlReap method for testing.
func (s *Store) TtlReap(ctx context.Context) { s.ttlReap(ctx) }

// EvictOldest exposes the internal evictOldest method for testing.
func (s *Store) EvictOldest(ctx context.Context) { s.evictOldest(ctx) }
