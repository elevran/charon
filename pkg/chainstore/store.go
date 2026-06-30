package chainstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// Clock abstracts time. Injected into Store so tests can simulate days in milliseconds.
type Clock interface {
	Now() time.Time
}

// RealClock is the production implementation.
type RealClock struct{}

func (RealClock) Now() time.Time { return time.Now() }

// Config holds all parameters for a Store.
type Config struct {
	MaxEntries       int64
	MaxBytes         int64
	TTL              time.Duration
	BucketDuration   time.Duration // LRU bucket width; default 1h
	ChunkSize        int           // Phase 6: fixed segment size for StreamStore; default 2MB (Pebble), 256KB (DynamoDB)
	EvictionInterval time.Duration // ticker period for capacity eviction; default 1m
	TTLInterval      time.Duration // ticker period for TTL reaper; default 5m
	Clock            Clock         // nil = RealClock
	Backend          Backend       // required — supply via pebble.Open() or dynamodb.Open()
}

func (c Config) bucketDuration() time.Duration {
	if c.BucketDuration <= 0 {
		return time.Hour
	}
	return c.BucketDuration
}

// BucketFor computes the BucketID for a given time.
func (c Config) BucketFor(t time.Time) BucketID {
	return BucketID(t.Unix() / int64(c.bucketDuration().Seconds()))
}

// Store is the public API for the chainstore. All business logic lives here;
// it delegates to a Backend for all KV operations.
type Store struct {
	cfg     Config
	backend Backend
	clock   Clock

	// In-memory counters (approximate; reloaded on open from backend.Stats()).
	// The persistent copy lives at pfxStats+0x01 as a single MERGE-able record.
	entries atomic.Int64
	bytes   atomic.Int64

	// nudgeEvict is buffered (capacity 1); Store signals eviction after a write
	// that pushes the store over capacity. The eviction goroutine consumes it
	// and runs evictOldest immediately rather than waiting for the next tick.
	nudgeEvict chan struct{}

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// New wires cfg into a Store and starts background goroutines.
// Callers should use pebble.Open() or dynamodb.Open() rather than calling New directly.
func New(cfg Config) (*Store, error) {
	if cfg.Backend == nil {
		return nil, errors.New("chainstore.New: Backend is required")
	}
	if cfg.BucketDuration <= 0 {
		cfg.BucketDuration = time.Hour
	}
	if cfg.EvictionInterval <= 0 {
		cfg.EvictionInterval = time.Minute
	}
	if cfg.TTLInterval <= 0 {
		cfg.TTLInterval = 5 * time.Minute
	}
	if cfg.Clock == nil {
		cfg.Clock = RealClock{}
	}

	s := &Store{
		cfg:        cfg,
		backend:    cfg.Backend,
		clock:      cfg.Clock,
		nudgeEvict: make(chan struct{}, 1),
	}

	// Reload stats from the persistent stats key so in-memory counters survive restarts.
	entries, bytes, err := cfg.Backend.Stats(context.Background())
	if err != nil {
		return nil, fmt.Errorf("chainstore.New: reload stats: %w", err)
	}
	s.entries.Store(entries)
	s.bytes.Store(bytes)

	s.ctx, s.cancel = context.WithCancel(context.Background())
	s.wg.Add(2)
	go s.evictionLoop(s.ctx)
	go s.ttlLoop(s.ctx)
	return s, nil
}

// Close stops background goroutines and releases backend resources.
func (s *Store) Close() error {
	s.cancel()
	s.wg.Wait()
	return s.backend.Close()
}

// notifyCapacityExceeded signals the eviction goroutine to run evictOldest
// outside its normal ticker cadence. The channel is buffered size 1: concurrent
// Store triggers coalesce.
func (s *Store) notifyCapacityExceeded() {
	select {
	case s.nudgeEvict <- struct{}{}:
	default: // already a nudge pending; drop
	}
}

// evictionLoop is the long-running capacity-eviction goroutine.
func (s *Store) evictionLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.EvictionInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.evictOldest(ctx)
		case <-s.nudgeEvict:
			s.evictOldest(ctx)
		}
	}
}

// ttlLoop is the long-running TTL-reaper goroutine.
func (s *Store) ttlLoop(ctx context.Context) {
	defer s.wg.Done()
	ticker := time.NewTicker(s.cfg.TTLInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.ttlReap(ctx)
		}
	}
}

// evictOldest removes nodes from the oldest bucket until store is under limits.
// Optimistic: re-reads Node via GetNode before deleting to guard against
// a concurrent Resolve that promoted the node to a newer bucket.
func (s *Store) evictOldest(ctx context.Context) {
	const batchSize = 100
	for {
		if s.cfg.MaxEntries > 0 && s.entries.Load() <= s.cfg.MaxEntries &&
			s.cfg.MaxBytes > 0 && s.bytes.Load() <= s.cfg.MaxBytes {
			return
		}
		if s.cfg.MaxEntries > 0 && s.entries.Load() > s.cfg.MaxEntries {
			// over entry limit, continue
		} else if s.cfg.MaxBytes > 0 && s.bytes.Load() > s.cfg.MaxBytes {
			// over byte limit, continue
		} else {
			return
		}

		bucket, err := s.backend.OldestBucket(ctx)
		if err != nil {
			return // ErrNotFound = store is empty
		}

		candidates, err := s.backend.GetEvictionCandidates(ctx, bucket, batchSize)
		if err != nil || len(candidates) == 0 {
			return
		}

		for _, nid := range candidates {
			node, err := s.backend.GetNode(ctx, nid)
			if err != nil {
				continue // already deleted
			}
			// Optimistic guard: skip if node was promoted to a newer bucket
			if node.BucketID != bucket {
				continue
			}
			s.deleteNode(ctx, node)
		}
	}
}

// deleteNode removes a single node and its blob atomically.
func (s *Store) deleteNode(ctx context.Context, node Node) {
	tx := Transaction{
		DeleteNodes: []NodeID{node.ID},
		DeleteBlobs: []BlobID{node.BlobID},
		StatsDelta: StatsDelta{
			EntryDelta: -1,
			BytesDelta: -int64(node.BlobSize),
		},
	}
	if node.ParentID != (NodeID{}) {
		tx.DeleteChildren = []NodeID{node.ParentID}
	}
	// Best-effort delete; errors surface on next eviction pass.
	if err := s.backend.Commit(ctx, tx); err == nil {
		s.entries.Add(-1)
		s.bytes.Add(-int64(node.BlobSize))
	}
}

// ttlReap removes expired nodes by walking buckets oldest-first.
func (s *Store) ttlReap(ctx context.Context) {
	if s.cfg.TTL <= 0 {
		return
	}
	cutoff := s.clock.Now().Add(-s.cfg.TTL).Unix()
	dur := int64(s.cfg.bucketDuration().Seconds())
	const batchSize = 100
	const maxStaleBucketSkips = 16

	staleSkips := 0
	for {
		bucket, err := s.backend.OldestBucket(ctx)
		if err != nil {
			return // store empty
		}

		// Stop when this bucket's time range is newer than the TTL cutoff.
		if int64(bucket)*dur > cutoff {
			return
		}

		candidates, err := s.backend.GetEvictionCandidates(ctx, bucket, batchSize)
		if err != nil || len(candidates) == 0 {
			return
		}

		allFresh := true
		for _, nid := range candidates {
			node, err := s.backend.GetNode(ctx, nid)
			if err != nil {
				continue
			}
			if node.LastAccessUnix >= cutoff {
				// Hot node with a stale bucket entry — will self-heal on next access.
				continue
			}
			allFresh = false
			s.deleteSubtree(ctx, nid)
		}
		if allFresh {
			if len(candidates) == batchSize && staleSkips < maxStaleBucketSkips {
				staleSkips++
				continue
			}
			return
		}
		staleSkips = 0
	}
}

// deleteSubtree deletes root and all descendants via BFS using GetChildren.
func (s *Store) deleteSubtree(ctx context.Context, root NodeID) {
	frontier := []NodeID{root}
	for len(frontier) > 0 {
		next := []NodeID{}
		for _, cur := range frontier {
			children, _ := s.backend.GetChildren(ctx, cur)
			next = append(next, children...)
		}

		const levelCap = 1000
		for len(frontier) > 0 {
			chunk := frontier
			if len(chunk) > levelCap {
				chunk = chunk[:levelCap]
			}
			tx := Transaction{}
			for _, nid := range chunk {
				node, err := s.backend.GetNode(ctx, nid)
				if err != nil {
					continue // already deleted
				}
				tx.DeleteNodes = append(tx.DeleteNodes, nid)
				tx.DeleteBlobs = append(tx.DeleteBlobs, node.BlobID)
				tx.StatsDelta.BytesDelta -= int64(node.BlobSize)
				tx.StatsDelta.EntryDelta--
				if node.ParentID != (NodeID{}) {
					tx.DeleteChildren = append(tx.DeleteChildren, node.ParentID)
				}
				s.entries.Add(-1)
				s.bytes.Add(-int64(node.BlobSize))
			}
			s.backend.Commit(ctx, tx) //nolint:errcheck
			frontier = frontier[len(chunk):]
		}
		frontier = next
	}
}
