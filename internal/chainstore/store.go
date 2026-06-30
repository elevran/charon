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
		cfg:     cfg,
		backend: cfg.Backend,
		clock:   cfg.Clock,
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

// evictionLoop is the long-running capacity-eviction goroutine (Phase 3).
func (s *Store) evictionLoop(ctx context.Context) {
	defer s.wg.Done()
	<-ctx.Done()
}

// ttlLoop is the long-running TTL-reaper goroutine (Phase 3).
func (s *Store) ttlLoop(ctx context.Context) {
	defer s.wg.Done()
	<-ctx.Done()
}
