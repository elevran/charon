package chainstore

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
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

// Store writes one conversation turn and its request blob atomically.
// previousResponseID may be empty for a root turn (start of a new conversation).
// Returns ErrNotFound if previousResponseID is non-empty but does not exist.
func (s *Store) Store(ctx context.Context, responseID, previousResponseID, tenantKey string, requestBlob []byte) error {
	id := nodeID(tenantKey, responseID)
	blobID := BlobID(uuid.New())
	now := s.clock.Now()

	node := Node{
		Version:         1,
		ID:              id,
		RequestBlobID:   blobID,
		LastAccessUnix:  now.Unix(),
		CreatedAt:       now.Unix(),
		BucketID:        s.cfg.BucketFor(now),
		RequestBlobSize: uint32(len(requestBlob)),
		ResponseID:      responseID,
	}

	tx := Transaction{
		PutNodes: []Node{node},
		PutBlobs: []BlobEntry{{BlobID: blobID, Data: requestBlob}},
		StatsDelta: StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(len(requestBlob)),
		},
	}

	if previousResponseID != "" {
		parentID := nodeID(tenantKey, previousResponseID)
		parent, err := s.backend.GetNode(ctx, parentID)
		if err != nil {
			return err
		}
		node.ParentID = parentID
		node.Depth = parent.Depth + 1
		tx.PutNodes[0] = node
		tx.PutChildren = []ChildEntry{{Parent: parentID, Child: id}}
	}

	if err := s.backend.Commit(ctx, tx); err != nil {
		return err
	}

	s.entries.Add(1)
	s.bytes.Add(int64(len(requestBlob)))
	return nil
}

// Resolve walks the chain from responseID to the root and returns all turns root-first.
// Each Turn carries both request and response blobs; ResponseBlob is nil for turns not
// yet completed (ResponseBlobID is zero). Updates LastAccessUnix and promotes LRU bucket
// index entries only for nodes that have crossed a bucket boundary since last access.
func (s *Store) Resolve(ctx context.Context, responseID, tenantKey string) ([]Turn, error) {
	id := nodeID(tenantKey, responseID)
	nodes, err := s.backend.LoadChain(ctx, id)
	if err != nil {
		return nil, err
	}

	now := s.clock.Now()
	nowUnix := now.Unix()
	currentBucket := s.cfg.BucketFor(now)

	turns := make([]Turn, len(nodes))
	updatedNodes := make([]Node, len(nodes))
	var bucketMoves []BucketMove

	for i, node := range nodes {
		reqBlob, respBlob, err := s.backend.GetBlobs(ctx, node)
		if err != nil {
			return nil, err
		}
		// nodes is leaf-first; turns must be root-first.
		turns[len(nodes)-1-i] = Turn{
			ResponseID:   node.ResponseID,
			RequestBlob:  reqBlob,
			ResponseBlob: respBlob,
		}

		updated := node
		updated.LastAccessUnix = nowUnix
		if node.BucketID != currentBucket {
			bucketMoves = append(bucketMoves, BucketMove{
				NodeID:    node.ID,
				OldBucket: node.BucketID,
				NewBucket: currentBucket,
			})
			updated.BucketID = currentBucket
		}
		updatedNodes[i] = updated
	}

	return turns, s.backend.Commit(ctx, Transaction{
		PutNodes:    updatedNodes,
		BucketMoves: bucketMoves,
	})
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
