package chainstore

import (
	"context"
	"errors"
	"fmt"
	"math"
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
	wg     sync.WaitGroup // used by Phase 3 background goroutines
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
	// Phase 3 will call s.wg.Add and launch eviction/TTL goroutines here.
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
// responseID must not exceed 255 bytes.
// Returns ErrNotFound if previousResponseID is non-empty but does not exist.
// Returns ErrChainTooDeep if the parent's depth is at the maximum uint32 value.
func (s *Store) Store(ctx context.Context, responseID, previousResponseID, tenantKey string, requestBlob []byte) error {
	if len(responseID) > 255 {
		return fmt.Errorf("chainstore.Store: responseID exceeds 255 bytes (len=%d)", len(responseID))
	}

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

	var children []ChildEntry
	if previousResponseID != "" {
		parentID := nodeID(tenantKey, previousResponseID)
		parent, err := s.backend.GetNode(ctx, parentID)
		if err != nil {
			return fmt.Errorf("chainstore.Store: parent lookup: %w", err)
		}
		if parent.Depth == math.MaxUint32 {
			return ErrChainTooDeep
		}
		node.ParentID = parentID
		node.Depth = parent.Depth + 1
		children = []ChildEntry{{Parent: parentID, Child: id}}
	}

	if err := s.backend.Commit(ctx, Transaction{
		PutNodes:    []Node{node},
		PutBlobs:    []BlobEntry{{BlobID: blobID, Data: requestBlob}},
		PutChildren: children,
		StatsDelta: StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(len(requestBlob)),
		},
	}); err != nil {
		return fmt.Errorf("chainstore.Store: commit: %w", err)
	}

	s.entries.Add(1)
	s.bytes.Add(int64(len(requestBlob)))
	return nil
}

// Resolve walks the chain from responseID to the root and returns all turns root-first.
// Each Turn carries both request and response blobs; ResponseBlob is nil for turns not
// yet completed (ResponseBlobID is zero). Updates LastAccessUnix and promotes LRU bucket
// index entries only for nodes that have crossed a bucket boundary since last access.
//
// NOTE: LoadChain and GetBlobs use independent Pebble snapshots. A concurrent Store
// completing a turn between the two reads will cause Resolve to see a non-nil
// ResponseBlob for a node that appeared incomplete in the chain snapshot (benign
// over-read). A concurrent Resolve that promotes a node to a newer bucket between
// LoadChain and this Resolve's touch commit may leave the node indexed under two
// LRU buckets (stale OldBucket delete is a no-op; node ends up in the newer bucket
// after the later commit). Both races are benign for Phase 2 and will be addressed
// in Phase 3 with a snapshot-spanning read path.
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
	bucketMoves := make([]BucketMove, 0, len(nodes))

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
