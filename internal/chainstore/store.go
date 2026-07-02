package chainstore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
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
	MaxEntries int64
	MaxBytes   int64
	// TTL is the maximum age of a chain. Set to 0 to disable TTL-based eviction.
	// Effective TTL precision is ±BucketDuration; set BucketDuration < TTL/100 for sub-percent accuracy.
	TTL              time.Duration
	BucketDuration   time.Duration         // LRU bucket width; default 1h
	EvictionInterval time.Duration         // ticker period for capacity eviction; default 1m
	TTLInterval      time.Duration         // ticker period for TTL reaper; default 5m
	Clock            Clock                 // nil = RealClock
	Log              *slog.Logger          // nil = slog.Default()
	Backend          Backend               // required — supply via pebble.Open() or dynamodb.Open()
	Registerer       prometheus.Registerer // nil = no metrics
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
	nudgeCount atomic.Int64 // counts successful nudge sends; exported via export_test.go

	metrics *storeMetrics

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Entries returns the approximate number of stored nodes (updated optimistically on write/delete).
func (s *Store) Entries() int64 { return max(s.entries.Load(), 0) }

// Bytes returns the approximate total blob bytes (updated optimistically on write/delete).
func (s *Store) Bytes() int64 { return max(s.bytes.Load(), 0) }

// New wires cfg into a Store and starts background goroutines.
// ctx is used only for the initial Stats() call to reload persistent counters;
// the store's own background goroutines run on an internal context.
// Callers should use pebble.Open() or dynamodb.Open() rather than calling New directly.
func New(ctx context.Context, cfg Config) (*Store, error) {
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
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}

	m, err := newStoreMetrics(cfg.Registerer)
	if err != nil {
		return nil, fmt.Errorf("chainstore.New: register metrics: %w", err)
	}

	s := &Store{
		cfg:        cfg,
		backend:    cfg.Backend,
		clock:      cfg.Clock,
		nudgeEvict: make(chan struct{}, 1),
		metrics:    m,
	}

	// Reload stats from the persistent stats key so in-memory counters survive restarts.
	entries, bytes, err := cfg.Backend.Stats(ctx)
	if err != nil {
		return nil, fmt.Errorf("chainstore.New: reload stats: %w", err)
	}
	s.entries.Store(entries)
	s.bytes.Store(bytes)
	if m != nil {
		m.entries.Set(float64(entries))
		m.bytes.Set(float64(bytes))
	}

	s.ctx, s.cancel = context.WithCancel(context.Background())
	if cfg.MaxEntries > 0 || cfg.MaxBytes > 0 {
		s.wg.Add(1)
		go s.evictionLoop(s.ctx)
	}
	if cfg.TTL > 0 {
		s.wg.Add(1)
		go s.ttlLoop(s.ctx)
	}
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

	entries := s.entries.Add(1)
	totalBytes := s.bytes.Add(int64(len(requestBlob)))
	if (s.cfg.MaxEntries > 0 && entries > s.cfg.MaxEntries) ||
		(s.cfg.MaxBytes > 0 && totalBytes > s.cfg.MaxBytes) {
		s.notifyCapacityExceeded()
	}
	return nil
}

// Complete stores the response blob for a previously stored turn.
// Returns ErrNotFound if responseID does not exist.
func (s *Store) Complete(ctx context.Context, responseID, tenantKey string, responseBlob []byte) error {
	id := nodeID(tenantKey, responseID)
	node, err := s.backend.GetNode(ctx, id)
	if err != nil {
		return fmt.Errorf("chainstore.Complete: node lookup: %w", err)
	}
	blobID := BlobID(uuid.New())
	node.ResponseBlobID = blobID
	node.ResponseBlobSize = uint32(len(responseBlob))
	if err := s.backend.Commit(ctx, Transaction{
		PutNodes:   []Node{node},
		PutBlobs:   []BlobEntry{{BlobID: blobID, Data: responseBlob}},
		StatsDelta: StatsDelta{BytesDelta: int64(len(responseBlob))},
	}); err != nil {
		return fmt.Errorf("chainstore.Complete: commit: %w", err)
	}
	s.bytes.Add(int64(len(responseBlob)))
	return nil
}

// Delete removes a turn and all its descendants.
// Returns ErrNotFound if responseID does not exist.
func (s *Store) Delete(ctx context.Context, responseID, tenantKey string) error {
	id := nodeID(tenantKey, responseID)
	if _, err := s.backend.GetNode(ctx, id); err != nil {
		return fmt.Errorf("chainstore.Delete: %w", err)
	}
	s.deleteSubtree(ctx, id)
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
// after the later commit). Both races are benign and will be addressed in a future
// phase with a snapshot-spanning read path.
func (s *Store) Resolve(ctx context.Context, responseID, tenantKey string) (turns []Turn, err error) {
	start := time.Now()
	defer func() {
		if s.metrics == nil {
			return
		}
		status := "ok"
		if err != nil {
			status = "error"
		}
		s.metrics.resolveLatency.WithLabelValues(status).Observe(time.Since(start).Seconds())
		if err == nil {
			s.metrics.chainDepth.Observe(float64(len(turns)))
		}
	}()

	id := nodeID(tenantKey, responseID)
	nodes, err := s.backend.LoadChain(ctx, id)
	if err != nil {
		return nil, err
	}

	now := s.clock.Now()
	nowUnix := now.Unix()
	currentBucket := s.cfg.BucketFor(now)

	turns = make([]Turn, len(nodes))
	updatedNodes := make([]Node, len(nodes))
	bucketMoves := make([]BucketMove, 0, len(nodes))

	for i, node := range nodes {
		var reqBlob, respBlob []byte
		reqBlob, respBlob, err = s.backend.GetBlobs(ctx, node)
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
