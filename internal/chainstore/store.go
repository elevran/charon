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

// buildNode constructs a partially-initialised Node with all fields that are
// independent of the final responseID and blob IDs: timestamps, bucket, and
// parent linkage. Callers must fill in ID, ResponseID, and blob fields.
// Returns ErrNotFound or ErrChainTooDeep if the parent cannot be resolved.
func (s *Store) buildNode(ctx context.Context, tenantKey, previousResponseID string) (Node, error) {
	now := s.clock.Now()
	node := Node{
		Version:        1,
		LastAccessUnix: now.Unix(),
		CreatedAt:      now.Unix(),
		BucketID:       s.cfg.BucketFor(now),
	}
	if previousResponseID != "" {
		parentID := nodeID(tenantKey, previousResponseID)
		parent, err := s.backend.GetNode(ctx, parentID)
		if err != nil {
			return Node{}, fmt.Errorf("parent lookup: %w", err)
		}
		if parent.Depth == math.MaxUint32 {
			return Node{}, ErrChainTooDeep
		}
		node.ParentID = parentID
		node.Depth = parent.Depth + 1
	}
	return node, nil
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

	node, err := s.buildNode(ctx, tenantKey, previousResponseID)
	if err != nil {
		return fmt.Errorf("chainstore.Store: %w", err)
	}
	node.ID = id
	node.RequestBlobID = blobID
	node.RequestBlobSize = uint32(len(requestBlob))
	node.ResponseID = responseID

	var children []ChildEntry
	if node.ParentID != (NodeID{}) {
		children = []ChildEntry{{Parent: node.ParentID, Child: id}}
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

// StoreWithStaging reads the staging record created by ResolveAndStage, fills in
// the inference-supplied responseID and responseBlob, and commits the final Node
// atomically with deletion of the staging record.
//
// If stagingID is empty, responseBlob is written as the sole blob (no staging lookup).
// Returns ErrNotFound if stagingID is non-empty but absent.
func (s *Store) StoreWithStaging(ctx context.Context, stagingID, responseID, previousResponseID, tenantKey string, responseBlob []byte) error {
	if len(responseID) > 255 {
		return fmt.Errorf("chainstore.StoreWithStaging: responseID exceeds 255 bytes (len=%d)", len(responseID))
	}

	id := nodeID(tenantKey, responseID)
	respBlobID := BlobID(uuid.New())

	tx := Transaction{
		PutBlobs: []BlobEntry{{BlobID: respBlobID, Data: responseBlob}},
		StatsDelta: StatsDelta{
			EntryDelta: 1,
			BytesDelta: int64(len(responseBlob)),
		},
	}

	var node Node

	if stagingID != "" {
		uid, err := uuid.Parse(stagingID)
		if err != nil {
			return fmt.Errorf("chainstore.StoreWithStaging: invalid stagingID: %w", err)
		}
		sid := BlobID(uid)
		staged, err := s.backend.GetStagingNode(ctx, sid)
		if err != nil {
			return fmt.Errorf("chainstore.StoreWithStaging: staging lookup: %w", err)
		}
		node = staged
		tx.DeleteStagingNodes = []BlobID{sid}
		tx.StatsDelta.BytesDelta += int64(staged.RequestBlobSize)
	} else {
		var err error
		node, err = s.buildNode(ctx, tenantKey, previousResponseID)
		if err != nil {
			return fmt.Errorf("chainstore.StoreWithStaging: %w", err)
		}
	}

	node.ID = id
	node.ResponseID = responseID
	node.ResponseBlobID = respBlobID
	node.ResponseBlobSize = uint32(len(responseBlob))
	node.Status = NodeStatusCompleted
	tx.PutNodes = []Node{node}

	if node.ParentID != (NodeID{}) {
		tx.PutChildren = []ChildEntry{{Parent: node.ParentID, Child: id}}
	}

	if err := s.backend.Commit(ctx, tx); err != nil {
		return fmt.Errorf("chainstore.StoreWithStaging: commit: %w", err)
	}

	entries := s.entries.Add(1)
	totalBytes := s.bytes.Add(tx.StatsDelta.BytesDelta)
	if (s.cfg.MaxEntries > 0 && entries > s.cfg.MaxEntries) ||
		(s.cfg.MaxBytes > 0 && totalBytes > s.cfg.MaxBytes) {
		s.notifyCapacityExceeded()
	}
	return nil
}

// Retrieve fetches a single node's metadata without updating LastAccessUnix or the
// LRU index. Returns ErrNotFound if responseID does not exist.
func (s *Store) Retrieve(_ context.Context, _, _ string) (PublicNode, error) {
	return PublicNode{}, ErrNotImplemented
}

// Ping performs a metadata read to verify the backend is reachable.
func (s *Store) Ping(ctx context.Context) error {
	_, _, err := s.backend.Stats(ctx)
	return err
}

// Delete removes a turn and its descendants (keepDescendants=false, the default)
// or only the named node (keepDescendants=true, leaving descendants with dangling
// parent pointers — caller's responsibility).
// Returns ErrNotFound if responseID does not exist.
func (s *Store) Delete(ctx context.Context, responseID, tenantKey string, keepDescendants bool) error {
	id := nodeID(tenantKey, responseID)
	node, err := s.backend.GetNode(ctx, id)
	if err != nil {
		return fmt.Errorf("chainstore.Delete: %w", err)
	}
	if keepDescendants {
		s.deleteNode(ctx, node)
	} else {
		s.deleteSubtree(ctx, id)
	}
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
	_, turns, err = s.walkAndTouch(ctx, id)
	return turns, err
}

// walkAndTouch loads the chain rooted at leaf, fetches blobs, updates LRU, and
// returns (nodes leaf-first, turns root-first). Both callers — Resolve and
// ResolveAndStage — need the leaf node's Depth, so nodes is returned alongside turns.
func (s *Store) walkAndTouch(ctx context.Context, leaf NodeID) (nodes []Node, turns []Turn, err error) {
	nodes, err = s.backend.LoadChain(ctx, leaf)
	if err != nil {
		return nil, nil, err
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
			return nil, nil, err
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

	if err := s.backend.Commit(ctx, Transaction{
		PutNodes:    updatedNodes,
		BucketMoves: bucketMoves,
	}); err != nil {
		return nil, nil, err
	}
	return nodes, turns, nil
}

// ResolveAndStage walks the chain from previousResponseID to root, stages the
// request blob durably, and returns an opaque stagingID plus the turn history
// root-first. The stagingID must be passed to StoreWithStaging once the
// inference server returns a response.
//
// When previousResponseID is empty the turn slice is empty (new conversation).
// When requestBlob is nil a staging record is still created (zero-length blob).
func (s *Store) ResolveAndStage(ctx context.Context, previousResponseID, tenantKey string, requestBlob []byte) (stagingID string, turns []Turn, err error) {
	var (
		parentID NodeID
		depth    uint32
	)

	if previousResponseID != "" {
		parentID = nodeID(tenantKey, previousResponseID)
		var nodes []Node
		nodes, turns, err = s.walkAndTouch(ctx, parentID)
		if err != nil {
			return "", nil, fmt.Errorf("chainstore.ResolveAndStage: walk chain: %w", err)
		}
		// nodes[0] is the leaf (previousResponseID); its depth + 1 is the new node's depth.
		if nodes[0].Depth == math.MaxUint32 {
			return "", nil, ErrChainTooDeep
		}
		depth = nodes[0].Depth + 1
	}

	now := s.clock.Now()
	sidUUID := uuid.New()
	sid := BlobID(sidUUID)
	reqBlobID := BlobID(uuid.New())

	staging := Node{
		Version:         1,
		ParentID:        parentID,
		RequestBlobID:   reqBlobID,
		RequestBlobSize: uint32(len(requestBlob)),
		CreatedAt:       now.Unix(),
		LastAccessUnix:  now.Unix(),
		BucketID:        s.cfg.BucketFor(now),
		Depth:           depth,
	}

	if err = s.backend.Commit(ctx, Transaction{
		PutStagingNodes: []StagingEntry{{StagingID: sid, Node: staging}},
		PutBlobs:        []BlobEntry{{BlobID: reqBlobID, Data: requestBlob}},
	}); err != nil {
		return "", nil, fmt.Errorf("chainstore.ResolveAndStage: commit staging: %w", err)
	}

	return sidUUID.String(), turns, nil
}
