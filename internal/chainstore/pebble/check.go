package pebble

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	crdbpebble "github.com/cockroachdb/pebble"

	"github.com/elevran/charon/internal/chainstore"
)

// ConsistencyReport summarises a full consistency scan of a Pebble chainstore
// directory. OK is true when no errors were found. MissingParents is reported
// but does not affect OK — it is the expected steady-state for any store that
// has had capacity eviction.
type ConsistencyReport struct {
	NodesScanned      int             // total pfxMeta entries scanned
	LRUEntriesScanned int             // total pfxLRU entries scanned
	DepthErrors       []DepthError    // child.Depth != parent.Depth+1 (parent present)
	DanglingLRU       []NodeIDError   // pfxLRU entries with no pfxMeta node
	DecodeErrors      []KeyValueError // pfxMeta records that failed to decode
	MissingParents    []NodeIDError   // children whose parent is absent from pfxMeta (capacity eviction)
	OK                bool            // true iff DepthErrors/DanglingLRU/DecodeErrors are empty
}

// DepthError is reported when a node's depth is inconsistent with its parent's.
type DepthError struct {
	ChildID  string // hex of child NodeID
	ParentID string // hex of parent NodeID
	Child    uint32 // observed child.Depth
	Parent   uint32 // observed parent.Depth
}

func (e DepthError) String() string {
	return fmt.Sprintf("depth mismatch: child %s has depth %d but parent %s has depth %d",
		e.ChildID, e.Child, e.ParentID, e.Parent)
}

// NodeIDError wraps a NodeID with its hex encoding for human-readable reports.
type NodeIDError string

func (e NodeIDError) String() string { return "node " + string(e) }

// KeyValueError reports a record that failed to decode; carries key/value context.
type KeyValueError struct {
	Key string // hex of the key that failed to decode
	Err string // human-readable decode error
}

func (e KeyValueError) String() string {
	return fmt.Sprintf("decode error on key %s: %s", e.Key, e.Err)
}

// ConsistencyCheck scans all pfxMeta and pfxLRU entries in b.db and verifies
// two invariants:
//   - For every non-root node, child.Depth == parent.Depth + 1.
//   - Every pfxLRU entry points to a node that still exists in pfxMeta.
//
// It is safe to call on a live store — it never writes — but the caller should
// be aware that mid-scan mutations may surface as transient errors that the
// next scan will not reproduce. The intended use is read-only maintenance via
// cmd/cache-check on a quiescent directory.
//
// The scan streams Pebble iterators (no full key-set materialisation). For a
// store with N nodes the memory cost is ~110 bytes per node (Node struct + map
// entry); one pfxMeta scan + one pfxLRU scan.
func (b *Backend) ConsistencyCheck(ctx context.Context) (*ConsistencyReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	report := &ConsistencyReport{}

	// Single pfxMeta scan: collect every node, build depth map keyed by ID.
	nodes := make([]chainstore.Node, 0, 1024)
	depths := make(map[[20]byte]uint32, 1024)

	metaIter, err := b.db.NewIter(&crdbpebble.IterOptions{
		LowerBound: []byte{pfxMeta},
		UpperBound: []byte{pfxMeta + 1},
	})
	if err != nil {
		return nil, fmt.Errorf("chainstore/pebble: meta iter: %w", err)
	}
	defer func() { _ = metaIter.Close() }()

	for metaIter.First(); metaIter.Valid(); metaIter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := metaIter.Key()
		if len(key) != 1+20 {
			report.DecodeErrors = append(report.DecodeErrors, KeyValueError{
				Key: hex.EncodeToString(key),
				Err: fmt.Sprintf("unexpected meta key length %d", len(key)),
			})
			continue
		}
		var id [20]byte
		copy(id[:], key[1:])
		node, err := decodeNode(metaIter.Value())
		if err != nil {
			report.DecodeErrors = append(report.DecodeErrors, KeyValueError{
				Key: hex.EncodeToString(key),
				Err: err.Error(),
			})
			continue
		}
		depths[id] = node.Depth
		nodes = append(nodes, node)
		report.NodesScanned++
	}
	if err := metaIter.Error(); err != nil {
		return nil, fmt.Errorf("chainstore/pebble: meta iter: %w", err)
	}

	// Validate parent.Depth from the in-memory slice (no second Pebble scan).
	for _, n := range nodes {
		if n.ParentID == (chainstore.NodeID{}) {
			continue
		}
		parentDepth, ok := depths[n.ParentID]
		if !ok {
			// Parent absent from pfxMeta. This is the expected steady-state
			// after capacity eviction (non-cascading deleteNode removes the
			// parent meta without touching children) — record it in
			// MissingParents so operators can see it without OK=false. We
			// cannot distinguish that from corruption without the eviction
			// log; the count is reported and operators can inspect if needed.
			report.MissingParents = append(report.MissingParents, NodeIDError(
				fmt.Sprintf("child %s (depth=%d) references missing parent %s",
					hex.EncodeToString(n.ID[:]), n.Depth, hex.EncodeToString(n.ParentID[:]))))
			continue
		}
		if n.Depth != parentDepth+1 {
			report.DepthErrors = append(report.DepthErrors, DepthError{
				ChildID:  hex.EncodeToString(n.ID[:]),
				ParentID: hex.EncodeToString(n.ParentID[:]),
				Child:    n.Depth,
				Parent:   parentDepth,
			})
		}
	}

	// pfxLRU scan: each entry must point to a node we collected in pfxMeta.
	lruIter, err := b.db.NewIter(&crdbpebble.IterOptions{
		LowerBound: []byte{pfxLRU},
		UpperBound: []byte{pfxLRU + 1},
	})
	if err != nil {
		return nil, fmt.Errorf("chainstore/pebble: lru iter: %w", err)
	}
	defer func() { _ = lruIter.Close() }()

	for lruIter.First(); lruIter.Valid(); lruIter.Next() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := lruIter.Key()
		// pfxLRU layout: [pfx=1][bucket=8][nodeID=20] = 29 bytes
		if len(key) != 1+8+20 {
			continue // unknown on-disk layout — skip
		}
		var id [20]byte
		copy(id[:], key[9:])
		bucket := binary.BigEndian.Uint64(key[1:9])
		report.LRUEntriesScanned++
		if _, ok := depths[id]; !ok {
			report.DanglingLRU = append(report.DanglingLRU, NodeIDError(
				fmt.Sprintf("bucket=%d node=%s", bucket, hex.EncodeToString(id[:]))))
		}
	}
	if err := lruIter.Error(); err != nil {
		return nil, fmt.Errorf("chainstore/pebble: lru iter: %w", err)
	}

	report.OK = len(report.DepthErrors) == 0 && len(report.DanglingLRU) == 0 && len(report.DecodeErrors) == 0
	// Note: MissingParents is reported but does not affect OK — see type doc.
	return report, nil
}
