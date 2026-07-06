package pebble

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/elevran/charon/internal/chainstore"
)

// ConsistencyReport summarises a full consistency scan of a Pebble chainstore
// directory. OK is true when no errors were found.
type ConsistencyReport struct {
	NodesScanned      int             // total pfxMeta entries scanned
	LRUEntriesScanned int             // total pfxLRU entries scanned
	DepthErrors       []DepthError    // child.Depth != parent.Depth+1
	DanglingLRU       []NodeIDError   // pfxLRU entries with no pfxMeta node
	DecodeErrors      []KeyValueError // pfxMeta records that failed to decode
	OK                bool            // true iff all error slices are empty
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
type NodeIDError struct {
	NodeID string // hex of the offending NodeID
}

func (e NodeIDError) String() string {
	return "node " + e.NodeID
}

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
// The scan streams Pebble iterators (no full key-set materialisation) and
// holds two maps in memory: nodeID → depth and nodeID-set for LRU validation.
// For a store with N nodes the memory cost is ~50 bytes per node.
func (b *Backend) ConsistencyCheck(ctx context.Context) (*ConsistencyReport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	report := &ConsistencyReport{}

	// Pass 1: scan pfxMeta, build depth map and node-set.
	depths := make(map[[20]byte]uint32, 1024)
	present := make(map[[20]byte]struct{}, 1024)

	metaIter, err := b.db.NewIter(&pebble.IterOptions{
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
		present[id] = struct{}{}
		report.NodesScanned++
	}
	if err := metaIter.Error(); err != nil {
		return nil, fmt.Errorf("chainstore/pebble: meta iter: %w", err)
	}

	// Pass 2: re-scan pfxMeta to validate parent.Depth for each non-root node.
	metaIter, err = b.db.NewIter(&pebble.IterOptions{
		LowerBound: []byte{pfxMeta},
		UpperBound: []byte{pfxMeta + 1},
	})
	if err != nil {
		return nil, fmt.Errorf("chainstore/pebble: meta iter (pass 2): %w", err)
	}
	defer func() { _ = metaIter.Close() }()

	for metaIter.First(); metaIter.Valid(); metaIter.Next() {
		node, err := decodeNode(metaIter.Value())
		if err != nil {
			continue // already reported in pass 1
		}
		if node.ParentID == (chainstore.NodeID{}) {
			continue
		}
		parentDepth, ok := depths[node.ParentID]
		if !ok {
			// Parent absent from pfxMeta — could be capacity-evicted (ErrChainExpired)
			// or genuine corruption. We cannot distinguish here without the eviction log,
			// so we surface it as a depth error against depth=0.
			report.DepthErrors = append(report.DepthErrors, DepthError{
				ChildID:  hex.EncodeToString(node.ID[:]),
				ParentID: hex.EncodeToString(node.ParentID[:]),
				Child:    node.Depth,
				Parent:   0,
			})
			continue
		}
		if node.Depth != parentDepth+1 {
			report.DepthErrors = append(report.DepthErrors, DepthError{
				ChildID:  hex.EncodeToString(node.ID[:]),
				ParentID: hex.EncodeToString(node.ParentID[:]),
				Child:    node.Depth,
				Parent:   parentDepth,
			})
		}
	}
	if err := metaIter.Error(); err != nil {
		return nil, fmt.Errorf("chainstore/pebble: meta iter (pass 2): %w", err)
	}

	// Pass 3: scan pfxLRU and verify each entry points to an existing pfxMeta node.
	lruIter, err := b.db.NewIter(&pebble.IterOptions{
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
			continue // malformed; ignore
		}
		var id [20]byte
		copy(id[:], key[9:])
		bucket := binary.BigEndian.Uint64(key[1:9])
		report.LRUEntriesScanned++
		if _, ok := present[id]; !ok {
			report.DanglingLRU = append(report.DanglingLRU, NodeIDError{
				NodeID: fmt.Sprintf("bucket=%d node=%s", bucket, hex.EncodeToString(id[:])),
			})
		}
	}
	if err := lruIter.Error(); err != nil {
		return nil, fmt.Errorf("chainstore/pebble: lru iter: %w", err)
	}

	report.OK = len(report.DepthErrors) == 0 && len(report.DanglingLRU) == 0 && len(report.DecodeErrors) == 0
	return report, nil
}
