package chainstore

import (
	"container/list"
	"sync"
	"time"
)

// Default cache configuration. The cache is disabled when MaxCacheBytes == 0
// so a zero-value Config preserves the existing "no caching" behaviour.
const (
	DefaultCacheMaxBytes int64         = 64 * 1024 * 1024 // 64 MiB
	DefaultCacheTTL      time.Duration = 5 * time.Minute
)

// cacheEntry holds one resolved turn keyed by the node's NodeID.
// Storing single turns (not full chains) keeps cache storage O(N) in chain
// length: resolving a chain of depth N populates N independent entries rather
// than one entry containing N-1 ancestor blobs.
type cacheEntry struct {
	key       NodeID
	turn      Turn
	bytes     int64
	expiresAt time.Time
}

// chainCache is a bounded LRU cache of resolved turns keyed by NodeID.
//
// On resolve, walkAndTouch checks each node's ID in the cache. A hit skips
// the GetBlobs call for that node. A miss fetches blobs from Pebble and
// stores the result. The cache is populated per-node, not per-chain, so
// cache storage is O(distinct nodes) regardless of chain depth.
//
// Bounded by total blob bytes (chainCache.used); when an insert pushes used
// over maxBytes the LRU tail is evicted. An entry whose size alone exceeds
// maxBytes is still admitted so the cache is never empty for steady-state
// workloads — the bound is a soft target.
//
// TTL is enforced on read; expired entries are dropped on access. The TTL
// bounds staleness from mutations that do not invalidate the cache
// (e.g. Complete, which changes a node's ResponseBlob in Pebble without
// deleting it).
//
// Eager invalidation: invalidateNodes drops entries for deleted NodeIDs.
// Called from the Commit path when Transaction.DeleteNodes is non-empty.
// O(len(deleted)) — direct map lookup, no scan required.
type chainCache struct {
	mu       sync.Mutex
	maxBytes int64
	ttl      time.Duration
	now      func() time.Time
	metrics  *storeMetrics // nil = no Prometheus publish

	used  int64
	items map[NodeID]*list.Element
	order *list.List // front = most recent; back = least recent

	hits      int64
	misses    int64
	evictions int64
}

// get returns the cached Turn for nodeID if present and not expired.
func (c *chainCache) get(nodeID NodeID) (Turn, bool) {
	if c == nil {
		return Turn{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[nodeID]
	if !ok {
		c.recordMiss()
		return Turn{}, false
	}
	entry := el.Value.(*cacheEntry)
	if c.ttl > 0 && !c.now().Before(entry.expiresAt) {
		c.removeElementLocked(el, entry)
		c.recordMiss()
		return Turn{}, false
	}
	c.order.MoveToFront(el)
	c.recordHit()
	return entry.turn, true
}

// put inserts or refreshes the cached turn for nodeID; bytes is the blob-byte cost.
// Evicts LRU-tail entries when used would exceed maxBytes.
func (c *chainCache) put(nodeID NodeID, turn Turn, bytes int64) {
	if c == nil || c.maxBytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[nodeID]; ok {
		entry := el.Value.(*cacheEntry)
		c.used += bytes - entry.bytes
		entry.turn = turn
		entry.bytes = bytes
		entry.expiresAt = c.now().Add(c.ttl)
		c.order.MoveToFront(el)
	} else {
		c.items[nodeID] = c.order.PushFront(&cacheEntry{
			key:       nodeID,
			turn:      turn,
			bytes:     bytes,
			expiresAt: c.now().Add(c.ttl),
		})
		c.used += bytes
	}
	if c.metrics != nil {
		c.metrics.cacheBytes.Set(float64(c.used))
	}
	c.enforceBoundLocked()
}

func (c *chainCache) enforceBoundLocked() {
	// Keep at least one entry even if it alone exceeds the bound; a single
	// oversized cached turn is still better than no cache at all.
	for c.used > c.maxBytes && c.order.Len() > 1 {
		oldest := c.order.Back()
		if oldest == nil {
			return
		}
		c.removeElementLocked(oldest, oldest.Value.(*cacheEntry))
		c.evictions++
		if c.metrics != nil {
			c.metrics.cacheEvictionsTotal.Inc()
		}
	}
}

// invalidateNodes removes cached entries for each deleted NodeID.
// O(len(deleted)) — direct map lookup, no full-cache scan.
func (c *chainCache) invalidateNodes(deleted []NodeID) {
	if c == nil || len(deleted) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, d := range deleted {
		if el, ok := c.items[d]; ok {
			entry := el.Value.(*cacheEntry)
			c.removeElementLocked(el, entry)
			c.evictions++
			if c.metrics != nil {
				c.metrics.cacheEvictionsTotal.Inc()
			}
		}
	}
}

// removeElementLocked removes el from list and map and adjusts the byte total.
// Called with c.mu held.
func (c *chainCache) removeElementLocked(el *list.Element, entry *cacheEntry) {
	c.order.Remove(el)
	delete(c.items, entry.key)
	c.used -= entry.bytes
	if c.metrics != nil {
		c.metrics.cacheBytes.Set(float64(c.used))
	}
}

func (c *chainCache) recordHit() {
	c.hits++
	if c.metrics != nil {
		c.metrics.cacheHitsTotal.Inc()
	}
}

func (c *chainCache) recordMiss() {
	c.misses++
	if c.metrics != nil {
		c.metrics.cacheMissesTotal.Inc()
	}
}

// stats returns a snapshot of cache counters and current byte usage.
func (c *chainCache) stats() (hits, misses, evictions int64, bytes int64, entries int) {
	if c == nil {
		return 0, 0, 0, 0, 0
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits, c.misses, c.evictions, c.used, c.order.Len()
}
