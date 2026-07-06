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

// cacheEntry holds one resolved chain. key is the leaf NodeID; container/list
// elements do not carry their own key, so we duplicate it for O(1) removal.
type cacheEntry struct {
	key       NodeID
	nodes     []Node
	turns     []Turn
	bytes     int64
	expiresAt time.Time
}

// chainCache is a bounded LRU cache of resolved chains keyed by leaf NodeID.
//
// On hit, walkAndTouch skips LoadChain, GetBlobs, and the touch/BucketMove
// commit. On miss, walkAndTouch re-walks the chain and stores the result here.
//
// Bounded by total blob bytes (chainCache.used); when an insert pushes used
// over maxBytes the LRU tail is evicted. An entry whose size alone exceeds
// maxBytes is still admitted so the cache is never empty for steady-state
// workloads — the bound is a soft target.
//
// TTL is enforced on read; expired entries are dropped on access. The TTL
// bounds staleness from mutations that do not invalidate the cache
// (e.g. Complete, which changes a turn's ResponseBlob in Pebble).
//
// Eager invalidation: invalidateNodes drops any entry whose nodes slice
// contains a deleted NodeID. Called from the Commit path when
// Transaction.DeleteNodes is non-empty.
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

// get returns the cached chain for leaf if present and not expired.
func (c *chainCache) get(leaf NodeID) ([]Node, []Turn, bool) {
	if c == nil {
		return nil, nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[leaf]
	if !ok {
		c.recordMiss()
		return nil, nil, false
	}
	entry := el.Value.(*cacheEntry)
	if c.ttl > 0 && !c.now().Before(entry.expiresAt) {
		c.removeElementLocked(el, entry)
		c.recordMiss()
		return nil, nil, false
	}
	c.order.MoveToFront(el)
	c.recordHit()
	return entry.nodes, entry.turns, true
}

// put inserts or refreshes the cached chain for leaf; bytes is the entry's
// blob-byte cost. Evicts LRU-tail entries when used would exceed maxBytes.
func (c *chainCache) put(leaf NodeID, nodes []Node, turns []Turn, bytes int64) {
	if c == nil || c.maxBytes <= 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[leaf]; ok {
		entry := el.Value.(*cacheEntry)
		c.used += bytes - entry.bytes
		entry.nodes = nodes
		entry.turns = turns
		entry.bytes = bytes
		entry.expiresAt = c.now().Add(c.ttl)
		c.order.MoveToFront(el)
	} else {
		c.items[leaf] = c.order.PushFront(&cacheEntry{
			key:       leaf,
			nodes:     nodes,
			turns:     turns,
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
	// oversized cached chain is still better than no cache at all.
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

// invalidateNodes removes any entry whose nodes slice contains one of deleted.
// Called from the Commit path when Transaction.DeleteNodes is non-empty.
// O(entries * avgDepth); in practice most invalidations touch a single
// deleted node and the inner linear scan avoids a per-call map allocation.
func (c *chainCache) invalidateNodes(deleted []NodeID) {
	if c == nil || len(deleted) == 0 {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var next *list.Element
	for el := c.order.Front(); el != nil; el = next {
		next = el.Next()
		entry := el.Value.(*cacheEntry)
		for _, d := range deleted {
			for _, n := range entry.nodes {
				if n.ID == d {
					c.removeElementLocked(el, entry)
					c.evictions++
					if c.metrics != nil {
						c.metrics.cacheEvictionsTotal.Inc()
					}
					goto nextEntry
				}
			}
		}
	nextEntry:
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
