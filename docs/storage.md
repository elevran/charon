# Charon: Storage Design

## The Core Data Structure

Every response is a node in a singly-linked list:

```
resp_A { prev: null,    input: [...], output: [...] }
   ↑
resp_B { prev: resp_A,  input: [...], output: [...] }
   ↑
resp_C { prev: resp_B,  input: [...], output: [...] }
```

The access patterns that drive the storage design are:

| Operation | Frequency | Description |
|-----------|-----------|-------------|
| **Store** | Every request | Write one new response node |
| **Resolve** | Every continuation turn | Walk from head to root, flatten into context |
| **Retrieve** | GET /responses/{id} | Fetch one node's stored data |
| **Evict / TTL** | Background | Remove expired or capacity-excess nodes |

There are no range scans over content. The hot path is: node lookup → chain walk → flatten.

---

## Chain Reconstruction Strategy

| Strategy | Write cost | Read cost | Storage cost |
|----------|-----------|-----------|--------------|
| Entry-per-turn | O(1) | O(N) — walk chain | O(N) total |
| Full-snapshot | O(N) | O(1) — single fetch | O(N²) total |

Charon uses **entry-per-turn**: each response stores only its own turn. Chain reconstruction walks from head to root on each resolve. This is storage-efficient and write-fast; read latency grows linearly with chain depth.

---

## Storage Backend: Pebble

Charon uses [Pebble](https://github.com/cockroachdb/pebble), an embedded LSM-tree key-value store. All data lives in a single Pebble database directory. There are no external processes.

When `storage.data_dir` is empty or omitted, Charon opens an in-memory Pebble instance — data is lost on restart. Use this for conformance/compliance testing and CI.

### What Is Stored

Charon stores four categories of objects in the database: **node metadata** (per-turn chain linkage and timestamps), **payload blobs** (the request and response content for each turn), an **LRU ordering index** (bucket-ordered keys used for capacity eviction), and **staging records** (transient in-flight streaming nodes that are invisible to chain walks and promoted to full nodes on stream commit).

---

## Payload Content

Each node's blob contains the items needed for chain reconstruction: the input and output items for that turn, plus a `previous_response_id` back-pointer. Fields not used for reconstruction — sampling params, tools, instructions, operational metadata — are stored for API completeness but not consulted during resolve. `instructions` is explicitly excluded from the reconstructed context; it is re-supplied by the caller on each turn.

`encrypted_content` fields on reasoning and compaction items are opaque provider blobs. They must be stored and returned verbatim.

---

## Chain Resolution

Resolution walks the parent-pointer chain from the head response back to the root, collecting input and output items at each node in reverse order, then reverses the list to produce chronological order. The assembled flat context is returned to the proxy before inference.

Resolution aborts early with a structured error if:
- The chain walk exceeds `storage.max_chain_depth` hops → `chain_too_deep` (HTTP 422)
- The assembled context exceeds `storage.max_context_bytes` bytes → `context_too_large` (HTTP 422)

---

## TTL and Eviction

### TTL (time-to-live)

Responses expire after `storage.ttl_days` days (default 30). A background TTL reaper runs every `workers.ttl_interval` (default 1h) and removes nodes whose creation timestamp is older than the TTL.

### Capacity eviction

When `storage.max_responses` or `storage.max_payload` limits are set, Charon evicts the oldest chains first (LRU order) whenever a new node would exceed the cap.

The LRU index sorts nodes into time buckets. Eviction scans the oldest bucket and removes entire chains (all nodes sharing the same chain root) to preserve chain coherence — partial eviction of a chain would leave orphaned nodes.

### Deletion

`DELETE /responses/{id}` removes a single response node. It does not cascade to other nodes in the chain. A resolve walk that encounters a missing node returns `previous_response_not_found`. Charon does not refuse deletion of non-leaf nodes and does not cascade.

---

## `store: false` Semantics

Charon has no knowledge of `store: false`; the proxy simply skips the store call.

---

## Staging (Streaming Ingest)

Streaming ingest delivers output items to Charon in chunks as tokens arrive from the inference backend. During streaming, the node is written to a staging slot — invisible to chain walks and resolve calls. On stream commit, the staging record is atomically promoted to a full node.

The background reaper removes staging records older than `StagingTTL` (default 1h) to recover from abandoned streams.

See [architecture docs](architecture.md#proxy–charon-interaction) for the full streaming protocol.

---

## What This Design Does Not Solve

- **Multi-instance deployments**: Pebble is single-process. Horizontal scaling of Charon requires a distributed backend. This is not yet implemented.

- **Semantic compaction**: Charon stores literal content verbatim. The proxy handles compaction decisions and stores the resulting `compaction` item via the normal store path.

- **DAG history (branching conversations)**: The spec allows `previous_response_id` to form a DAG. Charon's storage accommodates this structurally — branches produce separate node subtrees. Resolution walks each branch independently with no shared-prefix optimisation.
