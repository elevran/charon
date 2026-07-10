# Charon: System Architecture

## Problem Statement

The OpenAI Responses API is *stateful from the client's perspective*: each request can carry a `previous_response_id` that chains it to prior turns, so the client never needs to resend conversation history. But LLM inference is inherently *stateless*: every inference call receives a flat, ordered list of messages and produces output.

Charon bridges this gap. It is an internal service that:

1. Resolves a `previous_response_id` chain into a flat, ordered `[]Item` context
2. Returns that context (plus a staging ID) to the caller before inference; the staging ID is replaced by the inference server's canonical response ID once the response is committed
3. Accepts the completed inference output for durable storage after inference

Charon is **not** the client-facing API layer. It is called by a proxy that owns the
client surface (e.g., `/v1/responses/` API).

---

## System Components

```
Client
  ↓  OpenAI Responses API (HTTP / SSE / WebSocket)
Proxy ──────────────────────────────────────► Charon
  │  POST /staging (open staging, resolve chain)   (context resolution + storage)
  │  PUT /staging/{id}/chunks/{k} (append chunk)
  │  PUT /staging/{id}/complete (commit)
  │  GET /chain/{id} (read-only chain fetch, no commit)
  │  GET /responses/{id} (point retrieve)
  │
  ↓  stateless Responses API (full flat_context as input)
Inference Backend (OpenAI-compatible)
  │  returns response with canonical ID
  ↑
Proxy streams output back to client
```

Proxy and Charon are **peers**: the proxy calls Charon to resolve prior context before
inference and to store results after inference. The proxy calls the inference backend
directly — Charon is never in the inference call path.

**Why two components?** The proxy and Charon have different scaling axes and different rates of change. Proxy instances are stateless and scale horizontally without coordination. Charon scales independently based on storage throughput — a single Charon instance can serve many proxy instances. `store: false` semantics and connection-local WebSocket caches are proxy concerns; tying them to Charon would require Charon to understand connection lifecycle. Auth, TLS, and streaming protocol details change independently of storage logic and belong in the component that owns the client surface.

### Proxy

The proxy owns all client-facing concerns:

- HTTP transport: REST, SSE streaming, WebSocket
- Authentication and TLS termination
- Connection-local ephemeral cache for `store: false` responses (WebSocket sessions)
- Request validation and routing
- Streaming inference output back to the client
- Multi-tenancy: provides an optional tenant identifier with each Charon call; Charon uses it to namespace responses across tenants
- Strips non-chain-reconstruction fields from requests/responses before forwarding to Charon (Charon stores the resulting blobs opaquely)

### Charon

Charon owns storage and resolution:

- Resolves `previous_response_id` chains into flat `[]Turn` context arrays
- Persists request and response blobs to durable storage
- Manages staging records for in-flight streaming ingest; reaps orphans via background TTL worker
- Runs background workers: TTL expiry, staging reaper

---

### Proxy–Charon interaction

The proxy calls Charon differently depending on whether the response will be stored and whether the turn has a prior context to fetch.

**New chain** (`store: true`, no `previous_response_id`):
1. **Open staging**: `POST /staging` (no `prev` param) — Charon creates a staging record and returns a `staging_id`; `flat_context` is empty.
2. **Inference**: proxy calls inference with `flat_context=[]` + `input[]`. The inference server assigns the canonical response ID.
3. **Append chunks**: proxy delivers response bytes via `PUT /staging/{staging_id}/chunks/{k}`.
4. **Complete**: `PUT /staging/{staging_id}/complete?response_id={canonical_id}&total={N}` — Charon commits the node.

**Continuation** (`store: true`, `previous_response_id` present):
1. **Open staging**: `POST /staging?prev={previous_response_id}` — Charon resolves the prior chain, creates a staging record, returns `{staging_id, flat_context[]}`.
2. **Inference**: proxy appends new `input[]` to `flat_context` and forwards to the inference server. The inference server assigns the canonical response ID.
3. **Append chunks**: proxy delivers response bytes via `PUT /staging/{staging_id}/chunks/{k}`.
4. **Complete**: `PUT /staging/{staging_id}/complete?response_id={canonical_id}&total={N}` — Charon seals the staging record and commits the node atomically.

**`store: false`** (no persistence):
- The proxy skips the staging open call entirely.
- If `previous_response_id` is present, the proxy fetches context via `GET /chain/{previous_response_id}` (read-only, no staging record opened).
- No chunks are written and no commit is issued.

---

## Core Design Principles

### 1. Stateless front end

The proxy holds no durable state — its state is bounded to the lifetime of a single request/response (except the ephemeral per-connection cache for `store: false` WebSocket sessions). Any request can be served by any proxy instance. State lives exclusively in Charon's storage layer.

The inference backend is similarly stateless. It always receives the complete flat context assembled by Charon.

### 2. Single embedded storage backend

The storage layer uses [Pebble](https://github.com/cockroachdb/pebble), an embedded key-value store. All data — chain node metadata, payload blobs, LRU accounting, and staging records — lives in a single Pebble database directory. There are no external processes (no SQL server, no object store).

| Deployment level | Storage |
|-----------------|---------|
| Testing / CI    | In-memory Pebble (empty `data_dir`) |
| Development / Production | Pebble on local filesystem |

The binary is identical across all levels. Only `storage.data_dir` changes.

---

## The Stateful-to-Stateless Translation

This is the central operation Charon performs on every resolve call that carries `previous_response_id`.

**Logical model:** responses form a singly-linked list. Each response stores a pointer to its predecessor.

```
resp_A (root)  ←  resp_B  ←  resp_C  ←  resp_D (head)
```

**Translation to flat context:**

```
[resp_A.input] [resp_A.output]
[resp_B.input] [resp_B.output]
[resp_C.input] [resp_C.output]
[resp_D.input] [resp_D.output]   ← previous turn; new input is appended by the proxy
```

The proxy appends the new request's `input[]` to the flat context before forwarding to the inference backend.

Charon uses an entry-per-turn strategy: each response stores only its own turn, and the full context is assembled by walking the chain on each resolve call. See [storage design](storage.md) for the strategy trade-offs.

---

## Charon API

Charon exposes an internal HTTP API consumed only by the proxy. It is **not** required to conform to the OpenAI Responses API specification — it is designed for operational efficiency as an internal service.

### Staging protocol: `POST /staging` → chunks → complete

All store paths (new chain and continuation) use the staging protocol:

1. **Open staging**: `POST /staging?prev={prevID}` — resolves the prior chain (if `prev` is present), creates a staging record, returns `{staging_id, turns[]}`.

2. **Append chunks**: `PUT /staging/{id}/chunks/{k}` — delivers one batch of response bytes (0-based index `k`). The first chunk call binds the canonical response ID via `?response_id=...`.

3. **Complete staging**: `PUT /staging/{id}/complete?response_id=...&total=...` — seals the staging record and commits the node into the chain store. `total` is the total number of chunks.

Additional staging endpoints:
- `PUT /staging/{id}/abort` — marks the staging record as aborted; no node is committed. *(under review — may be removed if the TTL reaper provides sufficient cleanup)*
- `GET /staging/{id}` — returns staging status (in-progress, complete, or aborted).

### Read-only chain fetch: `GET /chain/{id}`

Returns the full assembled flat context for the chain rooted at `{id}` without opening a staging record or committing the current turn's request blob. Used by the proxy for `store: false` continuations.

### Retrieve: `GET /responses/{id}`

Returns the stored record for one response — request blob, response blob, depth. No chain walk.

### Delete: `DELETE /responses/{id}`

Point delete — removes the node and its blobs. No effect on other responses in the chain. Background TTL expiry handles bulk eviction.

### Buffered store: `POST /responses`

Commits a complete turn (both request and response blobs) in a single call. Used in tests; in production the proxy uses the staging protocol for all paths.

```
POST /responses
X-Tenant-Key: <tenant>
body: {
  "prev_id":       "resp_..." | <absent>,
  "response_id":   "resp_..." | <absent>,
  "request_blob":  "<base64>",
  "response_blob": "<base64>"
}
Response: 201 Created
  X-Depth: <chain depth>
```

---

## Response ID Lifecycle

Response IDs visible to clients are assigned by the inference server, not pre-minted by Charon or the proxy. This ensures the stored ID matches the ID the inference server used for its own logging, metrics, and internal correlation.

**Canonical response ID**: The `id` field returned by the inference server (e.g. `resp_xyz` from a vLLM Responses API backend). This is what clients see and what Charon stores as its primary key. Charon treats all IDs as opaque strings.

**Staging ID**: A 128-bit random UUID minted by Charon at `POST /staging` time and returned to the proxy. It is never exposed to clients. It ties the resolved chain context to the subsequent chunk writes and commit. The proxy binds the canonical response ID to the staging ID on the first chunk write (`PUT /staging/{id}/chunks/0?response_id=...`).

**ID flow — new chain or continuation (store: true):**
```
POST /staging[?prev={prev_response_id}]  → Charon returns staging_id + flat_context
inference → server assigns canonical_id
PUT /staging/{staging_id}/chunks/{k}?response_id={canonical_id}  (first chunk binds the ID)
PUT /staging/{staging_id}/complete?response_id={canonical_id}&total={N}  → committed
```

**ID flow — store: false continuation:**
```
GET /chain/{prev_response_id}  → Charon returns flat_context (no staging record)
inference → server assigns canonical_id (not stored)
```

---

## What Is and Isn't Persisted

The proxy is responsible for constructing the blobs sent to Charon — Charon stores them opaquely without parsing message content.

### Must be persisted (for chain reconstruction)

| Field | Why |
|-------|-----|
| `id` | Primary key for chain lookups |
| `previous_response_id` | Chain linkage |
| `input` items | The user/tool side of the turn |
| `output` items | The assistant side of the turn |
| `output[*].encrypted_content` | Opaque blob for reasoning/compaction items — must be returned verbatim |

### Must NOT be injected from stored history

| Field | Why |
|-------|-----|
| `instructions` | Re-supplied per request by the proxy; excluded from history by spec design to allow system prompt changes mid-conversation |
| Sampling params (`temperature`, `top_p`, etc.) | Per-request inference config |
| `tools`, `tool_choice` | Per-request config |

### Operational metadata (persist for API completeness, not chain reconstruction)

`status`, `created_at`, `model`, `usage`, `error`, `metadata`, `service_tier`, etc.

---

## `store: false` Semantics

When the client sets `store: false`, the proxy skips all Charon store calls — no staging record is opened, no chunks are written, and no commit is issued. The response is never written to durable storage.

For continuations with `store: false`, the proxy fetches prior context via `GET /chain/{previous_response_id}` (read-only, no staging record). For first turns with `store: false`, no Charon call is made at all.

For WebSocket connections, the proxy maintains a connection-local in-memory cache of `store: false` responses so that `previous_response_id` lookups within the same connection still work. On disconnect, this cache is lost. A reconnecting client that references a `store: false` response ID receives `previous_response_not_found` and must start a new chain.

Charon has no knowledge of `store: false`; the proxy simply omits the store calls.

---

## Deployment Modes

Proxy and Charon are separate services in production — separate processes, typically on separate hosts.

For testing and development, proxy and Charon may be colocated in the same binary. The proxy calls Charon's service layer directly (in-process, no HTTP hop). The binary is identical across all deployment levels; only `storage.data_dir` and service configuration change.

---

## Performance and Scale

### Access patterns

Charon's workload is append-only point-lookup and sequential parent-pointer walk — no joins, no predicate scans over payload content. These patterns fit an embedded key-value store far better than a relational database.

### Single-server throughput expectations

The in-memory LRU blob cache (`chainCache`, default 64 MiB) keeps recently accessed turns in RAM; the cache-warm and cache-cold paths below reflect whether a given node's blob is cached.

These are order-of-magnitude estimates for a single Charon instance on commodity server hardware (8–32 cores, NVMe SSD). They are not benchmarks; actual numbers depend on blob size, chain depth, and hardware.

**Write throughput — new response (`PUT /staging/{id}/complete`):**
Dominated by the Pebble write batch (WAL append + memtable insert). A single atomic commit writes one node record and one or two blob values. With Pebble's default write options, expect low thousands of commits per second at blob sizes of 10–100 KB. The staging flush path issues two Pebble commits per turn — one for the staging node, one for the final node — which roughly halves peak write throughput versus a single-commit path.

**Read throughput — `GET /responses/{id}`:**
`Retrieve` issues a single `GetNode` and `GetBlobs` call. No chain walk, no LRU touch. On a warm block cache, expect sub-millisecond p50 latency and tens of thousands of reads per second for typical blob sizes.

**Chain resolution latency — `POST /staging?prev=...` or `GET /chain/{id}`:**
`walkAndTouch` calls `backend.LoadChain` (N sequential `GetNode` reads from leaf to root) followed by up to N `GetBlobs` calls for nodes not in the in-memory cache. Latency scales approximately linearly with chain depth N:

- Cache-cold path: ~N Pebble key reads + ~N blob fetches. At 100 µs per Pebble read, a depth-10 chain resolves in roughly 2–5 ms; depth-100 in 20–50 ms.
- Cache-warm path: `LoadChain` still runs (N `GetNode` reads to walk the linked list and touch `LastAccessUnix`), but blob fetches for cached nodes are skipped. A fully cached depth-10 chain resolves in ~1–2 ms; depth-100 in ~10–20 ms.

After resolution, `walkAndTouch` issues one additional `backend.Commit` to update `LastAccessUnix` and any `BucketID` promotions for nodes that have crossed an LRU bucket boundary.

**Cache hit rate:**
The in-memory LRU cache (`chainCache`) stores per-node turns keyed by `NodeID`. Cache capacity is bounded by total blob bytes (`Config.CacheMaxBytes`, default `DefaultCacheMaxBytes` = 64 MiB). The cache TTL defaults to `DefaultCacheTTL` = 5 minutes.

Agentic workloads are append-heavy: each new turn appends one node to an existing chain and then immediately resolves the updated chain. Because cache entries are per-node (not per-chain), ancestor blobs are retained across turns. For short-to-medium chains (depth ≤ 50) where the working set fits in 64 MiB, nearly every blob fetch after the first resolve is a cache hit. Cache misses occur on cold start, after cache eviction due to byte pressure, or when `CacheTTL` expires (default 5 min of inactivity).

### Scaling limits

Pebble is a single-process embedded store. Charon inherits this constraint: a single Charon instance is the sole writer to its data directory. Multiple Charon instances cannot share one Pebble directory.

**Vertical scaling** (larger machine, more memory for Pebble's block cache and Charon's in-memory LRU) improves throughput and cache hit rate within a single server. This is the recommended path for most deployments.

**Horizontal scaling** is not currently implemented. When a single server is insufficient, the workload can be sharded by response ID prefix or tenant key — each shard runs an independent Charon instance with its own Pebble directory. A sharding proxy layer in front of Charon instances routes requests by key prefix. This is a deployment-level concern; Charon's API and storage format do not need to change.

A distributed KV backend (DynamoDB is the most likely candidate, though not yet implemented) is an alternative path to horizontal scale. The `Backend` interface in `internal/chainstore/backend.go` decouples the store logic from the Pebble implementation, so a distributed backend can be wired in without changing the chain-walk or staging logic.

The current architecture intentionally targets single-server deployments.

### Compression and TTL

**TTL-based reaping** (`Config.TTL`, `Config.StagingTTL`): The background `ttlLoop` goroutine scans LRU bucket keys periodically (`Config.TTLInterval`, default 5 min) and deletes chain nodes older than `Config.TTL`. `Config.StagingTTL` (default 1 h when non-zero) governs reaping of orphaned staging records left by proxy crashes. Both fields default to zero (disabled) and must be set explicitly for production deployments.

**Payload compression** (issue #37): Not yet implemented. When added, compressed blob sizes will directly reduce both Pebble SSTable footprint and the byte pressure on the in-memory LRU cache — a smaller `CacheMaxBytes` would cache the same number of turns. The `Config.CacheMaxBytes` field already tracks blob bytes so the accounting will remain correct after compression is introduced.

**Capacity eviction** (`Config.MaxEntries`, `Config.MaxBytes`): The background `evictionLoop` goroutine evicts the oldest LRU-bucket entries when the store exceeds either bound. Combined with `Config.TTL`, this keeps steady-state disk usage bounded without requiring manual intervention.

---

## What This Design Does Not Solve

- **Semantic compaction**: Charon stores the literal content of every turn verbatim. Semantic summarization — collapsing prior turns into a shorter representation — is a proxy concern, not a Charon concern. When the proxy calls `POST /responses/compact`, it sends the turns to be compacted to the inference backend, which returns a `compaction` item with opaque `encrypted_content`. The proxy then stores this compaction item via the normal Charon store path. Charon stores the compaction item verbatim alongside the other items in the chain; it does not drop or rewrite prior entries. Which responses to compact and what to do with the resulting item are decisions made by the proxy or the calling application.

- **DAG history (branching conversations)**: The spec allows `previous_response_id` to form a DAG (two responses can share the same parent). Charon's storage design accommodates this structurally — separate branches produce separate node subtrees. However, retrieving context from a non-linear DAG path is not specially optimised: each branch is walked independently, and there is no shared-prefix cache across branches.

- **Chunked streaming store**: Implemented via the staging protocol. See [Streaming Ingest](#proxy–charon-interaction) for the full flow.
