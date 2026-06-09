# Charon: System Architecture

## Problem Statement

The OpenAI Responses API is *stateful from the client's perspective*: each request can carry a `previous_response_id` that chains it to prior turns, so the client never needs to resend conversation history. But LLM inference is inherently *stateless*: every inference call receives a flat, ordered list of messages and produces output.

Charon bridges this gap. It is an internal service that:

1. Resolves a `previous_response_id` chain into a flat, ordered `[]Item` context
2. Returns that context (plus a new response ID) to the caller before inference
3. Accepts the completed inference output for durable storage after inference

Charon is **not** the client-facing API layer. It is called by a proxy that owns the
client surface (e.g., `/v1/responses/` API).

---

## System Components

```
Client
  ↓  OpenAI Responses API (HTTP / SSE / WebSocket)
Proxy ──────────────────────────────────────► Charon
  │  GET /responses/{id}/context (resolve)      (context resolution + storage)
  │  POST /responses/{id}        (store)
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

### Proxy

The proxy owns all client-facing concerns:

- HTTP transport: REST, SSE streaming, WebSocket
- Authentication and TLS termination
- Connection-local ephemeral cache for `store: false` responses (WebSocket sessions)
- Request validation and routing
- Streaming inference output back to the client

### Charon

Charon owns storage and resolution:

- Resolves `previous_response_id` chains into flat `[]Item` context arrays
- Assigns reservation IDs for write-intent correlation; canonical response IDs are assigned by the inference server
- Persists response payloads (input items, output items) to durable storage
- Manages write-intent safety across the index and payload stores
- Runs background workers: TTL expiry, write-intent recovery

Charon does **not** own: SSE, WebSocket, auth, TLS, model routing, or `store: false` semantics.

---

### Proxy–Charon interaction

The proxy calls Charon differently depending on whether this is a new chain or a continuation:

**New chain** (no `previous_response_id`):
- No Charon resolve call. Proxy calls inference with `flat_context=[]` + `input[]`.
- The inference server assigns the canonical response ID, returned in the first streaming chunk or response body.
- Proxy emits `response.created` to the client using that inference-server-assigned ID.
- If `store: true`: proxy calls `POST /responses/{canonical_id}` to store.

**Continuation** (has `previous_response_id`):
1. **Before inference** — resolve: `GET /responses/{previous_response_id}/context`, receives `{reservation_id, flat_context[]}`. Charon returns assembled history and a short-lived reservation ID for write-intent correlation. No write-intent is created yet.
2. **Start inference** — proxy appends the new `input[]` to `flat_context` and forwards to the inference server. The inference server's first streaming chunk carries the canonical response ID.
3. **Client notification** — proxy emits `response.created` to the client using the inference-server-assigned canonical ID.
4. **After inference** — store: `POST /responses/{canonical_id}` with `{reservation_id, previous_response_id, input[], output[], usage, status}`. Charon atomically creates the write-intent and commits the payload.

If inference fails before the canonical ID is known (no data returned at all), the proxy uses the `reservation_id` as the fallback response ID and calls `POST /responses/{reservation_id}` with `status: "failed"`. If the canonical ID was already received, the proxy uses it.

If `store: false` is set, the proxy skips the store call entirely. No write-intent is ever created. The `store: false` flag is a proxy-level concern; Charon is unaware of it.

---

## Core Design Principles

### 1. Stateless front end

The proxy holds no in-process conversation state (except the ephemeral per-connection cache for `store: false` WebSocket sessions). Any request can be served by any proxy instance. State lives exclusively in Charon's storage layer.

The inference backend is similarly stateless. It always receives the complete flat context assembled by Charon.

### 2. Replaceable storage backends

The storage layer is abstracted behind two interfaces:

```
IndexStore   — manages metadata and chain linkage (response ID, previous ID, position)
PayloadStore — manages content (input items, output items, the actual conversation content)
```

Application logic calls only these interfaces. Concrete backends are injected at startup via configuration:

| Deployment level | IndexStore | PayloadStore |
|-----------------|------------|--------------|
| Memory only (testing) | In-memory | In-memory |
| Single binary   | SQLite     | Local filesystem |
| Multi-instance  | PostgreSQL | MinIO / S3   |

The binary is identical across all levels. Only configuration changes.

### 3. User Inputs, not KV cache

The storage layer persists conversation history as serialized items (text, tool calls, tool outputs). It does not attempt to persist the inference engine's KV cache.

The expansion factor makes KV cache storage impractical (three orders of magnitude expansion).

Alternative: Tokens are portable across model versions and inference backends and could potentially be used instead.

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

**Implementation strategies:**

| Strategy | Write cost | Read cost | Storage cost | Notes |
|----------|-----------|-----------|--------------|-------|
| Entry-per-response | O(1) | O(N) — walk chain | O(N) total | Storage efficient; latency grows with chain depth |
| Full-snapshot-per-response | O(N) | O(1) — single fetch | O(N²) total | Write amplification; simplest reads |
| Checkpoint every K turns | O(1) amortized | O(K) — 1 checkpoint + ≤K deltas | O(N²/K) total | Trades storage for bounded read latency |

Each checkpoint blob is **cumulative** — it contains all turns from chain root to its position. The checkpoint at position nK holds nK payloads. Total checkpoint storage across a chain of length N grows as K + 2K + ... + (N/K)·K ≈ N²/(2K). With K=10 and a 1000-turn chain, checkpoint storage is roughly 50× the delta storage — the cost paid for O(K) reads with a single self-contained checkpoint fetch and no chaining.

An alternative **incremental checkpoint** design stores only the K new turns at each checkpoint position and keeps a back-reference to the prior checkpoint. This reduces storage to O(N) but requires loading ⌈N/K⌉ checkpoint blobs in sequence to reconstruct the full context — O(N/K) chained reads instead of O(K) reads. For read-heavy workloads the current cumulative design is preferable; for write-heavy workloads with very long chains the incremental design trades read latency for storage efficiency.

Charon uses the checkpoint strategy. See [storage design](storage.md) for details.

---

## Charon API

Charon exposes an internal HTTP API consumed only by the proxy. It is **not** required to conform to the OpenAI Responses API specification — it is designed for operational efficiency as an internal service.

The two read endpoints reflect two distinct proxy needs:

- **Resolve** (`GET /responses/{id}/context`): the proxy is about to call inference and needs the assembled chain as a flat context. Requires a full chain walk (up to K steps with checkpoints), multiple payload reads, and a new minted `reservation_id` for the upcoming response. Called before every continuation inference turn. The inference backend
returns the real response ID that can be used in future requests.

- **Retrieve** (`GET /responses/{id}`): the proxy is serving a client read request (`GET /responses/{id}` on the client-facing API). The client wants that specific turn's stored data — input items, output items, usage, status. No chain walk. No new ID. Only that one record. Using resolve here would walk the entire chain and mint a wasted ID.

These are different operations with different cost and different response shapes. The sub-resource `/context` reflects this: the base path is the stored resource itself; `/context` is a derived view assembled from the chain.

### Resolve: `GET /responses/{id}/context`

Called by the proxy before inference, for continuation turns only. Walks the stored chain and assembles `flat_context`. Returns a `reservation_id` for write-intent correlation — this is not the canonical response ID visible to clients.

```
GET /responses/{previous_response_id}/context

Response:
  { "reservation_id": "rsrv_...", "flat_context": [...] }
```

`flat_context` is the assembled history (all prior input/output items). The proxy appends the new `input[]` before forwarding to the inference backend. The canonical response ID is assigned by the inference server in its first streaming chunk or response body — the proxy uses that ID in the `response.created` event to the client and in the subsequent store call.

New chains skip this call — the proxy calls inference directly and uses the ID assigned by the inference server for the store call.

### Store: `POST /responses/{id}`

Called by the proxy after inference completes. The write-intent is created and resolved atomically within this call.

```
POST /responses/{canonical_response_id}
body: {
  "reservation_id": "rsrv_...",        // from preceding resolve; omitted for new chains
  "previous_response_id": "resp_...",  // null for new chains
  "input": [...],
  "output": [...],
  "usage": {...},
  "status": "completed" | "failed",
  ...
}
```

`canonical_response_id` in the path is the ID assigned by the inference server. `reservation_id` correlates this store call to the preceding resolve; Charon uses it for write-intent tracking and logging. On `status: "failed"`, Charon records the failure and skips payload write. Store calls are safe to retry: the payload key is derived from `canonical_response_id`, so a repeated PUT writes identical bytes to the same key.

### Retrieve: `GET /responses/{id}`

Called by the proxy to serve client-facing read requests (`GET /responses/{id}`, list sub-resources, pagination). Returns the full stored record for that single response — input items, output items, usage, status, metadata. No chain walk.

The proxy projects whatever fields or sub-resources the client requested from the returned record.

### Delete: `DELETE /responses/{id}`

Called by the proxy to serve `DELETE /responses/{id}` on the client-facing API. Point delete — no effect on other responses in the chain. Hard deletion may be deferred to the TTL worker.

---

## Response ID Lifecycle

Response IDs visible to clients are assigned by the inference server, not pre-minted by Charon or the proxy. This ensures the stored ID matches the ID the inference server used for its own logging, metrics, and internal correlation.

**Canonical response ID**: The `id` field returned by the inference server (e.g. `resp_xyz` from a vLLM Responses API backend, or translated from `chatcmpl-xyz` for chat-completions backends). This is what clients see and what Charon stores as its primary key.

**Reservation ID**: A short-lived internal identifier minted by Charon at resolve time. It is never exposed to clients. It serves two purposes:
- Write-intent correlation: the store call carries the `reservation_id` so Charon can match the resolve → store pair in logs and future write-intent pre-reservation.
- Failure fallback: if inference fails before returning any data (no canonical ID yet known), the proxy reports the failure using the `reservation_id` as the path parameter.

**ID flow — continuation:**
```
resolve → Charon returns reservation_id
inference → server assigns canonical_id (in first streaming chunk)
proxy sends response.created to client with canonical_id
store → POST /responses/{canonical_id}  body carries reservation_id
```

**ID flow — new chain:**
```
inference → server assigns canonical_id (in first streaming chunk)
proxy sends response.created to client with canonical_id
store → POST /responses/{canonical_id}  (no reservation_id)
```

**ID format**: The `resp_` prefix (`resp_<32-char hex>`) is the convention Charon expects for canonical IDs. When the inference backend returns IDs in a different format (e.g. `chatcmpl-` for OpenAI-style chat completions), the proxy translates them. Reservation IDs use a `rsrv_` prefix to distinguish them from canonical IDs in logs. Charon treats all IDs as opaque strings beyond their prefix convention.

---

## Streaming Store Modes

The proxy delivers inference output to Charon along a spectrum from fully buffered to token-at-a-time. All modes target the same store endpoint; the difference is when and in what granularity the payload arrives.

```
Fully buffered ◄────────────────────────────────────► Token-at-a-time
(single POST, peak memory = full output,    (many writes, peak memory = 1 token,
 no partial recovery)                        full partial recovery)
```

### Mode 1: Fully Buffered (Phase 1)

The proxy accumulates all inference output before calling store. A single `POST` carries the complete payload.

```
vLLM ──(stream tokens)──► Proxy  [buffers all]
Proxy ──POST /responses/{id} { complete payload } ──► Charon
```

Write-intent lifecycle: created and resolved in a single atomic store call. Peak proxy memory is proportional to total output length. If the proxy crashes before calling store, the response is lost with no partial recovery. This is the Phase 1 implementation — simplest Charon write path, no streaming protocol needed.

### Mode 2: Chunked (N tokens per chunk)

The proxy forwards output to Charon in batches as tokens arrive from vLLM. Chunk size is configurable from 1 to N output items.

Using separate calls per chunk:

```
vLLM ──(stream tokens)──► Proxy
Proxy ──POST /responses/{id}                                ──► Charon  (open stream, create write-intent)
Proxy ──PATCH /responses/{id} { items: [...] }              ──► Charon  (repeated per chunk)
...
Proxy ──PATCH /responses/{id} { items: [...], usage, status }──► Charon  (final chunk, close + commit)
```

Or equivalently, a single `POST` with `Transfer-Encoding: chunked` where each HTTP chunk is an ndjson line:

```
POST /responses/{id}
Content-Type: application/x-ndjson
Transfer-Encoding: chunked

{"type":"chunk","items":[...]}
{"type":"chunk","items":[...]}
{"type":"final","items":[...],"usage":{...},"status":"completed"}
```

Write-intent phases for chunked mode: `stream_open` → (chunks accumulating) → `committed` | `failed`. The recovery worker identifies streams stale beyond the threshold and marks them `failed`, preserving any partially written chunks for debugging.

**Seq field and ordering**: each `PATCH` chunk carries a `seq` integer (0-based). Charon stores chunks indexed by seq and sorts at commit time. This allows the proxy to dispatch chunk writes from concurrent goroutines without requiring arrival order — each goroutine is assigned a seq before being spawned. The proxy does not call commit until all chunk writes have returned successfully, so Charon always has a complete set of chunks by the time it assembles the payload. Gaps (a chunk seq with no corresponding PATCH) indicate a dropped request; the proxy must retry that chunk before committing.

**Chunk size trade-offs:**

| Chunk size | Peak proxy memory | Charon write ops | Durability boundary |
|------------|------------------|------------------|---------------------|
| 1 token | Minimal | One per token | Per token |
| N tokens | N tokens | One per N tokens | Per N tokens |
| Full output | Full output | One (Mode 1) | On completion only |

### Mode 3: Token-at-a-Time

The extreme end of Mode 2 with chunk size = 1. Every output token is forwarded to Charon immediately. Maximum durability — Charon has all output up to the crash point if the proxy fails mid-stream. Practical use is limited to latency-insensitive workloads where recovery granularity matters more than write overhead.

### Checkpoint Interaction

Checkpoint writes (every K turns) occur at stream close regardless of streaming mode. The full assembled `flat_context` — needed to materialize the checkpoint blob — is only known once all output items for the turn have arrived. In chunked mode, Charon accumulates received chunks and writes the checkpoint atomically at stream commit.

---

## What Is and Isn't Persisted

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

When the client sets `store: false`, the proxy skips the store call to Charon after inference. The response is never written to durable storage.

For WebSocket connections, the proxy maintains a connection-local in-memory cache of `store: false` responses so that `previous_response_id` lookups within the same connection still work. On disconnect, this cache is lost. A reconnecting client that references a `store: false` response ID receives `previous_response_not_found` and must start a new chain.

Charon has no knowledge of `store: false`. Because write-intents are created only at store time (not at resolve time), a minted response_id that never receives a store call leaves no orphaned state in Charon — there is nothing to clean up.

---

## Scaling Path

Proxy and Charon are always separate services in production — they run in separate processes, typically on separate hosts. Colocation in the same binary is provided only for testing purposes (conformance, compliance, and development iteration).

**Memory only** (conformance and compliance testing)
- IndexStore: in-memory
- PayloadStore: in-memory
- No persistence across restarts; suitable for running the openresponses.org compliance suite and integration tests

**Single binary** (development and compliance testing)
- IndexStore: embedded SQLite (no external process)
- PayloadStore: local filesystem
- Proxy and Charon in the same process on `:8080` (proxy) and `:8081` (Charon internal API)
- Not intended for production; data survives restarts but there is no horizontal scaling

**Production — single Charon instance**
- Proxy and Charon run as separate services (separate hosts or containers)
- IndexStore: PostgreSQL
- PayloadStore: object store (MinIO/S3)
- Multiple proxy instances share one Charon service; Charon is the single source of truth for chain state

**Production — scaled Charon**
- Same as above but Charon itself is horizontally scaled behind a load balancer
- Requires distributed locking or sharding for write-intent safety across Charon instances
- IndexStore: PostgreSQL with connection pooling (write-intent coordination via advisory locks or row-level locking)
- PayloadStore: distributed object store (MinIO cluster or S3)

The distinction between the last two levels is whether Charon is a single bottleneck or itself scaled. Most deployments start with a single Charon instance and scale it only when write throughput is measurably constrained.

---

## What This Design Does Not Solve

- **Durable KV cache across restarts**: The inference backend's KV cache is out of scope. This is an infrastructure-level concern orthogonal to Charon's design.

- **Semantic compaction**: Charon stores the literal content of every turn verbatim. Semantic summarization — collapsing prior turns into a shorter representation — is a proxy concern, not a Charon concern. When the proxy calls `POST /responses/compact`, it sends the turns to be compacted to the inference backend, which returns a `compaction` item with opaque `encrypted_content`. The proxy then stores this compaction item via the normal Charon store path. Charon stores the compaction item verbatim alongside the other items in the chain; it does not drop or rewrite prior entries. Which responses to compact and what to do with the resulting item are decisions made by the proxy or the calling application.

- **DAG history (branching conversations)**: The spec allows `previous_response_id` to form a DAG (two responses can share the same parent). Charon's storage design accommodates this structurally — the `chain_root_id` + `position` denormalisation and checkpoint blobs are keyed per chain, and separate branches simply produce separate keys. However, retrieving context from a non-linear DAG path is not specially optimised: each branch is walked independently, and there is no shared-prefix cache across branches. If DAG usage becomes common, branch-aware checkpoint sharing and a prefix cache would reduce redundant reads.

- **Chunked streaming store**: Implemented. The proxy delivers output to Charon in configurable-size batches via `PATCH /responses/{id}` as inference tokens arrive. See [Streaming Store Modes](#streaming-store-modes).
