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
  │  POST /staging (resolve + open staging)     (context resolution + storage)
  │  PUT /staging/{id}/complete (store)
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

- Resolves `previous_response_id` chains into flat `[]Turn` context arrays via `POST /staging`
- Persists request and response blobs to durable Pebble storage
- Manages staging records for in-flight streaming ingest; reaps orphans via background TTL worker
- Runs background workers: TTL expiry, staging reaper

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

### 2. Single embedded storage backend

The storage layer uses [Pebble](https://github.com/cockroachdb/pebble), an embedded key-value store. All data — chain node metadata, payload blobs, LRU accounting, and staging records — lives in a single Pebble database directory. There are no external processes (no SQL server, no object store).

| Deployment level | Storage |
|-----------------|---------|
| Testing / CI    | In-memory Pebble (empty `data_dir`) |
| Development / Production | Pebble on local filesystem |

The binary is identical across all levels. Only `storage.data_dir` changes.

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

### Buffered store: `POST /responses`

The non-streaming path. The proxy sends a complete serialised request blob and response blob in one call. Charon resolves the previous chain, stages the request, and commits the response atomically.

```
POST /responses
body: {
  "response_id": "resp_...",
  "previous_response_id": "resp_..." | null,
  "tenant_key": "",
  "request_blob": "<base64>",
  "response_blob": "<base64>"
}
Response: 201 Created
  X-Depth: <chain depth>
  { "staging_id": "..." }
```

### Streaming path: staging protocol

The streaming path separates resolve from store via a three-step staging protocol, allowing the proxy to begin inference immediately after resolve and deliver response chunks incrementally:

1. **Open staging**: `POST /staging?prev={prevID}` — resolves the prior chain, creates a staging record, returns a `staging_id`. The `flat_context` assembled here is included in the response for the proxy to use when building the inference request.

2. **Append chunks**: `PUT /staging/{id}/chunks/{k}` — delivers one batch of response bytes (0-based offset `k`). Returns next expected offset. Chunks may arrive out of order; Charon sorts at commit time.

3. **Complete staging**: `PUT /staging/{id}/complete?response_id=...&total=...` — seals the staging record and commits the node into the chain store. `total` is the total chunk count.

Additional staging endpoints:
- `PUT /staging/{id}/abort` — marks the staging record as aborted; no node is committed.
- `GET /staging/{id}` — returns staging status (in-progress, complete, or aborted).

### Retrieve: `GET /responses/{id}`

Returns the full stored record for one response — request blob, response blob, depth. No chain walk.

### Delete: `DELETE /responses/{id}`

Point delete — removes the node and its blobs. No effect on other responses in the chain. Background TTL expiry handles bulk eviction.

---

## Response ID Lifecycle

Response IDs visible to clients are assigned by the inference server, not pre-minted by Charon or the proxy. This ensures the stored ID matches the ID the inference server used for its own logging, metrics, and internal correlation.

**Canonical response ID**: The `id` field returned by the inference server (e.g. `resp_xyz` from a vLLM Responses API backend). This is what clients see and what Charon stores as its primary key. Charon treats all IDs as opaque strings.

**Staging ID**: A 128-bit random UUID minted by Charon at `POST /staging` time and returned to the proxy. It is never exposed to clients. It ties the resolved chain context to the subsequent chunk writes and commit. The proxy binds the canonical response ID to the staging ID on the first chunk write (`PUT /staging/{id}/chunks/0?response_id=...`).

**ID flow — continuation:**
```
POST /staging?prev={prev_response_id}  → Charon returns staging_id + flat_context
inference → server assigns canonical_id
PUT /staging/{staging_id}/chunks/{k}?response_id={canonical_id}  (first chunk binds the ID)
PUT /staging/{staging_id}/complete?response_id={canonical_id}&total={N}  → committed
```

**ID flow — new chain (via buffered path):**
```
POST /responses  body: { response_id, previous_response_id=null, request_blob, response_blob }
                 → 201 Created; node committed atomically
```

---

## Streaming Ingest

The staging protocol separates chain resolution from blob commit. This allows the proxy to deliver inference output to Charon incrementally as tokens arrive, without holding all output in proxy memory.

```
POST /staging?prev={prevID}           → staging_id, flat_context (resolve + open staging)
PUT /staging/{id}/chunks/{k}          → next_expected (repeated per batch; out-of-order OK)
PUT /staging/{id}/complete?total={N}  → committed (seal and write node atomically)
```

Chunks are stored by offset (0-based). The proxy may write chunks from concurrent goroutines in any order; Charon assembles them in order at commit time. If the proxy crashes after `POST /staging` but before `PUT .../complete`, the staging record is reaped by the staging TTL worker (background goroutine) and no orphaned node is left in the chain.

**Chunk size trade-offs:**

| Chunk size | Peak proxy memory | Charon write ops | Durability boundary |
|------------|------------------|------------------|---------------------|
| 1 batch | Minimal | One per batch | Per batch |
| Full output | Full output | One commit | On completion only |

The buffered `POST /responses` path is equivalent to a single-chunk staging flow executed atomically.

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

## Deployment Modes

Proxy and Charon are always separate services in production — they run in separate processes, typically on separate hosts. Colocation in the same binary is provided for testing purposes (conformance, compliance, and development iteration).

The current storage backend is [Pebble](https://github.com/cockroachdb/pebble) — an embedded key-value store with no external process dependency. All data (chain metadata, payload blobs, LRU accounting) lives in a single Pebble database directory.

**In-memory Pebble** (`storage.data_dir` empty — conformance and compliance testing)
- All data is lost on restart
- Suitable for running the openresponses.org compliance suite and integration tests

**On-disk Pebble** (`storage.data_dir` set — development and production)
- Data survives restarts; Pebble uses WAL-based crash recovery
- Single-node only: Pebble does not support multi-writer access from separate processes
- Multiple proxy instances may share one Charon process; Charon is the single source of truth for chain state

Multi-instance Charon scaling is not currently implemented.

---

## What This Design Does Not Solve

- **Durable KV cache across restarts**: The inference backend's KV cache is out of scope. This is an infrastructure-level concern orthogonal to Charon's design.

- **Semantic compaction**: Charon stores the literal content of every turn verbatim. Semantic summarization — collapsing prior turns into a shorter representation — is a proxy concern, not a Charon concern. When the proxy calls `POST /responses/compact`, it sends the turns to be compacted to the inference backend, which returns a `compaction` item with opaque `encrypted_content`. The proxy then stores this compaction item via the normal Charon store path. Charon stores the compaction item verbatim alongside the other items in the chain; it does not drop or rewrite prior entries. Which responses to compact and what to do with the resulting item are decisions made by the proxy or the calling application.

- **DAG history (branching conversations)**: The spec allows `previous_response_id` to form a DAG (two responses can share the same parent). Charon's storage design accommodates this structurally — the `chain_root_id` + `position` denormalisation and checkpoint blobs are keyed per chain, and separate branches simply produce separate keys. However, retrieving context from a non-linear DAG path is not specially optimised: each branch is walked independently, and there is no shared-prefix cache across branches. If DAG usage becomes common, branch-aware checkpoint sharing and a prefix cache would reduce redundant reads.

- **Chunked streaming store**: Implemented via the staging protocol. See [Streaming Ingest](#streaming-ingest).
