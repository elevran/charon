# Charon: Implementation Plan

## Language Choice: Go

**Go is the right choice for this workload.** The decision rests on four concrete factors:

### 1. The workload is I/O bound, not CPU bound

Every hot path — SQLite writes, filesystem reads, HTTP handling, zstd compression, SSE streaming — spends its time waiting on I/O. Rust's primary advantages (zero-cost abstractions, fine-grained memory control, deterministic allocation) pay dividends on CPU-bound work. For I/O-bound code that blocks on disk or network syscalls, Go and Rust produce programs with effectively identical runtime characteristics. There is no measurable performance gap for this use case.

### 2. SQLite integration is frictionless in Go

`modernc.org/sqlite` is pure Go — no CGo, no C toolchain dependency. It cross-compiles to any target with `GOOS`/`GOARCH` and produces a fully static binary with `CGO_ENABLED=0`. This is the single most important practical difference for the single-binary deployment goal.

In Rust, `rusqlite` with the `bundled` feature links SQLite via the `cc` crate, which requires a C compiler at build time. Cross-compiling to a different target (e.g., building on macOS for a Linux container) requires a configured C cross-toolchain — real friction that simply does not exist in Go.

### 3. Streaming and concurrency model fits naturally

The background write-intent recovery worker, TTL expiry worker, and per-request chain-walk goroutines are all naturally expressed as goroutines communicating over channels, with `context.Context` for cancellation and deadlines. This maps directly onto the problem.

The Rust equivalent — `tokio` tasks, `Arc<Mutex<State>>`, `await` across lock boundaries, fighting the borrow checker across yield points — is correct but adds ceremony to what is sequential background bookkeeping.

### 4. The repository is already Go

The workspace path (`go/src/github.com/elevran/charon`) establishes Go as the project language, minimizing contributor ramp-up.

### Where Rust would be better

Rust's `serde` + enum variants would be ideal for the discriminated union of output item types (`message`, `function_call`, `function_call_output`, `reasoning`, `compaction`). In Go, this requires an interface with type-switch or a tagged struct with `json.RawMessage` plus a second unmarshal pass — boilerplate for every variant. **Mitigation:** generate the Go types from the OpenAPI spec rather than hand-writing them. This eliminates most of the maintenance cost.

---

## Architecture Summary

```
Proxy (separate repo)
    ↓  GET /responses/{id}       resolve: flat_context + new response_id
    ↓  POST /responses/{id}      store: write-intent + payload commit
Charon HTTP API
    ↓
Conversation Service
    ↓ IndexStore interface      ↓ PayloadStore interface
  SQLite                       Local filesystem
  (Phase 2: Postgres)          (Phase 2: S3/MinIO)
```

Charon is an internal service — not client-facing. The proxy (separate repository) owns SSE, WebSocket, auth, and the OpenAI Responses API surface. Charon owns chain resolution, payload persistence, and write-intent safety. Backends are injected at startup from configuration. See [architecture.md](architecture.md) and [storage.md](storage.md) for design details.

---

## Phase 1: Single Executable

**Goal:** A working server that implements the Responses API with durable persistence, correct multi-turn history, and no external dependencies beyond the inference backend.

**Deliverables:**
- Single binary deployable with `./charon --config config.yaml`
- IndexStore: embedded SQLite via `modernc.org/sqlite`
- PayloadStore: local filesystem (structured directory tree)
- Charon internal HTTP API (`net/http` with `chi` router):
  - `GET /responses/{id}/context` — resolve: chain walk, mint response ID, return flat_context (called before inference, continuations only)
  - `POST /responses/{id}` — store: write-intent + payload commit (called after inference)
  - `GET /responses/{id}` — retrieve: return single stored response record (called to serve client reads)
  - `DELETE /responses/{id}` — delete: point delete, no cascade
  - `GET /metrics` — Prometheus metrics scrape endpoint
- Correct `previous_response_id` chain reconstruction
- Checkpoint every N turns (configurable, default 10)
- TTL expiry background worker
- Write-intent recovery background worker
- In-memory IndexStore + PayloadStore for tests
- Unit and integration test suite

**Basic observability (Phase 1):**
- HTTP request count and latency per endpoint (`resolve`, `store`, `retrieve`, `delete`)
- Write-intent failure count (critical correctness signal)
- Chain depth at resolve time (histogram — understand usage patterns)
- Active write-intents gauge (detect stuck intents)

**Non-goals for Phase 1:**
- SSE streaming, WebSocket (proxy concerns — separate repo)
- `/responses/compact` (proxy concern — requires inference client)
- Multi-node deployment
- Horizontal scalability
- ABAC access control (single-tenant mode)

### Package structure

```
cmd/charon/          -- binary entry point, config loading, dependency wiring
internal/api/        -- HTTP handlers for Charon's internal API (resolve, store, retrieve, delete)
internal/service/    -- ConversationService: chain resolution, context assembly, checkpoint logic
internal/storage/    -- IndexStore and PayloadStore interfaces + implementations
  sqlite/            -- SQLite IndexStore
  filesystem/        -- Filesystem PayloadStore
  memory/            -- In-memory implementations for tests
internal/model/      -- Go types for all Responses API structures
  items.go           -- Item discriminated union (generated from OpenAPI spec)
  response.go        -- ResponseRecord, ResponseMeta, Payload
internal/worker/     -- Background goroutines: TTL expiry, write-intent recovery
```

### Key dependencies

| Purpose | Package |
|---------|---------|
| SQL (embedded, no CGo) | `modernc.org/sqlite` |
| SQL query builder | `github.com/jmoiron/sqlx` |
| HTTP router | `github.com/go-chi/chi/v5` |
| Metrics | `github.com/prometheus/client_golang` |
| zstd compression (optional) | `github.com/klauspost/compress/zstd` |
| UUID generation | `github.com/google/uuid` |
| Structured logging | `log/slog` (stdlib) |

Schema initialization uses `CREATE TABLE IF NOT EXISTS` at startup — no migration framework. Since deployments always start with empty stores, live schema migration is not a requirement.

Phase 2 additions: `aws-sdk-go-v2` (S3/MinIO PayloadStore), `pgx` (PostgreSQL IndexStore), `github.com/hashicorp/golang-lru/v2` (hot-path context cache in Charon).

### Data directory layout (filesystem PayloadStore)

```
{data_dir}/
  responses.db              ← SQLite database
  payloads/
    {chain_root_id}/
      {position:08d}_{response_id}.json.zst     ← individual response payloads
      checkpoint_{position:08d}_{response_id}.json.zst  ← checkpoint blobs
```

Using `chain_root_id` + `position` in the path means the full chain is enumerable by directory listing, independent of SQL, and checkpoint files are co-located with their chain's individual payloads.

### Configuration

```yaml
server:
  listen: ":8080"
  
inference:
  base_url: "http://localhost:11434/v1"   # vLLM, Ollama, or any OpenAI-compatible endpoint
  api_key: ""                             # optional; passed as Authorization header

storage:
  index: "sqlite"                         # sqlite | postgres
  payload: "filesystem"                   # filesystem | s3
  data_dir: "./data"                      # used by sqlite and filesystem backends
  checkpoint_interval: 10                 # create a checkpoint every N turns
  ttl_days: 30                            # responses expire after N days

sqlite:
  path: "./data/responses.db"             # overrides data_dir if set
  wal_mode: true
  busy_timeout_ms: 5000

# Phase 2 only:
postgres:
  dsn: ""
s3:
  endpoint: ""
  bucket: ""
  access_key: ""
  secret_key: ""
```

### Response ID format

```
resp_<32-char hex UUID without dashes>    ← canonical; assigned by the inference server
rsrv_<32-char hex UUID without dashes>    ← reservation; assigned by Charon at resolve time
```

Examples:
- Canonical: `resp_4a3f8c2e1b0d9f7a6e5c4b3a2d1e0f9c` — the ID clients see and Charon stores as the primary key
- Reservation: `rsrv_9f1a2b3c4d5e6f7a8b9c0d1e2f3a4b5c` — proxy-internal, never exposed to clients

**Canonical IDs** are assigned by the inference server (vLLM or equivalent) and returned in the first streaming chunk or response body. When the inference backend returns IDs in a different format (e.g. `chatcmpl-` for chat completions), the proxy translates them to `resp_` format using `uuid.New()` keyed from the backend ID, or simply re-prefixes. IDs are opaque — no embedded timestamps, user identifiers, or model identifiers.

**Reservation IDs** are minted by Charon at resolve time using `uuid.New().String()` stripped of dashes, prefixed with `rsrv_`. They serve as correlation handles between the resolve call and the subsequent store call, and as a fallback response ID if inference fails before any canonical ID is known.

### Write path

The proxy makes two calls to Charon per inference turn (resolve is skipped for new chains):

**Resolve (before inference, continuations only):**
```
GET /responses/{previous_response_id}/context   // proxy → Charon
  ↓
ConversationService.Resolve(ctx, previousID)
  ↓
  walk chain with checkpoint: IndexStore + PayloadStore
  → returns flat []Item context (history only) + reservation_id
  ↓
proxy appends new input[] to flat_context
proxy forwards flat_context + input[] to inference backend
  ↓ (first streaming chunk from inference server)
proxy extracts canonical_id from inference server response
proxy sends response.created to client with canonical_id
```

**Store (after inference, Phase 1: fully buffered):**
```
POST /responses/{canonical_id}   // proxy → Charon
body: { reservation_id, previous_response_id, input[], output[], usage, status }
  ↓
ConversationService.Store(ctx, canonicalID, req)
  ↓
  INSERT write_intents (phase=pending, reservation_id, response_id=canonical_id)
  PayloadStore.Put(ctx, payloadKey, payload)   // atomic: write-to-temp, rename
  UPDATE write_intents (phase=file_written)
  IndexStore.Put(ctx, meta)
  UPDATE write_intents (phase=committed)
```

For new chains, `previous_response_id` is null and `reservation_id` is omitted. `store: false` requests skip the store call entirely. No write-intent is created.

**Store (after inference, future: chunked streaming):**

See [Streaming Store Modes](architecture.md#streaming-store-modes). The chunked protocol opens the write-intent at stream start and commits at stream close. The write path for individual chunks is:
```
POST /responses/{canonical_id}                        // open stream → INSERT write_intents (phase=stream_open)
PATCH /responses/{canonical_id} { items: [...] }      // each chunk → accumulate
PATCH /responses/{canonical_id} { items: [...], usage, status }  // final → PayloadStore.Put, IndexStore.Put, UPDATE write_intents (phase=committed)
```

### Write-intent safety (Phase 1, filesystem)

Write-intents are created only at store time. If the proxy crashes before calling store (e.g., inference failed with no failure notification, or `store: false`), no orphaned state exists.

For the SQLite + filesystem backend, the write sequence within a store call:

1. INSERT `write_intents` row with `phase = 'pending'`, `response_id = canonical_id`, `reservation_id` (if continuation), `created_at = updated_at = now`
2. Write payload file (atomic: write-to-temp, `os.Rename`)
3. UPDATE `write_intents` SET `phase = 'file_written'`, `updated_at = now`
4. INSERT `responses` metadata row
5. UPDATE `write_intents` SET `phase = 'committed'`, `updated_at = now`

Recovery condition: `phase IN ('pending', 'file_written') AND updated_at < now - stale_threshold` (default 5 minutes). On startup and periodically, the recovery worker finds stale intents and either completes them (re-write is safe: deterministic payload key) or marks them `failed`.

### Success criteria for Phase 1

- All conformance test cases pass: `resolve-new-chain`, `resolve-multi-turn`, `resolve-with-checkpoint`, `store-and-retrieve`, `write-intent-recovery`, `ttl-expiry`
- A conversation of 100 turns reconstructs correctly with correct flat_context returned
- Single binary starts with zero dependencies: `./charon --config config.yaml`
- Unit and integration tests cover chain reconstruction, checkpoint logic, and write-intent recovery at each failure phase
- Estimated code size: ~2,000–3,000 lines (no proxy/SSE/WebSocket scope)

---

## Phase 2: Scaled Storage Backends

**Goal:** Multiple API instances sharing a common storage backend. No API or business logic changes — only backend implementations added and wired into the existing interfaces.

**Deliverables:**
- PostgreSQL IndexStore implementation
- S3/MinIO PayloadStore implementation (using `aws-sdk-go-v2` with a custom endpoint for MinIO)
- Write-intent recovery with PostgreSQL as the coordinator
- Connection pooling configuration

**Deployment path for Phase 2:**

Phase 1 and Phase 2 are independent deployments chosen based on scale and operational requirements — there is no data migration between them. Both always start with empty index and payload stores.

1. Deploy Charon with PostgreSQL + S3/MinIO config
2. Set `storage.index` to `postgres`, configure `postgres.dsn`
3. Set `storage.payload` to `s3`, configure bucket and credentials
4. Application code is identical to Phase 1 — only configuration changes

**Success criteria for Phase 2:**

- Two API instances can serve the same conversation concurrently without corruption
- Write-intent recovery correctly completes or fails partial writes after a simulated crash
- Full chain retrieval for a 500-turn conversation completes in under 500ms

---

## Phase 3: Proxy Layer and Compliance Testing

**Goal:** Expose the client-facing OpenAI Responses API surface and validate it against the [openresponses.org compliance suite](https://www.openresponses.org/compliance) running entirely locally — no remote API, no GPU required for CI.

Charon is an internal service; it never speaks the Responses API directly. This phase builds the thin proxy layer that sits in front of Charon and translates the client-facing protocol into Charon's resolve/store calls. Once the proxy exists, the compliance suite can be pointed at `http://localhost:PORT` and run headlessly.

**Deliverables:**

*Proxy layer (client-facing HTTP):*
- `POST /responses` (REST) — resolves chain if `previous_response_id` present, forwards to inference, stores result
- `POST /responses` with `stream: true` — same flow, but streams SSE events to the client as tokens arrive from the inference backend
- `WebSocket /responses` — accepts `response.create` messages, streams output events back; supports `store: false` with connection-local cache
- `POST /responses/compact` — forwards to inference for compaction; stores result via Charon
- `GET /responses/{id}`, `DELETE /responses/{id}` — thin pass-through to Charon's retrieve/delete endpoints

*Mock inference backend (for CI):*
- A deterministic local HTTP server that implements the OpenAI chat completions API
- Returns canned responses matched against input patterns (no GPU, no network)
- Sufficient to satisfy the compliance suite's content assertions (e.g. "say hello in exactly 3 words")
- Configurable via the existing `inference.base_url` config key — no code changes needed to swap in the real inference backend

*Compliance integration:*
- `bun run test:compliance --base-url http://localhost:PORT` runnable locally and in CI
- CI runs the 12 achievable tests (those not requiring real LLM reasoning); remaining 5 are gated behind a `--extended` flag for use against a real deployment

**Test feasibility breakdown:**

| Category | Tests | CI target |
|----------|-------|-----------|
| Local fixture only | `response-output-phase-schema` | ✓ |
| Error / missing-data only | `compact-missing-model`, `websocket-previous-response-not-found`, `websocket-reconnect-store-false-recovery`, `websocket-failed-continuation-evicts-cache` | ✓ |
| Deterministic mock sufficient | `basic-response`, `streaming-response`, `system-prompt`, `multi-turn`, `websocket-response`, `websocket-sequential-responses`, `websocket-continuation` | ✓ |
| Requires real LLM | `tool-calling`, `image-input`, `assistant-phase`, `compact-response`, `websocket-compact-new-chain` | extended only |

**Infrastructure note:** The compliance runner requires `bun` (TypeScript runtime). CI must install it (`curl -fsSL https://bun.sh/install | bash` or the distro package). The runner is cloned from [github.com/openresponses/openresponses](https://github.com/openresponses/openresponses) and invoked as `bun run test:compliance`.

**Deployment model:** For the single-binary deployment, the proxy and Charon are colocated in the same process. The proxy calls Charon's service layer directly (in-process function calls, no HTTP hop). For multi-instance deployments (Phase 2 backends), the proxy makes real HTTP calls to a separate Charon instance.

**Success criteria for Phase 3:**

- All 12 CI-targeted compliance tests pass against the local stack with the mock inference backend
- `bun run test:compliance --base-url http://localhost:PORT` exits 0 in CI
- SSE streaming delivers `response.created`, one or more `response.output_item.added` events, and `response.completed` in that order
- `store: false` responses are served within the same WebSocket connection but not retrievable after reconnect (`previous_response_not_found`)
- Swapping `inference.base_url` from mock to a real vLLM endpoint requires zero code changes

---

## Phase 4: Multi-Tenant and Access Control

**Goal:** Tenant isolation, per-user response scoping, authentication.

**Deliverables:**
- ABAC attribute model: `owner_principal`, `roles`, `teams`, `projects`, `namespaces`
- Authentication middleware (header-based for trusted gateways; bearer token for direct access)
- Per-request access check on all read and delete operations
- `AuthorizedIndexStore` wrapper that injects WHERE clauses based on requester attributes
- Audit log for access denials

**Design notes:**

The access model must avoid the pitfall identified in existing implementations: a row should be accessible to the owner OR to users explicitly granted access — not to anyone who shares any team value. Shared access should be opt-in (explicit grants), not opt-out (attribute overlap).

**Success criteria for Phase 4:**

- User A cannot read User B's responses
- Shared responses (explicitly shared by owner) are accessible to designated users
- Unauthenticated requests in authenticated mode return 401, not 200

---

## Phase 5: Observability and Operational Hardening

**Goal:** Production-grade operations: metrics, structured logging, graceful shutdown, rate limiting.

**Deliverables:**
- Extended Prometheus metrics: checkpoint write rate and size, TTL expiry rate, storage backend latency, background worker run duration
- `GET /healthz` and `GET /readyz` probes
- Graceful shutdown: complete in-flight store calls before exit, allow background workers to finish their current iteration
- Structured logging hardening: audit all log call sites, ensure no raw content at INFO level

**Operational tooling:**

- `charon reconcile` subcommand: run the write-intent recovery and object store reconciliation job on demand

---

## Deferred / Out of Scope

The following are explicitly out of scope for initial delivery. Documenting them here prevents scope creep.

**KV cache persistence:** As established in the architecture document, storing transformer KV cache is not planned. The expansion factor (30,000–160,000×) makes it impractical in durable storage. Inference backends handle KV cache in GPU memory.

**Compaction endpoint:** `/responses/compact` is a proxy concern — it requires calling an inference backend to produce the compaction summary. Charon's role is standard chain resolution (resolve call); the proxy drives the compact flow. Automatic background summarization is similarly out of scope for Charon.

**Segment packing (Option D storage):** The pack-file approach offers the best long-term storage efficiency but is significantly more complex to implement. It becomes relevant if storage costs are measurable at scale. Option B + Option C snapshotting is the target for Phase 1 and 2.

**Embedding / vector search:** This is a stateful serving layer for structured conversation history, not a retrieval-augmented generation system. Vector indexing of response content is out of scope.

**DAG optimization:** The spec allows `previous_response_id` to form a DAG (two responses can share the same parent). The storage design accommodates this structurally, but tree-shaped retrieval is not specially optimized. It falls back to standard chain walk from each leaf.

**Chunked streaming store:** Phase 1 uses fully-buffered mode. Chunked delivery to Charon — from N-token batches down to single-token granularity — improves recovery granularity and reduces peak proxy memory, but requires a streaming ingest protocol in Charon (`PATCH` per chunk or ndjson over chunked HTTP) plus additional write-intent phases (`stream_open`). The design is specified in [Streaming Store Modes](architecture.md#streaming-store-modes); implementation is deferred to post-Phase-1.

---

## Estimated Scope

| Phase | Estimated LOC | Key complexity |
|-------|--------------|----------------|
| Phase 1 (single binary) | 2,500 – 4,000 | Chain reconstruction, checkpoint logic, write-intent recovery |
| Phase 2 (scaled backends) | +800 – 1,500 | PostgreSQL adapter, S3 adapter, cross-system consistency |
| Phase 3 (proxy + compliance) | +1,500 – 2,500 | SSE streaming, WebSocket, mock inference backend, compliance wiring |
| Phase 4 (access control) | +600 – 1,000 | ABAC model, middleware, authorized store wrapper |
| Phase 5 (observability) | +400 – 700 | Metrics, structured logging, operational subcommands |

The dominant complexity is not in routing or storage glue — it is in **managing history state correctly over time**: checkpoint boundaries, write-intent recovery, TTL, `store: false` semantics, and the `encrypted_content` round-trip for reasoning and compaction items. These correctness concerns are where most bugs will occur and where the test suite must be most thorough.
