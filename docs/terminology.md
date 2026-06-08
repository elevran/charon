# Charon: Bounded Context

## Glossary

### Charon

The internal storage and resolution service. Charon is **not** client-facing. It accepts calls from the proxy, resolves `previous_response_id` chains into flat `[]Item` context arrays, and persists completed response payloads to durable storage.

Charon does not own: SSE streaming, WebSocket handling, authentication, TLS, model routing, or `store: false` semantics.

### Proxy

The client-facing component that owns the OpenAI Responses API surface. The proxy handles HTTP transport (REST, SSE, WebSocket), authentication, TLS termination, and streaming.

For new chains (no `previous_response_id`), the proxy calls inference directly and uses the inference-server-assigned canonical response ID for the store call (if `store: true`). For continuations, it calls Charon to resolve before inference (receiving a `reservation_id` and `flat_context`) and to store after (using the canonical ID from the inference server). If `store: false`, it skips the store call entirely.

The proxy and Charon may be colocated in a single binary (Phase 1) or deployed as separate services (Phase 2+).

### Resolve

The operation Charon performs when the proxy calls `GET /responses/{previous_response_id}/context`. Charon walks the stored chain, assembles the flat `[]Item` context from prior turns, mints a `reservation_id`, and returns `{reservation_id, flat_context}` to the proxy. Only called for continuations — new chains skip resolve entirely.

The proxy appends the new `input[]` to `flat_context` before forwarding to the inference backend. The canonical response ID is not known until the inference server returns its first streaming chunk.

### Reservation ID

A short-lived, proxy-internal identifier minted by Charon at resolve time. Prefixed `rsrv_`. Never exposed to clients. Passed back to Charon in the store call body to correlate the resolve → store pair in logs and write-intent records. Also used as a fallback response ID if inference fails before the inference server returns any data (and therefore no canonical ID has been received).

### Store

The operation the proxy performs after inference completes, by calling `POST /responses/{canonical_response_id}` with `{reservation_id, previous_response_id, input[], output[], usage, status}`. Charon atomically creates the write-intent and writes the payload. `reservation_id` is included for continuation turns (omitted for new chains). If inference failed before the canonical ID was known, the proxy uses `reservation_id` as the path parameter. Skipped entirely when `store: false`.

### Charon API

Charon's HTTP API is an internal contract between the proxy and Charon. It is not required to conform to the OpenAI Responses API specification and is designed for operational efficiency. The proxy is responsible for mapping between Charon's internal API and the client-facing Responses API surface.

### Response

A single turn in a conversation: one `input[]` + one `output[]`, linked to the preceding turn via `previous_response_id`. Stored as a metadata record (IndexStore) and a payload blob (PayloadStore).

### Chain

A singly-linked list of responses connected via `previous_response_id`. Charon walks the chain during resolve to produce a flat ordered context. Chain root = the first response with no `previous_response_id`.

### Checkpoint

A materialized snapshot of the full flat context at a specific position in the chain, written every K turns (configurable). Bounds chain-walk cost to O(K) instead of O(N). See [storage.md](docs/storage.md).

### IndexStore

The storage interface that manages response metadata and chain linkage: `id`, `previous_response_id`, `chain_root_id`, `position`, `status`, `created_at`, etc. Backed by SQLite (Phase 1) or PostgreSQL (Phase 2+).

### PayloadStore

The storage interface that manages response content: `input[]`, `output[]`, and checkpoint blobs. Backed by the local filesystem (Phase 1) or an object store (Phase 2+).

### Write-Intent

A record inserted into the IndexStore before a payload file is written, used to detect and recover from partial writes when the process crashes between the filesystem write and the SQL commit. See [storage.md](docs/storage.md) for the recovery protocol.

### flat_context

The ordered `[]Item` array that Charon returns from a resolve call. Contains all input and output items from the resolved chain, in chronological order, ready to be forwarded to the inference backend (after the proxy appends the new `input[]`).

### `store: false`

A client-set flag indicating that the response must not be written to durable storage. The proxy skips the store call to Charon. For WebSocket sessions, the proxy maintains a connection-local in-memory cache of `store: false` responses to serve within-connection `previous_response_id` lookups. Charon is unaware of `store: false`.

### Instructions

The system prompt supplied per-request by the client. Explicitly excluded from stored history by spec design — re-injected each request — so callers can change the system prompt mid-conversation without starting a new chain.

### Inference Backend

A stateless OpenAI-compatible HTTP endpoint (vLLM, Ollama, etc.). Always receives the complete flat context. Has no concept of sessions or chains. Assigns the **canonical response ID** returned in the first streaming chunk or response body — this is the ID Charon stores as the primary key and clients receive in `response.created`. When the backend uses a non-`resp_` ID format (e.g. `chatcmpl-`), the proxy translates it before forwarding to the client and to Charon.
