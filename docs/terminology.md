# Charon: Bounded Context

## Glossary

### Charon

The internal storage and resolution service. Charon is **not** client-facing. It accepts calls from the proxy, resolves `previous_response_id` chains into flat `[]Item` context arrays, and persists completed response payloads to durable storage.

Charon does not own: SSE streaming, WebSocket handling, authentication, TLS, model routing, or `store: false` semantics.

### Proxy

The client-facing component that owns the OpenAI Responses API surface. The proxy handles HTTP transport (REST, SSE, WebSocket), authentication, TLS termination, and streaming.

For new chains (no `previous_response_id`), the proxy calls Charon's buffered store (`POST /responses`) after inference completes. For continuations, it calls `POST /staging` to resolve before inference (receiving a `staging_id` and `flat_context`) and uses the staging protocol to store incrementally after. If `store: false`, it skips the store call entirely.

The proxy and Charon may be colocated in a single binary (Phase 1) or deployed as separate services (Phase 2+).

### Resolve

The operation Charon performs when the proxy calls `POST /staging?prev={previousID}`. Charon walks the stored chain, assembles the flat `[]Turn` context from prior turns, creates a staging record, and returns `{staging_id, flat_context, turns}` to the proxy. The proxy uses `flat_context` to build the inference request. Only called for continuations — new chains use the buffered `POST /responses` path after inference.

### Staging ID

A 128-bit random UUID minted by Charon at `POST /staging` time. It is the correlation handle for the streaming ingest protocol. Never exposed to clients. The proxy binds the canonical response ID to the staging ID on the first chunk write and uses it through `PUT /staging/{id}/complete`.

### Store

The operation the proxy performs after inference completes. Two paths:
- **Buffered** (`POST /responses`): sends complete request and response blobs in one call. Atomic.
- **Streaming** (`PUT /staging/{id}/chunks/{k}` + `PUT /staging/{id}/complete`): delivers chunks as inference streams, then commits. Skipped entirely when `store: false`.

### Charon API

Charon's HTTP API is an internal contract between the proxy and Charon. It is not required to conform to the OpenAI Responses API specification and is designed for operational efficiency. The proxy is responsible for mapping between Charon's internal API and the client-facing Responses API surface.

### Response

A single turn in a conversation: one request blob + one response blob, linked to the preceding turn via `previous_response_id`. Stored as a Node (metadata) plus blob entries in Pebble.

### Chain

A singly-linked list of responses connected via `previous_response_id`. Charon walks the chain during resolve to produce a flat ordered context. Chain root = the first response with no `previous_response_id`.

### Checkpoint

A materialized snapshot of the full flat context at a specific position in the chain, written every K turns (configurable). Bounds chain-walk cost to O(K) instead of O(N). See [storage.md](storage.md).

### Write-Intent

A staging record created before blobs are committed, used to detect and recover from partial writes when the process crashes mid-streaming. Orphaned staging records are reaped by the background staging TTL worker. See [storage.md](storage.md) for the recovery protocol.

### flat_context

The ordered `[]Item` array that Charon returns from a resolve call. Contains all input and output items from the resolved chain, in chronological order, ready to be forwarded to the inference backend (after the proxy appends the new `input[]`).

### `store: false`

A client-set flag indicating that the response must not be written to durable storage. The proxy skips the store call to Charon. For WebSocket sessions, the proxy maintains a connection-local in-memory cache of `store: false` responses to serve within-connection `previous_response_id` lookups. Charon is unaware of `store: false`.

### Instructions

The system prompt supplied per-request by the client. Explicitly excluded from stored history by spec design — re-injected each request — so callers can change the system prompt mid-conversation without starting a new chain.

### Inference Backend

A stateless OpenAI-compatible HTTP endpoint (vLLM, Ollama, etc.). Always receives the complete flat context. Has no concept of sessions or chains. Assigns the **canonical response ID** returned in the first streaming chunk or response body — this is the ID Charon stores as the primary key and clients receive in `response.created`. When the backend uses a non-`resp_` ID format (e.g. `chatcmpl-`), the proxy translates it before forwarding to the client and to Charon.
