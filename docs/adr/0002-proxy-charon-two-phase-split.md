# ADR 0002: Proxy/Charon Two-Phase Split

**Status:** Accepted

## Context

The OpenAI Responses API must accept stateful requests (with `previous_response_id`) and forward a flat context to a stateless inference backend. The question is how to divide responsibilities between the client-facing component (the proxy) and the storage/resolution service (Charon).

Alternative architectures considered:

- **Charon as the client-facing API**: Charon owns SSE, WebSocket, auth, TLS, and model routing. The proxy is eliminated or reduced to a simple load balancer.
- **Single-component design**: One process handles everything — client API, chain resolution, inference forwarding, and storage.
- **Charon as a library**: Embedded into the proxy; no network boundary between them.

## Decision

Charon is an internal storage/resolution service (separate repository from the proxy). The proxy owns all client-facing concerns. They interact via a two-phase protocol over an internal HTTP API:

**Continuation turns** (has `previous_response_id`):
1. **Before inference** — `GET /responses/{previous_response_id}/context`: proxy receives `{reservation_id, flat_context[]}`. Charon assembles the history and mints a `reservation_id` for write-intent correlation. No write-intent is created yet.
2. **Inference** — proxy forwards to the inference server, which assigns the canonical response ID in its first streaming chunk.
3. **After inference** — `POST /responses/{canonical_id}`: proxy sends `{reservation_id, previous_response_id, input[], output[], status}`; Charon atomically creates the write-intent and commits to durable storage.

**New chains** (no `previous_response_id`):
- No resolve call. Inference server assigns the canonical response ID.
- After inference: `POST /responses/{canonical_id}` (no `reservation_id`) as above (or skipped if `store: false`).

## Reasons

**Separation of scaling axes.** Proxy instances are stateless and scale horizontally without coordination. Charon scales independently based on storage throughput. Coupling them forces both to scale together.

**`store: false` is a proxy concern, not a storage concern.** Connection-local ephemeral caches for WebSocket sessions belong in the component that owns the connection. Charon should not need to understand connection lifecycle.

**Model routing is an infrastructure concern.** Charon can be deployed per inference model (single-binary mode) or as a shared service fronting multiple backends. Neither deployment shape requires Charon to be the client-facing component.

**Auth, TLS, and streaming belong in the proxy.** These are operational concerns that change independently of storage logic. Keeping them in the proxy allows Charon's API to be simple and internal.

## Trade-offs Accepted

**Two network hops per request** (proxy → Charon resolve, proxy → inference, proxy → Charon store). In the single-binary deployment, these are in-process calls with no real network cost. In multi-instance deployment, the resolve call adds latency proportional to chain-walk time, not raw network RTT.

**Canonical IDs owned by the inference server.** The inference server (vLLM, etc.) assigns the canonical response ID returned in its first streaming chunk. The proxy uses this ID in `response.created` to the client and in the store call to Charon. Charon mints only a short-lived `reservation_id` at resolve time for write-intent correlation — this is never client-visible. When the inference backend returns a non-`resp_` format ID (e.g. `chatcmpl-`), the proxy translates it before forwarding. The `resp_` format convention is defined by Charon and followed by all parties.

**No write-intent cleanup needed for `store: false`.** Because write-intents are created only at store time (not at resolve time), a `reservation_id` minted at resolve that the proxy never stores leaves no orphaned state in Charon.

**Chunked streaming store is deferred.** In Phase 1, the proxy buffers the full inference output before the store call. Delivering output to Charon in chunks (from N-token batches to single tokens) would reduce peak proxy memory and improve recovery granularity if the proxy crashes mid-stream, but requires a streaming ingest protocol in Charon and additional write-intent phases (`stream_open`). Deferred to post-Phase-1; the design is specified in [Streaming Store Modes](../architecture.md#streaming-store-modes).
