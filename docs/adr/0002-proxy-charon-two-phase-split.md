# ADR 0002: Proxy/Charon Two-Phase Split

**Status:** Accepted

## Context

The OpenAI Responses API must accept stateful requests (with `previous_response_id`) and forward a flat context to a stateless inference backend. The question is how to divide responsibilities between the client-facing component (the proxy) and the storage/resolution service (Charon).

Alternative architectures considered:

- **Charon as the client-facing API**: Charon owns SSE, WebSocket, auth, TLS, and model routing. The proxy is eliminated or reduced to a simple load balancer.
- **Single-component design**: One process handles everything — client API, chain resolution, inference forwarding, and storage.
- **Charon as a library**: Embedded into the proxy; no network boundary between them.

## Decision

Charon is an internal storage/resolution service (separate repository from the proxy). The proxy owns all client-facing concerns. They interact via a staging protocol over an internal HTTP API:

**Continuation turns** (has `previous_response_id`):
1. **Open staging** — `POST /staging?prev={previous_response_id}`: proxy receives `{staging_id, flat_context[]}`. Charon assembles history and creates a staging record.
2. **Inference** — proxy forwards to the inference server, which assigns the canonical response ID in its first streaming chunk.
3. **Append chunks** — proxy delivers response bytes incrementally via `PUT /staging/{staging_id}/chunks/{k}`.
4. **Complete** — `PUT /staging/{staging_id}/complete?response_id={canonical_id}&total={N}`: Charon commits the node atomically.

**New chains** (no `previous_response_id`):
- No staging call needed. Inference server assigns the canonical response ID.
- After inference: buffered `POST /responses` (or staging protocol as above; skipped if `store: false`).

## Reasons

**Separation of scaling axes.** Proxy instances are stateless and scale horizontally without coordination. Charon scales independently based on storage throughput. Coupling them forces both to scale together.

**`store: false` is a proxy concern, not a storage concern.** Connection-local ephemeral caches for WebSocket sessions belong in the component that owns the connection. Charon should not need to understand connection lifecycle.

**Model routing is an infrastructure concern.** Charon can be deployed per inference model (single-binary mode) or as a shared service fronting multiple backends. Neither deployment shape requires Charon to be the client-facing component.

**Auth, TLS, and streaming belong in the proxy.** These are operational concerns that change independently of storage logic. Keeping them in the proxy allows Charon's API to be simple and internal.

## Trade-offs Accepted

**Two network hops per request** (proxy → Charon resolve, proxy → inference, proxy → Charon store). In the single-binary deployment, these are in-process calls with no real network cost. In multi-instance deployment, the resolve call adds latency proportional to chain-walk time, not raw network RTT.

**Canonical IDs owned by the inference server.** The inference server (vLLM, etc.) assigns the canonical response ID returned in its first streaming chunk. The proxy uses this ID in `response.created` to the client and in the store call to Charon. Charon mints only a short-lived `staging_id` at resolve time for ingest correlation — this is never client-visible. When the inference backend returns a non-`resp_` format ID (e.g. `chatcmpl-`), the proxy translates it before forwarding.

**No staging cleanup needed for `store: false`.** Because staging records are created only at `POST /staging` time (for continuations), a `staging_id` minted at resolve that the proxy never commits leaves no orphaned chain node — the staging reaper cleans up the record TTL.

**Chunked streaming store is implemented.** The proxy delivers output to Charon in chunks via the staging protocol (`PUT /staging/{id}/chunks/{k}` + `PUT /staging/{id}/complete`). See [Streaming Ingest](../architecture.md#streaming-ingest).
