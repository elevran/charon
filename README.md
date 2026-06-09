# Charon

**Response history service for LLM inference proxies.**

Charon stores and retrieves conversation history so stateless inference backends (vLLM, Ollama, any OpenAI-compatible endpoint) can serve multi-turn requests without maintaining state themselves. It sits between a reverse proxy and one or more inference servers, reconstructing flat context from a chain of prior responses before each inference call.

---

## Architecture

```
Client
  |  OpenAI Responses API (HTTP / SSE / WebSocket)
  v
Proxy ──────────────────────────────────────► Charon
  |  GET  /responses/{id}/context (resolve)    (chain resolution + storage)
  |  POST /responses/{id}         (store)
  |
  v  stateless (full flat_context as input)
Inference Backend (vLLM / Ollama / OpenAI-compatible)
```

The proxy calls Charon to resolve prior context before inference and to store the completed response after inference. Charon is never in the inference call path.

**Colocation:** For development and single-binary deployments, the proxy and Charon run in the same process (proxy on `:8080`, Charon internal API on `:8081`). In production they run as separate services.

---

## Features

- **Multi-turn context reconstruction** — walks the `previous_response_id` chain and returns a flat `[]Item` ready for the inference backend
- **Checkpoint strategy** — creates a full-context snapshot every N turns (default: 10) so chain-walk cost is O(K), not O(N), regardless of conversation length
- **Write-intent two-phase commit** — atomic store with crash recovery; a background worker detects stale write-intents and completes or fails them
- **TTL expiry** — a background worker purges responses older than a configurable number of days
- **Prometheus metrics** — request counts, latencies, write-intent failures, chain-depth histograms, active write-intents gauge
- **Optional proxy mode** — run without the built-in proxy (`--proxy=false`) to use Charon as a standalone storage service behind your own proxy

---

## Getting Started

### Build

```sh
make build
# binary at ./build/<GOOS>/<GOARCH>/charon
```

### Run

```sh
# With embedded proxy (default)
./charon --config config.yaml

# Storage service only (no proxy)
./charon --proxy=false --config config.yaml
```

### Minimal config (SQLite + filesystem)

```yaml
server:
  listen: ":8080"

charon:
  listen: ":8081"

inference:
  base_url: "http://localhost:11434"

storage:
  backend: sqlite
  data_dir: ./data
  checkpoint_interval: 10
  ttl_days: 30
```

---

## Configuration Reference

| Key | Default | Description |
|-----|---------|-------------|
| `server.listen` | `:8080` | Proxy listen address (client-facing) |
| `charon.listen` | `:8081` | Charon internal API listen address |
| `charon.base_url` | `""` | Base URL Charon advertises to the proxy (empty = use listen address) |
| `inference.base_url` | — | Upstream inference endpoint (vLLM, Ollama, etc.) |
| `inference.timeout_seconds` | `120` | Per-request timeout for inference calls |
| `storage.backend` | `sqlite` | Storage backend: `sqlite`, `filesystem`, or `memory` |
| `storage.data_dir` | `./data` | Root directory for SQLite DB and payload files |
| `storage.checkpoint_interval` | `10` | Create a context checkpoint every N turns |
| `storage.ttl_days` | `30` | Responses expire after N days (0 = no expiry) |
| `storage.sqlite.wal_mode` | `true` | Enable SQLite WAL mode (recommended) |
| `storage.sqlite.busy_timeout_ms` | `5000` | SQLite busy timeout in milliseconds |
| `workers.ttl_interval` | `1h` | How often the TTL expiry worker runs |
| `workers.recovery_interval` | `5m` | How often the write-intent recovery worker runs |
| `workers.stale_threshold` | `5m` | Age at which a pending write-intent is considered stale |

---

## API

Charon exposes an internal HTTP API consumed by the proxy. It is not the client-facing Responses API.

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/responses/{id}/context` | **Resolve** — walk the chain from `{id}`, return `{reservation_id, flat_context[]}`. Called before inference for continuation turns. |
| `POST` | `/responses/{id}` | **Store** — persist a completed response with write-intent safety. `{id}` is the canonical ID assigned by the inference server. |
| `PATCH` | `/responses/{id}` | **Append chunk** — append a streaming chunk to an open write-intent (chunked store mode). |
| `GET` | `/responses/{id}` | **Retrieve** — return the stored record for a single response (no chain walk). |
| `DELETE` | `/responses/{id}` | **Delete** — point-delete a response. No cascade. |
| `GET` | `/healthz` | Liveness probe — returns `200 OK` when the process is alive. |
| `GET` | `/readyz` | Readiness probe — returns `200 OK` when storage backends are ready to serve. |
| `GET` | `/metrics` | Prometheus metrics scrape endpoint. |

### Response ID conventions

- `resp_<32-char hex>` — canonical ID, assigned by the inference server
- `rsrv_<32-char hex>` — reservation ID, minted by Charon at resolve time for write-intent correlation; never exposed to clients

---

## Storage Backends

| Backend | Index store | Payload store | Use case |
|---------|------------|---------------|----------|
| `sqlite` | SQLite (embedded, no CGo) | Local filesystem | Single-binary deployment; no external dependencies |
| `memory` | In-memory | In-memory | Tests and compliance suite; no persistence |
| PostgreSQL + S3/MinIO | PostgreSQL | Object store | Multi-instance production (planned — Phase 2) |

The binary is identical across all backends. Only configuration changes.

**Data directory layout (sqlite backend):**

```
{data_dir}/
  responses.db                                         # SQLite index
  payloads/
    {chain_root_id}/
      {position:08d}_{response_id}.json.zst            # individual payloads
      checkpoint_{position:08d}_{response_id}.json.zst # checkpoint blobs
```

---

## Development

```sh
# Fast local gate: format check, vet, and short tests (<5s with warm cache)
make presubmit

# Full test suite with race detector (matches CI)
make test

# Unit tests only
make test-unit

# Integration tests (full in-process stack)
make test-integration

# Go compliance suite (mock inference, no external deps)
make test-compliance

# Lint
make lint

# Build container image
make image
```

### Compliance testing against the openresponses.org suite

Requires [bun](https://bun.sh) and a local clone of [github.com/openresponses/openresponses](https://github.com/openresponses/openresponses):

```sh
OPENRESPONSES_DIR=../openresponses make test-system
```

---

## `charon reconcile` subcommand

Runs the write-intent recovery and storage reconciliation job on demand, outside the normal background worker schedule. Useful for operational recovery after a crash or for verifying storage consistency.

```sh
./charon reconcile --config config.yaml
```

The reconciler finds write-intents in `pending` or `file_written` phase that are older than the stale threshold, attempts to complete them (re-write is safe — the payload key is deterministic), and marks irrecoverable intents as `failed`.

---

## Project Status

| Phase | Status | Description |
|-------|--------|-------------|
| Phase 1 | Complete | Single binary: SQLite + filesystem, full API, TTL, write-intent recovery |
| Phase 2 | Planned | PostgreSQL + S3/MinIO backends for multi-instance deployments |
| Phase 3 | Planned | Proxy layer, SSE/WebSocket, compliance suite integration |
| Phase 4 | Planned | Multi-tenant access control (ABAC) |
| Phase 5 | Planned | Extended observability, graceful shutdown hardening |
