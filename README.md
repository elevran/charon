# Charon

Charon is an internal context-store service for the [OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses). It bridges the gap between the stateful Responses API and stateless LLM inference:

- Resolves `previous_response_id` chains into the flat context an inference backend needs
- Persists response payloads (input items, output items) to durable storage
- Manages write-intent safety and background TTL/recovery workers

Charon is **not** the client-facing API layer. A proxy sits in front of Charon, owns the Responses API surface (REST, SSE, WebSocket), and calls Charon to resolve context before inference and to store results after.

The proxy included in this repository is provided for testing and to demonstrate how a proxy gateway integrates with Charon. It is not intended for production use.

> [!NOTE]
> Charon implements the **persistence and context-resolution layer** of the Responses API only. It does not implement the agentic loop or any of the following concerns — these must be provided by a separate orchestration service placed in front of Charon:
> - **Server-side tool execution** — file search, web search, code interpreter, image generation
> - **MCP tool orchestration** — tool discovery, session management, approval flows
> - **Background / async processing** — `background: true` request queueing and polling
> - **Guardrails** — input and output content moderation
> - **Conversation management** — dual-storage conversation threading (`conversation` parameter)

> [!WARNING]
> Charon is alpha-quality software. APIs, configuration, and storage formats may change without notice. Do not use in production.

---

## Running

```
./charon --config config.yaml
```

Without a config file, Charon starts with all defaults: in-memory storage, Charon internal API on `:8081`, proxy layer **disabled**.

### Subcommands

```
./charon reconcile --config config.yaml   # one-shot write-intent recovery sweep
```

---

## Configuration

All settings are grouped under two top-level keys:

| Key | What it configures |
|-----|--------------------|
| `charon` | Charon internal API, storage backends, background workers |
| `proxy` | Client-facing Responses API proxy (disabled by default) |

### Minimal example — Charon only (no proxy)

```yaml
charon:
  listen: ":8081"
  storage:
    data_dir: /var/lib/charon
```

### Full example — Charon + proxy enabled

```yaml
charon:
  listen: ":8081"
  storage:
    data_dir: /var/lib/charon
    ttl_days: 30              # response TTL in days (default 30)
  workers:
    ttl_interval: 1h          # how often the TTL reaper runs (default 1h)

proxy:
  enabled: true               # proxy is OFF by default; set true to enable
  listen: ":8080"
  charon_url: ""              # auto-derived from charon.listen when empty
  inference:
    base_url: "http://localhost:11434"
    api_key: ""
    timeout_seconds: 120
    store_buffer_bytes: 65536 # 0 -> 64 KB default; -1 -> flush every item
```

---

## Configuration reference

### `charon`

| Field | Default | Description |
|-------|---------|-------------|
| `listen` | `:8081` | Address for the Charon internal HTTP API |
| `storage.data_dir` | `./data` | Root directory for the Pebble database |
| `storage.ttl_days` | `30` | Responses expire after this many days |
| `storage.max_chain_depth` | `1000` | Abort resolution if the chain walk exceeds this many hops. Returns `chain_too_deep` (HTTP 422). |
| `storage.max_context_bytes` | `0` (unbounded) | Abort resolution if the assembled flat context exceeds this size. Supports unit suffixes (`MB`, `GB`). Returns `context_too_large` (HTTP 422). |
| `storage.max_responses` | `0` (unbounded) | Evict oldest chains when the total response count exceeds this limit. |
| `storage.max_payload` | `0` (unbounded) | Maximum size of a single response payload blob. |
| `workers.ttl_interval` | `1h` | How often the TTL expiry worker runs |

### `telemetry`

| Field | Default | Description |
|-------|---------|-------------|
| `telemetry.exporter_url` | `` | OTLP HTTP endpoint (e.g. `http://localhost:4318`). Empty disables tracing (no-op, zero overhead). Can also be set via `--telemetry-exporter-url`. |
| `telemetry.charon_service` | `charon` | OTel service name for the Charon internal API |
| `telemetry.proxy_service` | `charon-proxy` | OTel service name for the proxy layer |

### `proxy`

| Field | Default | Description |
|-------|---------|-------------|
| `enabled` | `false` | Start the proxy layer. **Off by default.** |
| `listen` | `:8080` | Address for the client-facing Responses API |
| `charon_url` | auto | URL the proxy uses to reach Charon. Auto-derived from `charon.listen` when empty (wildcard hosts become `127.0.0.1`). |
| `inference.base_url` | `http://localhost:11434` | Stateless Responses API inference backend |
| `inference.api_key` | `` | Bearer token for the inference backend (empty = no auth) |
| `inference.timeout_seconds` | `120` | Inference request timeout |
| `inference.store_buffer_bytes` | `65536` | Proxy-to-Charon chunk buffer size. `0` -> 64 KB default; `-1` -> flush every output item immediately |

---

## Storage backend

Charon uses [Pebble](https://github.com/cockroachdb/pebble) as its embedded key-value store. All data — chain metadata, payload blobs, and LRU accounting — lives in a single Pebble database directory (`storage.data_dir`).

- **In-process**: no external database or object-store process required.
- **Durable**: data survives restarts; Pebble uses WAL-based crash recovery.
- **Single-node**: suitable for single-instance deployments. Multi-instance scaling is not yet supported.

Without a `data_dir` configured, Charon starts with an in-memory Pebble instance (data lost on restart). Use this mode for conformance/compliance testing and CI.

---

## Deployment modes

| Mode | `storage.data_dir` | `proxy.enabled` | Use for |
|------|-------------------|-----------------|---------|
| In-memory (no dir) | (empty / omit) | `false` | Conformance testing, CI |
| Single binary | `/var/lib/charon` | `false` (Charon only) | Development |
| Single binary + proxy | `/var/lib/charon` | `true` | Compliance testing, single-node production |
| Separate services | `/var/lib/charon` | Proxy in its own process | Production |

---

## Compliance testing

To run the [openresponses.org](https://www.openresponses.org/compliance) compliance suite locally:

```bash
# Start Charon with proxy enabled
./charon --config config.yaml   # proxy.enabled: true, inference.base_url pointing at vLLM

# In another terminal (requires bun and a clone of openresponses/openresponses)
OPENRESPONSES_DIR=/path/to/openresponses make test-compliance-bun
```

The Go compliance suite (no external tools required) runs as part of `make test`:

```bash
make test-compliance
```
