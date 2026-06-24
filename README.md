# Charon

Charon is an internal context-store service for the [OpenAI Responses API](https://platform.openai.com/docs/api-reference/responses). It bridges the gap between the stateful Responses API and stateless LLM inference:

- Resolves `previous_response_id` chains into the flat context an inference backend needs
- Persists response payloads (input items, output items) to durable storage
- Manages write-intent safety and background TTL/recovery workers

Charon is **not** the client-facing API layer. A proxy sits in front of Charon, owns the Responses API surface (REST, SSE, WebSocket), and calls Charon to resolve context before inference and to store results after.
<AGENT>
Proxy provided in code base is for testing only and to demonstrate the possible integration between a proxy gateway and charon.
</AGENT>

> [!NOTE]
> Charon implements the **persistence and context-resolution layer** of the Responses API only. It does not implement the agentic loop or any of the following concerns — these must be provided by a separate orchestration service placed in front of Charon:
> - **Server-side tool execution** — file search, web search, code interpreter, image generation
> - **MCP tool orchestration** — tool discovery, session management, approval flows
> - **Background / async processing** — `background: true` request queueing and polling
> - **Guardrails** — input and output content moderation
> - **Conversation management** — dual-storage conversation threading (`conversation` parameter)

<AGENT>
Add warning on this being alpha quality and should not be used in production. 
</AGENT>

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
    backend: sqlite
    data_dir: /var/lib/charon
```

### Full example — Charon + proxy enabled

```yaml
charon:
  listen: ":8081"
  storage:
    backend: sqlite
    data_dir: /var/lib/charon
    checkpoint_interval: 10   # checkpoint every N turns (default 10)
    ttl_days: 30              # response TTL (default 30)
    write_intent_stale_threshold: 5m
  workers:
    ttl_interval: 1h
    recovery_interval: 5m

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
| `storage.backend` | `memory` | Storage backend: `memory` or `sqlite` |
| `storage.data_dir` | `./data` | Root directory for SQLite database and payload files |
| `storage.checkpoint_interval` | `10` | Write a full-context checkpoint every N turns |
| `storage.ttl_days` | `30` | Responses expire after this many days |
| `storage.write_intent_stale_threshold` | `5m` | Write intents older than this are recovered on startup |
| `workers.ttl_interval` | `1h` | How often the TTL expiry worker runs |
| `workers.recovery_interval` | `5m` | How often the write-intent recovery worker runs |

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

## Storage backends

### `memory` (default)

In-memory stores. No persistence across restarts. Use for conformance/compliance testing and lightweight local development.

### `sqlite`

Embedded SQLite (pure Go, no CGo). Persists to `data_dir/responses.db` (index) and `data_dir/payloads/` (payload blobs). WAL journal mode is always enabled. Suitable for single-instance production deployments.

For multi-instance deployments, use PostgreSQL + MinIO/S3 (see [architecture docs](docs/architecture.md)).

---

## Deployment modes

| Mode | `charon.storage.backend` | `proxy.enabled` | Use for |
|------|--------------------------|-----------------|---------|
| Memory only | `memory` | `false` | Conformance testing, CI |
| Single binary | `sqlite` | `false` (Charon only) | Development |
| Single binary + proxy | `sqlite` | `true` | Compliance testing, single-node production |
| Separate services | `sqlite` / `postgres` | Proxy in its own process | Production |

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
