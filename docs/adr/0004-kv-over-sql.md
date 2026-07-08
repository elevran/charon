# ADR 0004: Embedded Key-Value Store (Pebble) over SQL

**Status:** Accepted

## Context

Charon must durably store conversation turns (request and response blobs keyed by response ID) and support two query patterns:

1. **Point lookup by response ID** — `GET /responses/{id}`, parent-pointer fetch during chain walk.
2. **Sequential parent-pointer walk** — resolve `previous_response_id` chains from leaf to root by following a linked list of keys.

There are no joins, no predicate-based queries over payload content, and no schema evolution requirements driven by query shape. Blobs are opaque to the storage layer; only the key (`NodeID`) and a small set of chain-linkage metadata fields are structured.

The primary alternatives considered were:

- **Relational database (SQLite or embedded PostgreSQL via pgx/pgembedded)**: Rich query language, mature tooling, ACID transactions.
- **Embedded key-value store (Pebble)**: Point lookups and sequential scans only, no query language, no external process.
- **Object store (S3-compatible or local filesystem blobs + SQLite index)**: Decouples metadata from blobs; adds operational complexity.

## Decision

Use [Pebble](https://github.com/cockroachdb/pebble), an embedded key-value store, as the sole storage backend.

## Reasons

**The access pattern is a pure key-value workload.** Every read is a point lookup by `NodeID` or a sequential scan over a known key prefix (LRU bucket scan, TTL reap). SQL's query planner, index maintenance, and join execution add overhead with no benefit when every query reduces to a primary-key fetch.

**Large blobs fragment across SQL pages.** SQL engines use fixed page sizes (typically 8–16 KB). Request and response blobs are serialised conversation items that frequently exceed one page. The SQL engine must read, assemble, and write multiple pages per blob. Pebble writes each blob as a single contiguous SSTable value; a point read recovers the whole blob in one I/O.

**LSM write path matches the append-only workload.** Charon writes each new response as one atomic batch: one node record and one or two blob values. Pebble's LSM absorbs writes into an in-memory memtable and flushes to sorted SSTables — ideal for an append-mostly pattern where existing values are rarely updated in place. SQL B-tree pages require in-place updates and lock-based concurrency control even for pure inserts when the index must be maintained.

**No external process.** Pebble is a pure-Go library; it embeds directly into the Charon binary. A SQL deployment requires an external database server (network dependency, separate process lifecycle, connection pooling). The single-binary deployment goal (see ADR 0001) is satisfied by an embedded store; a SQL server breaks it.

**Pure-Go, no CGo.** Pebble cross-compiles with `CGO_ENABLED=0` to any `GOOS`/`GOARCH` target without requiring a C toolchain. SQLite requires CGo; a pure-Go SQLite binding adds significant latency overhead for the write path due to the CGo call cost per transaction.

**The `Backend` interface abstracts the storage layer.** `chainstore.Backend` (in `internal/chainstore/backend.go`) defines `GetNode`, `GetBlobs`, `LoadChain`, `Commit`, and `Stats`. The Pebble implementation lives in `internal/chainstore/pebble/`. A DynamoDB backend is planned but not yet implemented; it would enable distributed deployments without changing chain-walk or staging logic. Switching storage implementations does not require changing chain-walk or staging logic.

## Trade-offs Accepted

**No ad-hoc query capability.** Debugging tools cannot run `SELECT … WHERE …` over payload content. Operational inspection requires purpose-built CLI tools (the `cache-check` CLI added in phase 5) or offline export. This is acceptable because Charon's API already exposes all necessary retrieval paths.

**Single-writer constraint.** Pebble does not support concurrent writers from separate processes. A single Charon instance owns a Pebble directory exclusively. Horizontal scale requires sharding by response ID prefix or switching to a distributed `Backend` implementation (see the Performance and Scale section in `architecture.md`).

**No built-in replication.** Pebble durability is limited to the local disk. WAL-based crash recovery protects against process crashes; hardware failure requires external backup or RAID. This is consistent with the single-server deployment target.
