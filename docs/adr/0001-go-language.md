# ADR 0001: Go as Implementation Language

**Status:** Accepted

## Context

The workload is I/O-bound: embedded KV writes (Pebble), HTTP handling, zstd compression. The primary deployment target is on-premises infrastructure for AI teams running self-hosted inference backends. Charon is an internal service (separate repo from the client-facing proxy).

The core alternatives were Go and Rust.

## Decision

Go.

## Reasons

**No measurable performance gap for this workload.** Rust's advantages — zero-cost abstractions, deterministic allocation, fine-grained memory control — pay dividends on CPU-bound code. For I/O-bound work that blocks on disk or network syscalls, Go and Rust produce programs with effectively identical runtime characteristics.

**Pure-Go storage stack with no CGo.** The storage layer uses [Pebble](https://github.com/cockroachdb/pebble), a pure-Go embedded KV engine. It cross-compiles to any target with `GOOS`/`GOARCH` and produces a fully static binary with `CGO_ENABLED=0`. The Rust equivalent would require C toolchain dependencies for embedded storage. This friction simply does not exist in Go and is the single most important practical difference for the single-binary deployment goal.

**Goroutines fit the concurrency model naturally.** Background write-intent recovery, TTL expiry, and per-request chain-walk goroutines are all naturally expressed as goroutines communicating over channels with `context.Context` for cancellation. The Rust equivalent — `tokio` tasks, `Arc<Mutex<State>>`, `await` across lock boundaries — is correct but adds ceremony to sequential background bookkeeping.

**The repository is already Go.** Minimizes contributor ramp-up.

## Trade-offs Accepted

Rust's `serde` + enum variants would be cleaner for the discriminated union of output item types (`message`, `function_call`, `function_call_output`, `reasoning`, `compaction`). In Go, this requires an interface with type-switch or a tagged struct with `json.RawMessage` plus a second unmarshal pass.

Mitigation: generate Go types from the OpenAPI spec rather than hand-writing them.
