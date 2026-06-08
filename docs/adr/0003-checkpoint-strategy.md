# ADR 0003: Checkpoint Strategy for Chain Reconstruction

**Status:** Accepted

## Context

Responses form a singly-linked list. To reconstruct the flat context for a new turn, Charon must walk the chain from the current head back to the root and assemble all input/output items in order. Three strategies are viable:

| Strategy | Write cost | Read cost |
|----------|-----------|-----------|
| Delta-per-response | O(1) | O(N) — full chain walk |
| Full-snapshot-per-response | O(N) | O(1) — single fetch |
| Checkpoint every K turns | O(1) amortized | O(K) — walk at most K steps |

## Decision

Checkpoint every K turns (configurable, default 10).

Every K-th response stores a full materialized snapshot of the flat context at that position. A resolve call walks backward from the chain head until it hits a checkpoint, then reads forward from the checkpoint. Worst-case walk is K steps.

## Reasons

**Delta-per-response is unbounded.** A 500-turn conversation requires reading 500 payload files and making 500 IndexStore lookups on every new request. Latency grows linearly with conversation length with no bound.

**Full-snapshot-per-response has unacceptable write amplification.** Turn N requires writing the entire prior context (N payloads). A 500-turn conversation writes a 500-item payload on turn 500. Total storage grows as O(N²).

**Checkpoints bound both costs.** Write amplification is O(K) at most (the checkpoint payload grows with conversation length, but only written every K turns, not every turn). Read cost is O(K) regardless of total chain depth. K=10 keeps reads fast without significant write overhead.

**Checkpoints are co-located with chain payloads.** Using `chain_root_id + position` in the filesystem path means checkpoint files are in the same directory as their chain's individual payloads. The full chain is enumerable by directory listing independent of the database.

## Trade-offs Accepted

**Checkpoint writes are larger than delta writes.** A checkpoint at position 100 in a long conversation includes all 100 prior turns' items. This is the cost of bounding read latency.

**K is a tunable constant, not adaptive.** A smarter strategy would checkpoint based on item count (tokens) rather than turn count, since turn length varies widely. K-by-turn is simpler to implement and sufficient for Phase 1. Adaptive checkpointing can be added later without changing the storage format.
