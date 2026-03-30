# Behavior, ordering, and Python as the reference

This document states how **this Go port** relates to the **[Python Fleuve](https://github.com/anomaly/fleuve)** implementation for semantics that matter in production: ordering, delivery guarantees, offsets, and recovery.

## Reference implementation

- **Wire level** (HTTP shapes, PostgreSQL schema, NATS payload layout, config keys): documented as **compatible** so Go can share infrastructure with an existing Python deployment.
- **Behavior level** (when to ack, how far to advance a consumer, how recovery behaves, retry boundaries): **the Python implementation is the standard.** Where Go differs, treat that as a **bug or gap** to close, not an alternate design—unless the Python project explicitly changes.

This repository does **not** aim to invent divergent runner semantics.

## What this repo does *not* support

**Mixed runners** (Python and Go both consuming and mutating the same logical event stream / partitions for the same workflows at the same time) are **not** a supported deployment mode here. They invite duplicate processing, offset races, and undefined split-brain behavior.

Supported mental model:

- **One active runner stack per stream/partition contract** (either Python *or* Go for that scope), with **cutover** between them after validation.
- Sharing **only the database** for **read-only** checks (e.g. UI, reporting) during migration is fine; that is not “mixed runners.”

## Ordering (Go runner today)

- Events are ordered by **`stored_events.global_id`** (monotonic in the shared store).
- The runner processes many events **concurrently**, but the **committed consumer position** it publishes is the **largest contiguous `global_id` such that all events with `global_id ≤ that value have finished processing** (`InflightTracker` in `pkg/runner`). So downstream offset visibility respects **global order**, not completion order of arbitrary in-flight work.
- **Per-workflow** command application remains **serialized** by the repository (locking per `workflow_id`), so concurrent global processing does not reorder commands for a single aggregate.

**Parity expectation:** advance rules and visibility of progress should match Python’s consumer semantics; if they do not, align Go to Python.

## Exactly-once vs at-least-once

- **End-to-end exactly-once** is **not** claimed: the stream and activities layers are built around **redelivery** and **idempotent handling** where possible.
- **PostgreSQL command handling** uses transactions and versioned rows so **duplicate delivery of the same logical command** should **not** append duplicate versions if the implementation matches the Python model (verify per workflow and command path).
- **Activities** may be retried; rows in **`activities`** track status; recovery may **re-queue** work. That is **at-least-once** execution of side effects unless adapters are strictly idempotent.

**Parity expectation:** retry counts, terminal failure, and “failed → manual retry” behavior should follow Python; Go’s `pkg/actions` and gateway retry endpoint should be kept aligned with upstream behavior.

## Consumer start offsets

- **PostgreSQL-backed reader** (`pkg/stream.PGReader`): on `Init`, loads **`offsets.last_read_event_no`** for a stable **reader name** (`{WorkflowName}_pg` in the shipped runner). Batches read `WHERE global_id > offset ORDER BY global_id`. The runner calls **`SetCommittedOffset`** when the inflight tracker advances; the reader persists that to **`offsets`**.
- **NATS JetStream** (`pkg/stream.NATSReader`): on `Init`, optionally loads the same **`offsets`** row (`{WorkflowName}_nats`) into **`committedSeq`**, then creates/uses a **durable consumer** with **`DeliverByStartSequencePolicy`** / **`OptStartSeq: committedSeq + 1`**. **`SetCommittedOffset`** updates in-memory sequence and, when configured, **upserts `offsets`**.

**Parity expectation:** reader names, offset meaning (stream sequence vs `global_id`), and restart behavior must match Python. Any mismatch is integration-breaking.

### NATS JetStream ack timing (Go)

Go **does not** ack when a message is only queued on the internal channel. It **acks contiguous stream sequences** only after the runner’s `SetCommittedOffset` advances past them (same contiguous rule as `global_id` processing). The **`offsets`** row for the NATS reader is updated to **`lastJSAcked`** (highest sequence acked to JetStream), not the runner’s in-flight high-water mark, so a restart does not skip messages that were processed logically but not yet acked.

On **Close**, undelivered pending messages are **Nak**’d so they can redeliver. Align further with Python (e.g. `InProgress`, `DoubleAck`) if the upstream consumer uses them.

## Recovery

- **Activities:** periodic recovery loads **non-terminal** rows from **`activities`** (for the configured workflow type), reloads the corresponding event from **`stored_events`**, and **re-schedules** execution. Failed-action **HTTP retry** resets state and re-queues when allowed.
- **Parity expectation:** which statuses are recoverable, how `runner_id` / leases work (if any in Python), and interaction with the stream cursor must match Python.

## How to validate parity

1. Same DB + same NATS (if used): run a **single** workflow type under Python, capture **`stored_events`**, **`offsets`**, and **`activities`** for a scenario; repeat under Go with **only Go runners**; diff outcomes.
2. For ordering/offsets: inject controlled failures (kill runner mid-batch) and compare **redelivery** and **offset** rows with Python.
3. When in doubt, **read the Python implementation** for the code path in question and open an issue or PR to align Go.

## Related docs

- [INTEGRATION.md](./INTEGRATION.md) — schema and connecting to an existing Python database (not mixed runners).
- [operations.md](./operations.md) — operational checklist.
