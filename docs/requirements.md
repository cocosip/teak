# Requirements

This document defines Teak's normative v0.1 behavior. Architecture choices belong in
[design.md](design.md); implementation status belongs in [development.md](development.md).

## 1. Product Scope

Teak is an embedded durable work queue for a single Go process. One Badger database may contain many
named streams. Each stream provides work-queue semantics: concurrent readers compete for records
rather than each receiving a copy.

Teak is not a retained event log or a pub/sub broker in v0.1. Consumer groups, cross-process access,
exactly-once external side effects, and replay of committed records are out of scope.

## 2. Reliability Invariants

### R1: Successful writes survive restart

`Write` and `BatchWrite` must return success only after the record data and sequence metadata are
durably committed. A failed or canceled operation must not report success.

### R2: Pending data is never skipped

A pending record may leave the active queue only through:

1. a successful `Commit`, after the caller has processed it; or
2. a successful `DeadLetter`, which preserves the original payload and failure information.

Timeouts, retry counts, memory limits, shutdown, and recovery must never mark a record complete.
Unreadable or inconsistent data must produce an error rather than being skipped.

### R3: One record cannot block global consumption

Records may be delivered and committed out of order. A missing commit for one record must not prevent
later records from being delivered, retried, committed, or physically reclaimed.

Teak must not base correctness on a single contiguous consumption watermark.

### R4: Delivery is at least once

An uncommitted record becomes eligible again after its delivery lease expires. All process-local
leases expire on restart, so every remaining pending record is recovered. Duplicate delivery is
allowed; lost delivery is not.

### R5: Resource limits apply backpressure

Ready and in-flight queues must be bounded. Reaching a configured limit may delay reads or retries,
but must not delete, acknowledge, or skip persistent records.

## 3. Producer Requirements

- Multiple goroutines may call a stream concurrently.
- Sequence numbers are unique and increase within a stream.
- Data and the durable stream tail are updated atomically.
- `BatchWrite` is all-or-nothing. Oversized batches fail without automatic partial splitting.
- v0.1 has no write mode that returns before durability is known.

## 4. Consumer Requirements

- Multiple goroutines may call `Read` concurrently.
- `Read` blocks until at least one record is available, the context is canceled, or the stream closes.
- A delivery has an opaque receipt, attempt number, and lease deadline.
- `Commit` durably removes the corresponding pending record.
- `Retry` releases a delivery for delayed redelivery.
- `Extend` extends a valid delivery lease for long-running work.
- Lease expiry makes abandoned work eligible for redelivery.
- New work and due retries are scheduled fairly so neither can starve the other.
- Stale or duplicate operations must not affect another record.

## 5. Dead-letter Requirements

- Dead-lettering is always an explicit caller decision.
- Moving a pending record to dead letter is atomic.
- A dead-letter record retains its original stream, sequence, payload, and failure reason.
- Requeue creates a new pending sequence while retaining origin information.
- Teak never automatically deletes dead letters.

## 6. Lifecycle Requirements

- Only one process may open a database directory.
- A factory owns the shared Badger instance and its named streams.
- Closing a stream stops new operations and waits for its goroutines without deleting pending data.
- Closing the factory closes all streams before closing Badger.
- Reopening reconstructs pending work from stored records, not from an optimistic scan cursor.

## 7. Observability and Privacy

Teak exposes counts and ages for pending, ready, in-flight, retry, and dead-letter records, plus write,
delivery, commit, retry, lease-expiry, and storage-error counters.

Factory statistics expose value-log GC runs, rewrites, no-op passes, errors, and recovered maintenance
panics. Maintenance failures may increase disk usage but cannot acknowledge or delete queue records.

Errors and structured logs include the operation, stream, and sequence when available. Payload bytes
must never be logged.

Logging is opt-in through an injected `slog.Logger`. Teak recovers panics only at extension and owned
goroutine boundaries where it can preserve queue state; it does not silently convert internal
invariant failures into successful operations.

## 8. Acceptance Criteria

v0.1 is releasable only when tests demonstrate:

- no acknowledged write is lost across clean and unclean restart tests;
- an indefinitely failing record does not stop later records from completing;
- every uncommitted record is redelivered after lease expiry and process restart;
- out-of-order commits remain complete after restart;
- dead-letter transfer is atomic at every injected failure boundary;
- queue bounds, cancellation, and shutdown do not leak goroutines or discard records;
- the race-enabled suite passes on Windows and Linux.
