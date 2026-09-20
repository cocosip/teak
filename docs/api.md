# API Guide

This guide describes the supported v0.1 API. Storage internals under `internal/` are not public
contracts.

## Factory

Create one `Factory` per Badger directory and share it across the process. `New` opens and locks the
directory. `Factory.Open` caches a named competing-consumer log. `Factory.Close` stops maintenance,
closes logs, waits for active operations, and then closes Badger.

```go
options := teak.DefaultOptions("./queue-data").
    WithLogger(logger).
    WithDefaultLog(teak.DefaultLogConfig().
        WithPrefetchCapacity(512).
        WithMaxInFlight(128).
        WithVisibilityTimeout(time.Minute))

factory, err := teak.New(options)
```

Configuration builders return copies. `WithLog` copies its map, so independently derived options do
not alias. Raw Badger options are intentionally unavailable; synchronous writes cannot be disabled.

## Producing

`Write` returns one durable `Position`. `BatchWrite` writes every item and the new tail in one
transaction, or writes none. The atomic batch payload limit is 32 MiB; Badger may also reject a batch
whose encoded transaction exceeds its internal transaction limit.

Empty batches succeed with an empty position slice. A canceled context is honored before a durable
operation starts. Once a transaction has started, the call waits for a definitive result.

## Consuming

`Read` blocks until it leases at least one delivery, its context ends, or the log closes. It may return
fewer than the requested count. A `Delivery` contains copied payload bytes, sequence, attempt, deadline,
and optional requeue origin. Its receipt is deliberately opaque.

- `Commit` durably deletes only the delivered pending keys.
- `Retry` retains pending keys and schedules delayed redelivery in memory.
- `Extend` adds time to valid delivery leases.
- `DeadLetter` atomically replaces a pending key with a dead-letter key.

Receipt operations validate the log, receipt, sequence, and attempt. A copied delivery remains valid;
a forged, expired, already completed, or cross-log delivery cannot delete data.

## Dead Letters

`DeadLetters` returns an ordered bounded page. `Requeue` atomically removes a dead letter, appends a new
pending sequence, and carries the original stream and sequence into the new delivery. Teak never
automatically dead-letters or deletes a record.

## Typed API

`typed.JSON[T](log)` applies `encoding/json` while retaining the byte log's durability and lifecycle.
Custom codecs implement `typed.Codec[T]`. Encoding completes before persistence, so an encode error
writes nothing.

A decode error returns `*typed.DecodeError` containing every raw delivery from that read. Callers use
`Raw()` to retry, commit, or dead-letter those deliveries explicitly. Codec panics become
`*typed.CodecPanicError`; they never acknowledge the affected data.

## Errors

Use `errors.Is` with the exported sentinels, including `ErrClosed`, `ErrInvalidOptions`,
`ErrInvalidName`, `ErrInvalidDelivery`, `ErrStaleDelivery`, `ErrNotFound`, `ErrBatchTooLarge`, and
`ErrCorruptStorage`. Context cancellation errors are returned unchanged.

## Statistics

`Log.Stats` returns persistent pending/dead-letter counts and tail, process-local ready/retry/in-flight
counts, oldest pending age, operation counters, backpressure, and storage errors. `Factory.Stats`
returns value-log GC and maintenance-panic counters. Statistics are snapshots and are not a transaction
with concurrent producer or consumer operations.
