# Architecture and Protocols

This document describes the target v0.1 design. Requirements are defined in
[requirements.md](requirements.md), and the gap between this design and the current code is tracked in
[development.md](development.md).

## 1. Design Summary

Teak uses Badger key existence as the durable queue state:

- `d/<seq>` exists: the record is pending;
- `d/<seq>` is deleted by `Commit`: the record is complete;
- `d/<seq>` is atomically replaced by `x/<seq>`: the record is dead-lettered.

Each record completes independently. There is no global committed watermark, completed-range set,
gap tolerance, or timeout that advances progress past unprocessed data. This is the main difference
from the FASTER reference implementation: FASTER exposes physical byte addresses, while Teak controls
logical sequence numbers and can model pending work directly.

## 2. Package Boundaries

```text
github.com/cocosip/teak
├── root package         public Factory, Log, Delivery, Position, Options, errors, and stats
├── internal/store      Badger persistence and key encoding
├── internal/dispatch   ready queue, delivery leases, retry scheduling, and fairness
└── typed               optional generic codec wrapper; JSON codec in v0.1
```

The root package owns the supported contract. Badger options and transactions are not exposed through
that contract. This prevents callers from disabling durability or depending on the on-disk layout.

## 3. On-disk Model

One factory owns one Badger database. Named streams share it and are isolated by key prefix:

```text
t/<stream>/d/<8-byte big-endian seq>   pending record envelope
t/<stream>/x/<8-byte big-endian seq>   dead-letter envelope
t/<stream>/m/tail                      highest allocated sequence
t/<stream>/m/schema                    on-disk schema version
```

Names are restricted to `[A-Za-z0-9._-]{1,100}`. Sequence encoding preserves numeric ordering.

The envelope has an explicit format version and contains a copied payload. A dead-letter envelope also
contains the original stream and sequence, failure reason, and dead-letter time. Unknown versions are
errors; the reader does not guess or skip them.

Schema version 1 uses these big-endian binary envelopes:

```text
pending: version:u8, created_unix_nano:u64, origin_seq:u64,
         origin_stream_len:u32, origin_stream:bytes, payload_len:u64, payload:bytes

dead:    version:u8, created_unix_nano:u64, dead_lettered_unix_nano:u64, origin_seq:u64,
         origin_stream_len:u32, origin_stream:bytes, payload_len:u64, payload:bytes,
         reason_len:u32, reason:bytes
```

Schema and envelope versions are both `1` for v0.1. Lengths are validated before allocation and
trailing data is rejected. On open, the durable tail must be at least the greatest pending or
dead-letter sequence; regression is an error because continuing could overwrite or reuse a sequence.

## 4. Write Protocol

Each stream has a write coordinator. Concurrent callers enter the coordinator, which serializes
sequence allocation within that stream. Different streams may write concurrently.

For `Write`, one Badger transaction stores the record and the new `m/tail`. `BatchWrite` stores every
record in its assigned contiguous range and updates `m/tail` in one transaction. The in-memory tail is
advanced only after a successful transaction.

Badger is opened with `SyncWrites=true`, and the public configuration cannot turn it off. This makes a
successful result a durable result. An oversized batch returns a batch-size error; it is not silently
split into partially successful writes.

There is no lossy buffered mode in v0.1. A future group-commit implementation may batch coordinator
requests, but every caller must wait for its transaction result.

## 5. Dispatch and Delivery

Each stream owns one dispatcher with three bounded structures:

1. a fresh-record lane filled by incremental Badger scans and local write notifications;
2. a retry lane ordered by next-attempt time;
3. an in-flight table keyed by sequence and opaque receipt.

Scans use short-lived read transactions and copy envelopes before closing the transaction. The
dispatcher never waits on a full channel while retaining a Badger iterator.

When fresh and retry work are both ready, the dispatcher alternates one delivery from each lane. This
prevents a poison record from starving new work and prevents a continuous write stream from starving
retries.

`Read` creates an in-memory delivery lease. The persistent `d/<seq>` key remains untouched while the
caller works:

```text
pending -> in flight -> committed
                    \-> retry delay -> pending
                    \-> lease expiry -> pending
                    \-> dead letter
```

`Extend` changes only a valid in-memory lease. Limits apply backpressure; they never alter persistent
state.

## 6. Commit, Retry, and Dead Letter

`Commit` validates that each opaque receipt belongs to the stream and delivery, then deletes its
pending key in a synchronous transaction. Multiple deliveries may be committed in one transaction.
Because deletion is per key, sequence 2 can remain pending while sequences 3 through 1,000 complete.

`Retry` removes a valid delivery from the in-flight table and inserts it into the delayed retry lane.
Retry state is an optimization, not the durable source of truth; the pending key remains present.

`DeadLetter` reads the pending envelope, writes the dead-letter envelope, and deletes the pending key
in one transaction. Requeue validates an opaque owner and immutable sequence identity before performing
the inverse using a new sequence and retaining origin information.

Duplicate and stale delivery operations are idempotent where possible and otherwise return a sentinel
error. They can never commit a different record.

## 7. Restart and Failure Recovery

Delivery leases and scan cursors are process-local. On restart, all remaining `d/` keys are pending
again; committed keys are already absent. The dispatcher scans stored pending data rather than trusting
a persisted cursor that may have advanced past unfinished work.

This produces at-least-once behavior. A consumer may finish its external side effect and crash before
`Commit`, causing a duplicate after restart. Teak cannot make that external boundary exactly once, so
consumers must use idempotency keys or an equivalent business transaction.

Storage errors, corrupt envelopes, tail regression, and unknown schema versions stop the affected
operation and surface an error. Recovery never converts them into successful skips.

## 8. Public API Shape

The supported root API is:

```go
type Position struct {
    Seq uint64
}

type Factory interface {
    Open(ctx context.Context, name string) (Log, error)
    RunMaintenance(ctx context.Context) error
    Stats() FactoryStats
    Close(ctx context.Context) error
}

type Log interface {
    Name() string
    Write(ctx context.Context, data []byte) (Position, error)
    BatchWrite(ctx context.Context, data [][]byte) ([]Position, error)
    Read(ctx context.Context, count int) ([]Delivery, error)
    Commit(ctx context.Context, deliveries ...Delivery) error
    Retry(ctx context.Context, deliveries ...Delivery) error
    Extend(ctx context.Context, extension time.Duration, deliveries ...Delivery) error
    DeadLetter(ctx context.Context, delivery Delivery, reason string) error
    DeadLetters(ctx context.Context, from Position, count int) ([]DeadLetter, error)
    Requeue(ctx context.Context, deadLetter DeadLetter) (Position, error)
    Stats() (Stats, error)
    Close(ctx context.Context) error
}
```

`Delivery` exposes payload, position, attempt, and deadline, but keeps its receipt opaque. The typed
wrapper delegates lifecycle and delivery operations to the byte-level log and applies only codec work.
Decode errors retain the raw deliveries so callers can explicitly retry or dead-letter them.
Typed constructors return validation errors rather than panicking on invalid arguments.

## 9. Configuration

```go
type Options struct {
    Dir                    string
    DefaultLog             LogConfig
    Logs                   map[string]LogConfig
    Logger                 *slog.Logger
    MaintenanceInterval    time.Duration
    ValueLogGCDiscardRatio float64
}

type LogConfig struct {
    PrefetchCapacity   int
    MaxInFlight        int
    VisibilityTimeout time.Duration
    RetryBackoff       BackoffConfig
}
```

Initial defaults are:

| Setting | Default |
|---|---:|
| Prefetch capacity | 1,024 records |
| Maximum in-flight deliveries | 1,024 records |
| Visibility timeout | 30 seconds |
| Retry backoff | exponential, 1 second to 1 minute |
| Value-log GC interval | 5 minutes |
| Value-log GC discard ratio | 0.5 |

Configuration uses `time.Duration`, validates every bound, and cannot weaken synchronous durability.
Options, log settings, and retry backoff also provide copy-returning `With...` methods for fluent
configuration. Named-log builders clone the overrides map so derived configurations do not alias.
Setting the maintenance interval to zero disables automatic GC; callers may still invoke
`Factory.RunMaintenance`. GC errors and recovered maintenance panics are exposed by `Factory.Stats`.

## 10. Lifecycle

`Close` rejects new operations, cancels the dispatcher, waits for its goroutines, and leaves every
uncommitted record in Badger. A closed named log remains the factory's cached terminal handle; reopening
the durable stream requires a new factory. The factory closes streams before closing the shared database.

Cancellation before a request enters the stream coordinator has no persistent effect. Once an
accepted write or commit enters its transaction, Teak waits for a definitive result rather than
returning an ambiguous cancellation while durability is undecided.

## 11. Space Reclamation

Logical completion deletes individual pending keys, so it is not blocked by an older uncommitted
sequence. Badger compaction reclaims LSM tombstones. A low-frequency maintenance task may call value-log
GC, but GC failures affect disk usage only and never change queue state.

## 12. Observability

Stats include durable tail, pending, ready, in-flight, retry, and dead-letter counts; operation
counters; lease expirations; duplicate deliveries; oldest pending age and sequence; backpressure; and
storage errors. Logs include operation, stream, and sequence but never payload data.
