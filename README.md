# Teak

Teak is an embedded durable, at-least-once work queue for Go, backed by
[BadgerDB](https://github.com/hypermodeinc/badger). It supports concurrent producers and consumers,
out-of-order completion, visibility leases, retry, dead letters, and restart recovery.

The project is inspired by
[SharpAbp.Abp.Faster](https://github.com/cocosip/sharp-abp/tree/master/framework/src/SharpAbp.Abp.Faster),
but uses a Badger-native pending-record model. FASTER physical-address gaps are not applied to Teak's
logical sequences.

## Guarantees

- `Write` and `BatchWrite` return only after Badger synchronously commits the payload and durable tail.
- A pending record is removed only by a successful `Commit` or atomic `DeadLetter` transfer.
- Records complete independently; one missing commit never blocks later records.
- Lease expiry, consumer failure, and restart make uncommitted records eligible again.
- Delivery is at least once. External side effects must be idempotent.
- Ready, retry, and in-flight state are bounded; backpressure never acknowledges data.
- Corrupt or conflicting durable state returns an error instead of being skipped or repaired silently.

Teak v0.1 is a work queue for one process. It is not a retained event log, multi-process broker,
pub/sub system, or exactly-once transaction coordinator.

## Requirements And Installation

- Go 1.26 or later
- A local filesystem directory that supports reliable flush semantics
- One process opening a given database directory at a time

```text
go get github.com/cocosip/teak
```

Module path: `github.com/cocosip/teak`

## Quickstart

```go
package main

import (
    "context"
    "errors"
    "log"

    "github.com/cocosip/teak"
)

func main() {
    if err := run(context.Background()); err != nil {
        log.Fatal(err)
    }
}

func run(ctx context.Context) (returnErr error) {
    factory, err := teak.New(teak.DefaultOptions("./queue-data"))
    if err != nil {
        return err
    }
    defer func() {
        returnErr = errors.Join(returnErr, factory.Close(context.Background()))
    }()

    jobs, err := factory.Open(ctx, "jobs")
    if err != nil {
        return err
    }

    position, err := jobs.Write(ctx, []byte("job payload"))
    if err != nil {
        return err
    }
    log.Printf("stored sequence %d", position.Seq)

    deliveries, err := jobs.Read(ctx, 1)
    if err != nil {
        return err
    }

    delivery := deliveries[0]
    log.Printf("processing sequence %d attempt %d", delivery.Position.Seq, delivery.Attempt)

    // Perform the business side effect before Commit. Use stream/sequence or a
    // business key to make that side effect idempotent.
    return jobs.Commit(ctx, delivery)
}
```

`Read` blocks until it can lease at least one record, its context is canceled, or the log closes. It
may return fewer records than requested.

## Configuration

Configuration types provide copy-returning `With...` methods, so options can be safely derived and
chained. A named log override inherits zero-valued fields from the default log configuration.

```go
logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

options := teak.DefaultOptions("./queue-data").
    WithLogger(logger).
    WithMaintenanceInterval(10 * time.Minute).
    WithValueLogGCDiscardRatio(0.5).
    WithDefaultLog(teak.DefaultLogConfig().
        WithPrefetchCapacity(512).
        WithMaxInFlight(128).
        WithVisibilityTimeout(time.Minute).
        WithRetryBackoff(teak.BackoffConfig{}.
            WithInitial(2 * time.Second).
            WithMax(2 * time.Minute).
            WithMultiplier(2))).
    WithLog("critical", teak.LogConfig{}.
        WithMaxInFlight(32).
        WithVisibilityTimeout(5 * time.Minute))

factory, err := teak.New(options)
```

Default values:

| Setting | Default |
|---|---:|
| Fresh-record prefetch capacity | 1,024 |
| Maximum in-flight deliveries | 1,024 |
| Visibility timeout | 30 seconds |
| Retry backoff | 1 second to 1 minute, multiplier 2 |
| Value-log GC interval | 5 minutes |
| Value-log GC discard ratio | 0.5 |

Set `WithMaintenanceInterval(0)` to disable automatic value-log GC and call
`Factory.RunMaintenance` from the host scheduler instead. Durability settings are internal;
configuration cannot disable synchronous writes.

## Core API

One `Factory` owns a Badger database and caches named logs:

| Method | Purpose |
|---|---|
| `Open(ctx, name)` | Open or return the cached competing-consumer log |
| `RunMaintenance(ctx)` | Run one value-log GC attempt |
| `Stats()` | Read maintenance counters |
| `Close(ctx)` | Stop maintenance, close logs, and close Badger |

A `Log` is one named durable queue:

| Method | Purpose |
|---|---|
| `Name()` | Return the validated stream name |
| `Write(ctx, payload)` | Durably append one payload |
| `BatchWrite(ctx, payloads)` | Atomically append all payloads or none |
| `Read(ctx, count)` | Lease up to `count` pending records |
| `Commit(ctx, deliveries...)` | Durably delete successfully processed records |
| `Retry(ctx, deliveries...)` | Release records for delayed in-process redelivery |
| `Extend(ctx, duration, deliveries...)` | Extend valid visibility leases |
| `DeadLetter(ctx, delivery, reason)` | Atomically move one pending record to dead letter |
| `DeadLetters(ctx, from, count)` | Read an ordered dead-letter page |
| `Requeue(ctx, deadLetter)` | Atomically restore a dead letter under a new sequence |
| `Stats()` | Read durable and process-local queue statistics |
| `Close(ctx)` | Permanently close this named handle for the factory lifetime |

Log names must match `[A-Za-z0-9._-]{1,100}`. `BatchWrite` is all-or-nothing and Teak never splits it
silently. The combined payload limit is 32 MiB, while Badger may reject a smaller batch if encoded
transaction overhead reaches its internal limit.

`Position.Seq` is monotonic within a log. `Delivery` also exposes copied payload bytes, attempt,
deadline, and optional requeue origin. Delivery receipts and dead-letter identities are opaque and
validated by Teak; modifying exposed positions cannot authorize another record.

## Processing Outcomes

Commit only after successful processing:

```go
deliveries, err := jobs.Read(ctx, 16)
if err != nil {
    return err
}
for _, delivery := range deliveries {
    if err := handle(delivery.Payload); err != nil {
        if retryErr := jobs.Retry(ctx, delivery); retryErr != nil {
            return errors.Join(err, retryErr)
        }
        continue
    }
    if err := jobs.Commit(ctx, delivery); err != nil {
        return err
    }
}
```

For long-running work, extend the current lease before its deadline:

```go
if err := jobs.Extend(ctx, 30*time.Second, delivery); err != nil {
    return err
}
```

Move permanently invalid work to dead letter only through an explicit application decision:

```go
if err := jobs.DeadLetter(ctx, delivery, "unsupported job version"); err != nil {
    return err
}

deadLetters, err := jobs.DeadLetters(ctx, teak.Position{}, 100)
if err != nil {
    return err
}
for _, deadLetter := range deadLetters {
    if _, err := jobs.Requeue(ctx, deadLetter); err != nil {
        return err
    }
}
```

Retry deadlines, leases, and scan cursors are process-local optimizations. The pending key remains
durable. Restart discards those process-local states and makes every uncommitted record eligible with
attempt 1.

## Typed JSON API

The optional `typed` package applies a codec without changing the byte queue's durability or lifecycle:

```go
package main

import (
    "context"

    "github.com/cocosip/teak"
    teaktyped "github.com/cocosip/teak/typed"
)

type Job struct {
    ID   string `json:"id"`
    Kind string `json:"kind"`
}

func useTypedLog(ctx context.Context, factory teak.Factory) error {
    raw, err := factory.Open(ctx, "jobs")
    if err != nil {
        return err
    }
    jobs, err := teaktyped.JSON[Job](raw)
    if err != nil {
        return err
    }

    if _, err := jobs.Write(ctx, Job{ID: "42", Kind: "index"}); err != nil {
        return err
    }
    deliveries, err := jobs.Read(ctx, 1)
    if err != nil {
        return err
    }
    return jobs.Commit(ctx, deliveries...)
}
```

Custom codecs implement `typed.Codec[T]`. Encoding finishes before persistence, so an encode failure
writes nothing. A decode failure returns `*typed.DecodeError` with the raw leased deliveries; callers
must explicitly retry, commit, or dead-letter each one through `typed.Log.Raw()`. Codec panics become
`*typed.CodecPanicError` and never acknowledge data.

## Errors

Use `errors.Is` with Teak's exported sentinels:

| Error | Meaning |
|---|---|
| `ErrClosed` | Factory or log has started closing |
| `ErrInvalidOptions` | Unsupported configuration |
| `ErrInvalidName` | Invalid log name |
| `ErrInvalidCount` | Non-positive read or page count |
| `ErrInvalidDelivery` | Delivery belongs to another log or has no valid identity |
| `ErrInvalidDeadLetter` | Dead letter belongs to another log or was modified |
| `ErrStaleDelivery` | Lease expired or delivery was already completed |
| `ErrInvalidExtension` | Lease extension is not positive |
| `ErrNotFound` | Pending or dead-letter record is absent |
| `ErrBatchTooLarge` | Batch exceeds Teak or Badger's atomic transaction limit |
| `ErrSequenceExhausted` | Log consumed every uint64 sequence and cannot assign more |
| `ErrCorruptStorage` | Schema, envelope, tail, or durable state is inconsistent |

Context cancellation and deadline errors are returned unchanged. If a commit result is not obtained
because the process terminates, the record remains eligible for at-least-once redelivery; consumers
must tolerate duplicate business effects.

## Observability And Maintenance

`Log.Stats` reports durable tail, pending and dead-letter counts, process-local ready/retry/in-flight
counts, oldest pending age, writes, deliveries, commits, retries, lease expirations, duplicate scan
attempts, backpressure, and storage errors.

`Factory.Stats` reports value-log GC runs, rewrites, no-op passes, failures, and recovered maintenance
panics. Maintenance affects disk reclamation only; it is never part of acknowledgement correctness.

Inject an optional `slog.Logger` with `WithLogger`. Teak logs storage and maintenance failures with
operation, stream, sequence, error, and panic stack when available. It does not log payload bytes or
dead-letter reasons, and it does not emit one log event per successful record.

## Shutdown And Recovery

Stop producers and consumers, then close the factory:

```go
if err := factory.Close(shutdownCtx); err != nil {
    return err
}
```

Closing wakes blocked readers, waits for active operations, and never deletes pending records. Closing
an individual `Log` is terminal for that cached handle; create a new factory to reopen the durable
stream. Do not copy an open Badger directory as a backup.

## Benchmarks

The repository includes five benchmarks in `benchmark_test.go`:

```text
go test -count=1 -run '^$' -bench . -benchmem -benchtime=10x .
```

Current short baseline, measured on 2026-09-20 with Windows amd64, Intel Core Ultra 9 185H,
Go 1.27.1, and Badger v4.9.6:

| Benchmark | Operation | ns/op | B/op | allocs/op |
|---|---|---:|---:|---:|
| `DurableWrite` | One synchronous durable write | 16,180 | 1,988 | 52 |
| `AtomicBatchWrite100` | One atomic batch of 100 records | 91,330 | 66,257 | 1,658 |
| `ConcurrentStreams` | One synchronous write across eight streams | 10,960 | 3,182 | 61 |
| `DeliveryFanOut` | One concurrent read and commit | 25,900 | 86,352 | 87 |
| `OutOfOrderCommit32` | Write, read, and reverse-order commit of 32 records | 127,330 | 179,442 | 1,636 |

This `10x` run is a smoke baseline, not a throughput SLA. `SyncWrites=true` is enabled. Compare results
only on the same host, filesystem, power policy, Go version, Badger version, payload size, and benchmark
duration. See [docs/benchmarks.md](docs/benchmarks.md) for the maintained baseline notes.

## Verification

```text
go build ./...
go vet ./...
go test ./...
go test -race -timeout 10m ./...
go run -mod=readonly github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1 run ./...
```

CI runs the build, vet, pinned lint, and race-enabled test gates on Windows and Linux.

## Documentation

- [Requirements](docs/requirements.md)
- [Architecture and protocols](docs/design.md)
- [API guide](docs/api.md)
- [Operations guide](docs/operations.md)
- [Benchmark baseline](docs/benchmarks.md)
- [Decision records](docs/decisions.md)
- [Development status](docs/development.md)
- [Testing strategy](docs/testing.md)

License: [MIT](LICENSE)
