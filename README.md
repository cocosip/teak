# Teak

Teak is a durable, at-least-once work queue for Go, backed by
[BadgerDB](https://github.com/hypermodeinc/badger). It is designed for concurrent producers and
consumers, out-of-order completion, retry after consumer failure, and restart recovery.

The project is inspired by
[SharpAbp.Abp.Faster](https://github.com/cocosip/sharp-abp/tree/master/framework/src/SharpAbp.Abp.Faster),
but uses a Badger-native pending-record model. Teak does not copy FASTER's physical-address gap
handling into logical sequence processing.

## Guarantees

- A successful write is durably stored before it returns.
- A record is removed only after an explicit successful commit or an atomic move to dead letter.
- An uncommitted record does not block later records from being delivered or committed.
- Consumer failures and server restarts cause redelivery, not data loss.
- Delivery is at least once; consumers must make external side effects idempotent.

## Status

Teak is in active development. Durable storage, bounded delivery scheduling, the public byte API, the
optional JSON typed wrapper, crash-process recovery tests, and value-log maintenance are implemented.
Release benchmarks and final operational documentation remain before v0.1.0.

## Quickstart

```go
package main

import (
    "context"
    "log"

    "github.com/cocosip/teak"
)

func main() {
    ctx := context.Background()
    options := teak.DefaultOptions("./queue-data").WithDefaultLog(
        teak.DefaultLogConfig().WithMaxInFlight(256),
    )
    factory, err := teak.New(options)
    if err != nil {
        log.Fatal(err)
    }
    defer func() { _ = factory.Close(ctx) }()

    jobs, err := factory.Open(ctx, "jobs")
    if err != nil {
        log.Fatal(err)
    }
    if _, err := jobs.Write(ctx, []byte("job payload")); err != nil {
        log.Fatal(err)
    }
    deliveries, err := jobs.Read(ctx, 1)
    if err != nil {
        log.Fatal(err)
    }
    if err := jobs.Commit(ctx, deliveries...); err != nil {
        log.Fatal(err)
    }
}
```

Use `Retry` for recoverable processing failures and `DeadLetter` for an explicit permanent-failure
decision. A consumer that performs external side effects should use the delivery sequence as part of
an idempotency key before calling `Commit`.

## Documentation

- [Requirements](docs/requirements.md)
- [Architecture and protocols](docs/design.md)
- [API guide](docs/api.md)
- [Operations guide](docs/operations.md)
- [Benchmark baseline](docs/benchmarks.md)
- [Decision records](docs/decisions.md)
- [Development status](docs/development.md)
- [Testing strategy](docs/testing.md)
- [Changelog](CHANGELOG.md)

Module: `github.com/cocosip/teak`

Baseline: Go 1.26, BadgerDB v4.9.x

License: [MIT](LICENSE)
