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

Teak is in early development. The durable `internal/store` layer is complete: writes are synchronous,
pending records and sequence tails are atomic, and commit/dead-letter operations never depend on a
contiguous progress watermark. The bounded dispatcher implements fair retries, delivery leases,
restart recovery, and cancellation. The public API is the next implementation milestone.

The public `teak` API shown in the design is not implemented yet. See
[development.md](docs/development.md) for the verified status and next milestones.

## Documentation

- [Requirements](docs/requirements.md)
- [Architecture and protocols](docs/design.md)
- [Decision records](docs/decisions.md)
- [Development status](docs/development.md)
- [Testing strategy](docs/testing.md)

Module: `github.com/cocosip/teak`

Baseline: Go 1.26, BadgerDB v4.9.x

License: [MIT](LICENSE)
