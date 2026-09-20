# Testing Strategy

Tests must prove both safety and liveness:

- safety: no acknowledged write or uncommitted record is lost;
- liveness: one failed record does not prevent later records from completing.

Windows is the primary development platform. Windows and Linux run the race-enabled suite in CI.

## 1. Storage Coverage

The `internal/store` tests cover:

- key ordering, envelope round trips, corruption, and schema rejection;
- stream-name validation, reuse, and isolation;
- atomic append/tail behavior, oversized batches, clean reopen, and concurrent appends;
- bounded owned-value scans;
- independent and batch-atomic commits;
- failure-injected dead-letter transfer and origin-preserving requeue;
- persistent counts and oldest-pending metadata.

Killed-process recovery and public delivery behavior are added in later milestones.

The `internal/dispatch` tests use `testing/synctest` and a fake persistent source to cover receipt
validation, persistence failures, fresh/retry fairness, retry bounds, lease expiry and extension,
in-flight backpressure, receipt collision and entropy-failure rollback, write notification, restart
recovery, cancellation, source errors, and clean shutdown.

Root-package integration tests use the public API with real Badger storage. They cover out-of-order
commit across restart, poison-record liveness, dead-letter requeue, forged and cross-log receipts,
dead-letter identity mutation, public error mapping, concurrent producers and consumers, configuration
validation, terminal log close, and blocked-reader shutdown. The `typed` package tests JSON round trips,
constructor validation, codec panic conversion, and recoverable decode failures.

Killed-subprocess tests terminate the test binary inside uncommitted Badger transactions and directly
after successful append, commit, and dead-letter operations. Reopen assertions prove that each
boundary leaves the queue in the old or new atomic state, never a partially applied state. Maintenance
tests prove that GC errors and recovered worker panics are observable without altering pending data.

## 2. Target Test Layers

| Layer | Scope | Technique |
|---|---|---|
| Unit | key/envelope codecs, option validation, retry ordering, receipt validation | table-driven tests |
| Store integration | atomic append/tail, commit delete, dead letter, requeue, scan batches | real Badger in `t.TempDir()` |
| Dispatcher | leases, expiry, extension, fairness, backpressure, shutdown | `testing/synctest` with a fake store |
| End to end | concurrent writers/readers, out-of-order completion, restart recovery | public API plus real Badger |
| Crash recovery | interruption at transaction and lifecycle boundaries | killed subprocess and failure injection |
| Stress and race | queue saturation, slow consumers, repeated retry, concurrent close | `go test -race` |
| Benchmarks | durable writes, batches, streams, fan-out, deletion and GC | `go test -bench`, `b.ReportAllocs` |

## 3. Required Safety Scenarios

- A successful append remains after clean close, process termination, and reopen.
- Failure before transaction commit leaves neither data nor an advanced tail.
- Failure after transaction commit returns a definitive committed result when the process remains alive.
- An oversized batch writes no subset of its records.
- `Commit` deletes only the record identified by a valid delivery receipt.
- A stale or forged receipt cannot delete any record.
- Dead-letter transfer leaves either the pending record or the dead-letter record at every injected
  failure point, never neither.
- Corrupt or unknown envelopes stop the operation and remain stored.

## 4. Required Liveness Scenarios

- Sequence 2 may remain uncommitted while later sequences are delivered and committed.
- A `Retry` delay does not block fresh records.
- A continuous stream of fresh records does not starve due retries.
- An abandoned delivery is redelivered after its visibility timeout.
- `Extend` prevents premature redelivery during long processing.
- Restart immediately makes all remaining pending records eligible, without waiting for old leases.
- Full ready or in-flight bounds apply backpressure without record loss or goroutine leaks.

## 5. Timing and Goroutine Rules

Use `testing/synctest` for visibility deadlines, retry delays, and shutdown coordination. Do not use
real sleeps to prove timing behavior. A synctest bubble must finish with every owned goroutine exited.

Badger I/O runs outside fake-time assertions because filesystem operations are not durably blocked
inside a synctest bubble. Dispatcher tests use a fake store; integration tests use real Badger and
explicit synchronization.

## 6. CI Gates

The release gate is:

```text
go build ./...
go vet ./...
golangci-lint run ./...
go test ./...
go test -race -timeout 10m ./...
```

The workflow runs build, vet, pinned golangci-lint v2.13.1, and race-enabled tests on Windows and
Linux. The complete gate is verified locally on Windows; Linux execution is performed by CI because
the primary development host has no Linux runtime.
