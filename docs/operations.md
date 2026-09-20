# Operations Guide

## Durability Model

Keep the Badger directory on a local filesystem with reliable flush semantics. Teak forces
`SyncWrites=true`; a successful write includes its pending envelope and durable tail in one transaction.
Committed keys are deleted independently, so an older failed record does not retain or block later
records.

Do not copy an open database directory as a backup. Stop and close the factory before taking a
filesystem-level copy. Only one process may open a directory at a time.

## Consumer Rules

Delivery is at least once. A process may complete an external side effect and stop before `Commit`, so
the same sequence can be delivered again. Use `<stream>/<sequence>` or a stable business identifier as
an idempotency key in the external system.

Choose a visibility timeout longer than normal processing. Call `Extend` before the deadline for known
long-running work. Use `Retry` for recoverable failures and `DeadLetter` only after an explicit
application decision. A timeout or retry count never deletes data.

## Capacity And Backpressure

`PrefetchCapacity` bounds the fresh queue and `MaxInFlight` bounds active leases. The retry lane is
bounded by their combined capacity. At a bound, reads wait for commits, retries, or lease expiry;
persistent records remain untouched. Size these limits from payload memory, worker concurrency, and
the acceptable number of outstanding retries. Completions (`Commit`, `DeadLetter`) serialize per log
across the synchronous transaction, so throughput scales by spreading work across logs, not by adding
consumers to one log.

Atomic batch payloads are limited to 32 MiB. Use application-sized batches below that limit rather than
retrying one oversized batch as smaller pieces after an ambiguous result. Teak itself never silently
splits a batch.

## Monitoring

Alert on increasing `Pending`, `OldestPendingAge`, `Retry`, `StorageErrors`, or `ValueLogGCErrors`.
`BackpressureEvents` indicates configured limits are constraining reads. A nonzero
`MaintenancePanics` means automatic GC stopped and should be investigated; queue delivery remains
correct because GC is not part of acknowledgement.

Inject a `slog.Logger` to receive storage, recovery, and maintenance failures. Teak logs operation,
stream, sequence, error, and maintenance panic stack when available. It never logs payload bytes or
dead-letter reasons. Routine successful records are counters, not log lines.

## Value-log GC

The default maintenance interval is five minutes with a discard ratio of 0.5. Disable automatic GC
with `WithMaintenanceInterval(0)` when the host owns scheduling, then call `RunMaintenance`. No-rewrite
results are successful no-op passes. GC failure can increase disk usage but cannot acknowledge work.

## Shutdown And Restart

Cancel producers and consumers, close individual logs if desired, then close the factory. Closing wakes
blocked readers and waits for active calls. It does not delete pending records. On restart all remaining
pending keys are immediately eligible with attempt 1; old process-local leases and retry deadlines are
discarded.

If opening reports `ErrCorruptStorage`, stop the affected workflow and preserve the directory for
diagnosis. Teak rejects unknown schemas, unknown envelope versions, tail regression, and conflicting
pending/dead-letter state rather than skipping or overwriting data.
