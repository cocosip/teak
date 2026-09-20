# Decision Records

These decisions define the current v0.1 baseline. Earlier draft choices based on FASTER physical
addresses are superseded by ADR-0010.

## ADR-0001: Go 1.26 baseline - Accepted

`go.mod` declares Go 1.26. Interval-driven tests use the stable `testing/synctest` package.

## ADR-0002: BadgerDB v4 storage - Accepted

Use `github.com/dgraph-io/badger/v4`. Development moved to the
`github.com/hypermodeinc/badger` repository, but the canonical Go module and import path remains
`github.com/dgraph-io/badger/v4`.

## ADR-0003: One shared database with prefixed streams - Accepted

One factory owns one Badger database. Streams use validated names and separate key prefixes. This
avoids multiple directory locks and duplicate Badger memory budgets.

## ADR-0004: Synchronous durable writes only in v0.1 - Accepted

Badger is opened with `SyncWrites=true`. `Write`, `BatchWrite`, `Commit`, and `DeadLetter` return only
after their transaction result is known. The former draft's lossy buffered mode is removed. Future
group commit must preserve the same return-time durability contract.

## ADR-0005: Byte core with a JSON typed wrapper - Accepted

The root queue contract stores bytes. A generic typed wrapper owns the `Codec[T]` boundary and bundles
JSON support in v0.1. Additional codecs do not change the storage contract.

## ADR-0006: Export and retained replay are deferred - Accepted

v0.1 is a work queue, not a retained event log. Committed records are deleted. Export and replay, if
needed later, require a separate retention design rather than additions to the queue API.

## ADR-0007: Public module path - Accepted

The module path is `github.com/cocosip/teak`.

## ADR-0008: MIT license - Accepted

Teak is MIT licensed. Badger's Apache-2.0 license is compatible with this usage.

## ADR-0009: One competing-consumer group per stream - Accepted

Readers of one stream compete for work. Consumer groups and pub/sub fan-out are deferred because they
would require independent durable record state per group.

## ADR-0010: Pending keys replace gap merging and truncate watermarks - Accepted

The existence of `d/<seq>` is the durable pending state. `Commit` deletes that key independently.
There is no continuous committed watermark, address-match tolerance, completed-range cap, forced gap
completion, or prefix truncation protocol.

This supersedes the earlier sequence-lease and FASTER-style gap design. FASTER's gaps are artifacts of
physical byte addresses; applying the same tolerance to Teak sequence numbers could delete a delayed
concurrent write.

## ADR-0011: Process-local delivery leases with durable pending records - Accepted

Delivery leases, retry deadlines, and scan cursors are in memory. The pending key remains durable while
a record is in flight. Restart discards leases and redelivers every remaining pending key, providing
at-least-once behavior without a fragile persisted cursor.

## ADR-0012: Explicit atomic dead letter - Accepted

Teak never discards a record after a timeout or attempt limit. The caller may atomically move a record
from pending to dead letter. Requeue assigns a new sequence and retains origin information.

## ADR-0013: Optional structured logging and narrow panic recovery - Accepted

Applications may inject a `slog.Logger`. Teak logs storage and recovery failures with operation,
stream, sequence, error, and panic stack when applicable; payload bytes and dead-letter reasons are
never logged. Routine throughput is exposed through `Stats` rather than one log event per record.

Teak does not blanket-recover panics in synchronous storage code because that would conceal invariant
violations. Panics from caller-provided typed codecs are converted to `CodecPanicError`, leaving the
delivery pending. Owned maintenance goroutines recover only at their outer boundary, log the stack,
and stop that maintenance loop without changing queue state.
