# Decision Records

Lightweight ADRs. All decisions below were confirmed with the user on 2026-09-20; development
has started against them. Context and analysis live in [design.md](design.md).

## ADR-0001 — Go 1.26 baseline — Accepted

`go.mod` declares `go 1.26`. Badger v4 requires Go ≥ 1.23, so the 1.26 toolchain is backward
compatible with it. `testing/synctest` (stable since Go 1.25) is used to test the interval-driven
background tasks (Commit/Complete/Truncate) without real sleeps.

## ADR-0002 — Storage engine: hypermodeinc/badger (module dgraph-io/badger/v4) — Accepted

The GitHub repository moved to `hypermodeinc/badger` (v4.9.6, 2025-08) after Dgraph Labs was
acquired by Hypermode, but the Go module path is unchanged: the go.mod still declares
`module github.com/dgraph-io/badger/v4`, so that remains the canonical import path (importing
`hypermodeinc/…` fails with a module-path mismatch). Pure Go (no cgo), requires Go ≥ 1.24,
Windows supported. Concurrent blind writes are conflict-free (design.md §2.1) — the
load-bearing property for the append-only write path.

## ADR-0003 — One shared DB; streams isolated by key prefix — Accepted

The Factory owns a single `badger.DB`; each named stream uses the prefix `t/<log>/…`.
Rationale: one directory lock, one shared compaction/memtable budget, O(1) stream creation.
Trade-off: per-stream space reclaim is incremental range-delete by the background task, not
"delete the directory".

## ADR-0004 — Default write mode: Immediate — Accepted

`Immediate` commits a write transaction per Write/BatchWrite call — durable on return
(`SyncWrites` on). `Buffered` (in-memory aggregation + background flush, FASTER-like high
throughput with a lossy recent window) stays available as an explicit opt-in. Default to the
safe mode.

## ADR-0005 — Codecs: interface-only in core; JSON codec bundled — Accepted

`core` is byte-level; `typed` defines `Codec[T]` and ships `JSONCodec` only in v0.1. A binary
codec (msgpack or custom) can be added later without interface changes. Keeps dependencies minimal.

## ADR-0006 — Export deferred; not in the v0.1 Logger interface — Accepted

Range dump to files (mirroring `ExportAsync`) is real but not needed by the primary consume flow.
It can be added later as a separate optional `Exporter` interface without breaking the core
interface.

## ADR-0007 — Module path — Accepted

`module github.com/cocosip/teak`; repository `git@github.com:cocosip/teak.git`.

## ADR-0008 — License — Accepted

MIT; see [LICENSE](../LICENSE). Badger itself is Apache-2.0; MIT usage of an Apache-2.0
dependency is fine.

## ADR-0009 — Consumer position model — Accepted

One consumption position per stream; multiple consumers share it (work-queue semantics,
out-of-order commits healed by the gap merger) — same as the C# reference. Redelivery after a
crash is at-least-once. Multi-group pub/sub semantics can be added later (truncation would then
advance only to the slowest position).
