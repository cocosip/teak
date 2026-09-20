# Testing Strategy

CI runs everything with `-race`. Windows is the primary dev platform; Linux also runs in CI.

## Layers

| Layer | Scope | Tools |
|---|---|---|
| Unit | range merging (tolerance, gaps, force-skip, MaxCompletedRanges cap), key codec round-trip & ordering, seq lease allocation/persistence | table-driven tests |
| Component | scanner backpressure & close semantics; Complete/Truncate interval behavior; buffered-mode flush batching | `testing/synctest` (fake time; stable since Go 1.25) |
| Integration | real Badger in `t.TempDir()`: multi-goroutine writers, multiple consumers, out-of-order `Commit`, `ForceCommitGap`, reopen recovery | plain `go test` |
| Crash recovery | unclean shutdown injection points: between seq-lease persists, mid-scan, mid-truncate; assert no watermark regression and the documented hole behavior | injected failures / killed subprocess |
| Benchmarks | write throughput (immediate vs buffered), batch sizes, read fan-out, truncate + value-log GC cost | `go test -bench`, `b.ReportAllocs` |

## Rules

- Every interval-driven mechanism is tested under `testing/synctest`; no real-time sleeps in tests.
- Gap / data-loss paths assert the documented warning semantics, not just absence of error.
- Badger options in tests use small memtable / value-log settings so flush and compaction paths
  are exercised quickly.
- The experimental goroutine-leak detector (Go 1.26 runtime) runs over the background-task suite
  to catch task goroutines leaked after `Close`.
- Benchmarks are informational until M5; the M4 exit gate is correctness only.
