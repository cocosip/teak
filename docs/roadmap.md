# Roadmap

Development order and exit criteria. Test gates are defined in [testing.md](testing.md).

## M0 — Bootstrap

- `go.mod` (Go 1.26; blocked only on the ADR-0007 module path), package skeleton directories,
  golangci-lint config, CI workflow (build + vet + test `-race`, Windows & Linux).
- **Exit**: `go build ./...` and `go vet ./...` clean on an empty skeleton; CI green.

## M1 — badgerstore

- Key encoding (`t/<log>/d/<big-endian seq>` + meta prefix), sequence allocator (in-process atomic
  counter + leased watermark persistence), `Store` interface implementation.
- **Exit**: store unit tests pass, including reopen-recovery of the seq lease and `ScanFrom`
  ordering; `-race` clean.

## M2 — Core mechanics

- `ranges.go` (sorted set, tolerance merge, gap tracking), `scanner.go` (bounded channel,
  backpressure, close semantics), `tasks.go` (Complete/Truncate wiring), byte-level `Logger`.
- **Exit**: table-driven merge tests; synctest-based scanner/task tests; single-producer /
  single-consumer happy path; metrics counters.

## M3 — Factory, typed wrapper, example

- `Factory` (shared DB, named streams, per-stream config), `TypedLogger[T]` + JSON codec,
  `examples/quickstart`.
- **Exit**: multi-goroutine writers plus multiple consumers with out-of-order commits; gap
  force-skip scenario test passes.

## M4 — Hardening

- Crash-recovery tests (unclean shutdown between seq-lease persists, mid-scan, mid-truncate),
  `Close` semantics (ctx cancel, channel drain), gap warning / force-skip timing, Windows
  path & locking cases.
- **Exit**: full integration suite green with `-race` on Windows + Linux; goroutine-leak check
  over the background-task suite.

## M5 — v0.1.0 release

- Benchmarks (write immediate vs buffered, batch sizes, read fan-out, truncate + value-log GC cost)
  recorded in docs; configuration reference in README; godoc pass; CHANGELOG; license file
  (ADR-0008); version tag.
