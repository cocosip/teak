# Development Status

Last reviewed against the repository on 2026-09-20 at commit `23f6c02`.

This document records implemented behavior, known redesign work, and the order of development. A
checked item means the behavior exists in code and has relevant tests; design approval alone does not
count as implementation.

## 1. Current Snapshot

| Area | Status | Evidence |
|---|---|---|
| Module and dependency baseline | Complete | `go.mod`: Go 1.26 and Badger v4.9.6 |
| License | Complete | MIT `LICENSE` |
| Linux and Windows CI | Complete | build, vet, and race-enabled tests in `.github/workflows/ci.yml` |
| golangci-lint rule set | Complete | `.golangci.yml` aligned with the `go-dicom` baseline |
| Durable pending-record store | Complete | `internal/store` and storage integration tests |
| Delivery dispatcher and leases | Complete | bounded `internal/dispatch` scheduler and fake-time tests |
| Public byte API | Complete | root `Factory`, `Log`, delivery, options, errors, and stats |
| Typed JSON wrapper | Complete | generic `typed.Log` and JSON codec |
| Crash and liveness hardening | Complete | killed-process boundaries, stress, GC, panic, and CI gates |

The current test suite passes with `go test ./...` and `go test -race -timeout 10m ./...` on the
review machine. Storage tests validate M1R; later milestone guarantees remain prospective.

## 2. Completed Storage Foundation

The persistence layer now provides:

- one shared synchronously written `badger.DB` owned by `Root`;
- cached and validated named streams with schema metadata;
- atomic single and batch append with a durable tail;
- versioned pending and dead-letter envelopes;
- bounded scans that return owned payload bytes;
- independent commit deletion, atomic dead-letter transfer, and atomic requeue;
- persistent counts and oldest-pending metadata.

## 3. Superseded Prototype Behavior

The M1R implementation removed these prototype behaviors:

| Removed behavior | Problem | Replacement |
|---|---|---|
| Sequence numbers leased in windows of 1,024 | Deliberately creates holes after reopen | Atomic record plus `m/tail` transaction |
| `DefaultOptions` inherits `SyncWrites=false` | A successful update is not a hard-reboot durability guarantee | Force `SyncWrites=true` |
| `Allocate` and `Put` are separate calls | A crash can leave allocated holes; callers can write arbitrary sequences | Store-level atomic append operations |
| `DeleteBelow` deletes a contiguous prefix | One uncommitted record would block reclamation, or skipping it would lose data | Delete individual committed pending keys |
| `ScanFrom` invokes callbacks inside one read transaction | Backpressure can retain a long-lived Badger iterator and snapshot | Bounded scan batches with copied values |
| Raw Badger options are public | Callers can disable required durability | Internal Badger configuration with safe Teak options |
| No envelope or schema version | Future decoding cannot distinguish incompatible data | Versioned pending and dead-letter envelopes |
| No atomic dead-letter operation | Permanent failures have no lossless escape path | Copy-to-dead-letter plus pending delete in one transaction |

## 4. Milestones

### M1R: Repair the persistence contract

- [x] Replace sequence leasing with serialized atomic append and durable `m/tail`.
- [x] Force synchronous writes and remove public Badger options.
- [x] Add versioned pending and dead-letter envelopes.
- [x] Add bounded `ScanBatch` with owned payload bytes.
- [x] Add atomic per-record commit deletion.
- [x] Add atomic dead-letter and requeue operations.
- [x] Replace lease, hole, and prefix-delete tests with transaction-failure and per-record tests.

Exit: storage tests prove that successful writes survive reopen, failed transactions do not advance
the tail, out-of-order deletes are independent, and dead-letter transfer is atomic.

### M2: Dispatcher and delivery leases

- [x] Implement bounded fresh, retry, and in-flight structures.
- [x] Alternate fresh and retry deliveries when both are ready.
- [x] Implement visibility timeout, `Retry`, and `Extend`.
- [x] Wake scans on local writes without polling for correctness.
- [x] Recover all pending keys after restart.
- [x] Implement cancellation and leak-free shutdown.

Exit: a permanently failing record is repeatedly available but does not prevent later records from
being delivered and committed; restart redelivers every uncommitted record.

### M3: Public API and typed wrapper

- [x] Add root `Factory`, `Log`, `Delivery`, options, stats, and sentinel errors.
- [x] Protect commit operations with opaque delivery receipts.
- [x] Add the JSON typed wrapper.
- [x] Add a runnable quickstart after the public API exists.

Exit: concurrent producers and consumers complete an end-to-end work-queue scenario using only the
public API.

### M4: Recovery and operational hardening

- [x] Add killed-subprocess crash tests around write, commit, and dead-letter boundaries.
- [x] Add corrupt-envelope and schema-version failure tests.
- [x] Add retry fairness, queue saturation, and slow-consumer stress tests.
- [x] Add value-log GC maintenance and metrics without coupling it to correctness.
- [x] Configure the full race-enabled suite on Windows and Linux; verify it locally on Windows.
- [x] Add golangci-lint execution to CI with a pinned tool version.

Exit: every acceptance criterion in [requirements.md](requirements.md) has an automated test, and CI
passes build, vet, lint, tests, and race tests.

### M5: v0.1 release

- [x] Benchmark durable single writes, atomic batches, concurrent streams, delivery fan-out, and
  out-of-order commits.
- [x] Publish configuration and operations guidance.
- [x] Complete API documentation and changelog.
- [ ] Tag v0.1.0 only after the on-disk schema is declared stable.

## 5. Progress Rules

- Update this file in the same change that completes or invalidates a milestone item.
- Never mark a design, interface, or test scenario complete before corresponding code and verification
  exist.
- Record environment-blocked checks explicitly rather than treating them as passing.
- Changes to the two reliability invariants require an ADR and explicit approval:
  no acknowledged data loss, and no global consumption blockage from an uncommitted record.
