# Development Status

Last reviewed against the repository on 2026-09-24 after the full-code walkthrough pass.

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
| Crash and liveness hardening | Complete | killed-process boundaries, receipt rollback, stress, GC, panic, and CI gates |
| Benchmark coverage | Complete | root scenarios, typed wrapper, envelope coding, and scheduler micro-benchmarks |

The complete build, vet, test, race, lint, crash-recovery, and benchmark gates pass on the Windows
review machine. Linux build, lint, and race execution remains a CI responsibility because no local
Linux runtime is available.

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

### M5: v0.1 release readiness

- [x] Benchmark durable single writes, atomic batches, concurrent streams, delivery fan-out, and
  out-of-order commits.
- [x] Publish configuration and operations guidance.
- [x] Complete API documentation.
- [x] Declare on-disk schema version 1 stable and complete the final local release gate.

Version-control tagging and publishing are repository-owner release actions. The development workflow
does not create a release tag.

### M6: 2026-09-24 code walkthrough

A full walkthrough of `store`, `dispatch`, the facade, `typed`, and `options` found no durability or
correctness defects. It found four hot-path costs, which are fixed with before/after evidence in
[benchmarks.md](benchmarks.md):

- [x] `Counts` counted pending keys with Badger's default value prefetch enabled, loading every
  pending payload while only the oldest value is needed; it now counts keys only.
- [x] `Counts` held the stream lock across its whole read transaction, so `Stats` serialized with
  durable appends; it now snapshots the tail under the lock and scans without it.
- [x] `expireLocked` swept the entire in-flight table on every read, commit, retry, extend, and
  snapshot; it now keeps a lower-bound watermark of the earliest deadline and skips the sweep while
  no lease can be due.
- [x] Each delivered payload was copied three times (envelope decode, scheduler take, facade wrap);
  the decode now aliases the caller-owned buffer and the scheduler hands the queued payload to the
  facade, which remains the single copy boundary toward callers.
- [x] Expand benchmark coverage to payload sizes, batched consuming, retry churn, `Stats` polling,
  the typed wrapper, envelope coding, and scheduler occupancy.

Verification on the walkthrough machine: build, vet, the full non-race suite, and the pinned lint
gate pass. The race gate is environment-blocked there: `go run -race` of a hello-world program
exits with `0xc0000139`, so race verification remains a CI responsibility on this host.

### Walkthrough observations kept as-is

These behaviors were reviewed and deliberately left unchanged:

- `popFreshLocked` moves the fresh slice per pop, which is bounded by `PrefetchCapacity` and costs
  microseconds at the default 1,024; a deque would complicate the scheduler for no measured need.
- `nextWakeLocked` scans the in-flight table, but only on the blocked-reader path, not per
  operation.
- `Log.Stats` reads durable counts and process-local dispatcher state in two snapshots, so the
  combination is advisory rather than mutually atomic.
- `Factory.RunMaintenance` holds the factory lock for the duration of a value-log GC pass,
  deliberately serializing `Open` and `Close` with GC rather than closing Badger under a live GC.
- `Stream.load` writes the schema-version key inside its open transaction, so the first open of a
  log performs one write.
- `AppendBatch` returns records with copied payloads even though the facade uses only their
  sequences; the copy keeps the returned `Record` self-contained for internal callers.

## 5. Progress Rules

- Update this file in the same change that completes or invalidates a milestone item.
- Never mark a design, interface, or test scenario complete before corresponding code and verification
  exist.
- Record environment-blocked checks explicitly rather than treating them as passing.
- Changes to the two reliability invariants require an ADR and explicit approval:
  no acknowledged data loss, and no global consumption blockage from an uncommitted record.
