# Benchmark Baseline

Benchmarks cover user-visible queue scenarios, the typed wrapper, and two internal hot paths:

- root `benchmark_test.go`: durable single writes at several payload sizes, 100-record atomic
  batches, concurrent writes across eight streams, parallel read plus commit fan-out, 32-record
  reverse-order commits, batched consuming, retry redelivery churn, and `Stats` polling over a
  10,000-record backlog;
- `typed/benchmark_test.go`: JSON encoding on write and a full write, read, and commit round trip;
- `internal/store/envelope_bench_test.go`: envelope encoding and decoding without storage I/O;
- `internal/dispatch/benchmark_test.go`: scheduler batch cycles and per-commit cost at different
  in-flight occupancy, using a synthetic source and a fixed clock.

Run them with:

```text
go test -count=1 -run "^$" -bench "." -benchmem -benchtime=10x ./...
```

## 2026-09-24 Short Baseline

Environment: Windows amd64, Intel Core i5-9400, Go 1.27.1, Badger v4.9.6. The run used
`-benchtime=10x`; it is a smoke baseline, not a capacity result or durability SLA. `SyncWrites=true`
is enabled for every write.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| DurableWrite | 21,300 | 2,037 | 53 |
| DurableWriteSizes/Payload=64B | 15,100 | 2,076 | 52 |
| DurableWriteSizes/Payload=4096B | 18,080 | 11,343 | 53 |
| DurableWriteSizes/Payload=65536B | 112,670 | 148,504 | 53 |
| AtomicBatchWrite100 | 171,200 | 66,350 | 1,658 |
| ConcurrentStreams | 6,580 | 2,195 | 57 |
| DeliveryFanOut | 25,910 | 11,831 | 60 |
| OutOfOrderCommit32 | 197,180 | 179,725 | 1,576 |
| ConsumeBatch100 | 7,530 | 10,747 | 36 |
| RetryCycle | 469,080 | 9,810 | 17 |
| Stats | 1,073,910 | 1,451 | 29 |
| TypedJSONWrite | 17,710 | 2,348 | 57 |
| TypedJSONRoundTrip | 60,730 | 90,908 | 140 |
| PendingEnvelopeEncode | 850 | 1,152 | 1 |
| PendingEnvelopeDecode | 110 | 8 | 1 |
| DeadEnvelopeRoundTrip | 380 | 1,184 | 3 |
| SchedulerBatch100 | 85,900 | 58,556 | 112 |
| SchedulerCommitWithInFlight/InFlight=16 | 870 | 136 | 2 |
| SchedulerCommitWithInFlight/InFlight=1024 | 19,090 | 28,836 | 105 |

Reading notes:

- `RetryCycle` configures a one-nanosecond retry delay and no storage writes. On Windows the host
  clock granularity dominates the cycle, so treat it as a redelivery-churn smoke number; on hosts
  with finer clock observation it approaches the pure scheduler cost.
- `Stats` iterates the 10,000 pending keys in the benchmark backlog. Its cost grows with backlog
  size; it counts keys and fetches only the oldest pending value.
- `SchedulerCommitWithInFlight` pins the clock, so nothing expires; the growth from 16 to 1,024
  in-flight deliveries is the per-operation scheduler work that scales with occupancy (receipt
  generation plus refill of the drained fresh lane).

## 2026-09-24 Hot-Path Fixes

A code walkthrough found four hot-path costs. Each fix keeps semantics unchanged and is covered by
the existing test suite. Back-to-back `-benchtime=20x` comparisons on the same host:

| Path | Before | After | Fix |
|---|---|---|---|
| `Stats` over a 10,000-record backlog | 5,948,465 ns, 194,004 B, 10,528 allocs | 1,025,270 ns, 1,445 B, 29 allocs | `Counts` counted keys with Badger's default value prefetch enabled, loading every pending payload; it now counts keys only and fetches the oldest value on demand |
| Scheduler commit with 1,024 in-flight | 44,665 ns | 9,385 ns | `expireLocked` swept the whole in-flight table on every operation; it now keeps a lower-bound watermark of the earliest deadline and skips the sweep while nothing can be due |
| Scheduler commit with 16 in-flight | 1,350 ns | 625 ns | Same expiry watermark |
| Pending envelope decode, 1 KiB payload | 465 ns, 1,032 B, 2 allocs | 70 ns, 8 B, 1 alloc | Decoded payloads now alias the caller-owned input buffer instead of copying every record |
| Read plus commit fan-out | 26,970 ns, 7,744 B | 17,640 ns, 7,084 B | Combined effect of the expiry watermark and removing one of three payload copies per delivery |

The remaining `Stats` cost is one LSM iteration over the pending and dead-letter key prefixes;
there is no per-record value access anymore.

## 2026-09-20 Short Baseline

Environment: Windows amd64, Intel Core Ultra 9 185H, Go 1.27.1, Badger v4.9.6. The run used
`-benchtime=10x`; it is a smoke baseline, not a capacity result or durability SLA.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| DurableWrite | 16,180 | 1,988 | 52 |
| AtomicBatchWrite100 | 91,330 | 66,257 | 1,658 |
| ConcurrentStreams | 10,960 | 3,182 | 61 |
| DeliveryFanOut | 25,900 | 86,352 | 87 |
| OutOfOrderCommit32 | 127,330 | 179,442 | 1,636 |

Compare changes on the same host, filesystem, power policy, Go version, Badger version, payload size,
and `-benchtime`. Filesystem cache and device flush behavior materially affect these numbers.
