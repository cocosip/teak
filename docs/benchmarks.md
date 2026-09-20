# Benchmark Baseline

Benchmarks live in `benchmark_test.go` and cover the v0.1 release scenarios:

- synchronous durable single writes;
- 100-record atomic batches;
- concurrent writes across eight streams;
- parallel read plus commit fan-out;
- 32-record reverse-order commit batches.

Run them with:

```text
go test -count=1 -run "^$" -bench "." -benchmem -benchtime=10x .
```

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
