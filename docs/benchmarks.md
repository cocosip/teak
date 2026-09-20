# Benchmark Baseline

Benchmarks live in `benchmark_test.go` and cover the v0.1 release scenarios:

- synchronous durable single writes;
- 100-record atomic batches;
- concurrent writes across eight streams;
- parallel read plus commit fan-out;
- 32-record reverse-order commit batches.

Run them with:

```text
go test -run "^$" -bench "." -benchmem .
```

## 2026-09-20 Short Baseline

Environment: Windows amd64, Intel Core Ultra 9 185H, Go 1.27.1, Badger v4.9.6. The run used
`-benchtime=10x`; it is a smoke baseline, not a capacity result or durability SLA.

| Benchmark | ns/op | B/op | allocs/op |
|---|---:|---:|---:|
| DurableWrite | 15,110 | 2,048 | 52 |
| AtomicBatchWrite100 | 104,440 | 66,224 | 1,657 |
| ConcurrentStreams | 9,110 | 3,273 | 61 |
| DeliveryFanOut | 32,490 | 86,060 | 86 |
| OutOfOrderCommit32 | 155,640 | 175,171 | 1,631 |

Compare changes on the same host, filesystem, power policy, Go version, Badger version, payload size,
and `-benchtime`. Filesystem cache and device flush behavior materially affect these numbers.
