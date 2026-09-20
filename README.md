# Teak

Durable append-only log for Go, built on [BadgerDB](https://github.com/hypermodeinc/badger).
Modeled after [SharpAbp.Abp.Faster](https://github.com/cocosip/sharp-abp/tree/master/framework/src/SharpAbp.Abp.Faster):
multi-producer writes, multi-consumer reads with out-of-order commit, gap self-healing, and
consumption-progress-driven truncation.

**Status: early development** — M1 (badgerstore) in place; core mechanics (M2) next.
See the [roadmap](docs/roadmap.md).

- Module `github.com/cocosip/teak` · Go 1.26 · BadgerDB v4 (`github.com/dgraph-io/badger/v4`,
  repo home [hypermodeinc/badger](https://github.com/hypermodeinc/badger))
- Design & architecture: [docs/design.md](docs/design.md)
- Decision records: [docs/decisions.md](docs/decisions.md)
- Roadmap: [docs/roadmap.md](docs/roadmap.md)
- Testing: [docs/testing.md](docs/testing.md)

## Planned usage (design sketch, not implemented yet)

```go
factory, _ := teak.Open(teak.Options{Dir: "data"})
lg, _ := factory.Open(ctx, "events")

pos, _ := lg.Write(ctx, []byte("hello"))

entries, _ := lg.Read(ctx, 100)
// ... process entries ...
_ = lg.Commit(ctx, positionsOf(entries))
```

License: [MIT](LICENSE).
