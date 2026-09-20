# Teak — Durable Log Module on BadgerDB: Architecture Design

> Modeled after [SharpAbp.Abp.Faster](https://github.com/cocosip/sharp-abp/tree/master/framework/src/SharpAbp.Abp.Faster)
> (C#, built on Microsoft FASTER Log), implemented in Go + BadgerDB:
> a durable append-only log with multi-producer writes, multi-consumer reads,
> and consumption-progress-driven truncation.

---

## 0. Technology Baseline

| Item | Choice |
|---|---|
| Language | Go 1.26 (`go.mod` declares `go 1.26`) |
| Storage engine | `github.com/dgraph-io/badger/v4` (repo home: hypermodeinc/badger, v4.9.x — module path unchanged) |
| Module path | `github.com/cocosip/teak` (ADR-0007) |
| Documentation language | English |

Relevant Go 1.26 notes: `testing/synctest` (stable since Go 1.25) is the standard tool for testing
the interval-driven background tasks (Commit/Complete/Truncate) without real sleeps; the
experimental goroutine-leak detector (Go 1.26 runtime) can guard the long-running task goroutines
in CI.

## 1. Reference Module Analysis (SharpAbp.Abp.Faster)

### 1.1 It wraps FASTER Log, not FASTER KV

The core abstraction is a **persistent append-only log**, `IFasterLogger<T>` — not a KV store. Semantics:

- **Write**: `WriteAsync` / `BatchWriteAsync` → FASTER `EnqueueAsync` (enqueue makes data visible;
  durability comes from a background Commit task every `CommitIntervalMillis=2000ms`)
- **Read**: a background Scan task continuously moves iterator data into a **bounded channel**
  (capacity `PreReadCapacity=5000`; Wait mode when full → backpressure); `ReadAsync(count)` takes a
  batch from the channel
- **Consumer ack**: after processing a batch, the consumer calls `CommitAsync(positions)`
- **Truncation**: completed address ranges are merged to advance `TruncateBeforeAddress`;
  a periodic task calls `TruncateUntilPageStart` to reclaim disk
- **Gap self-healing**: out-of-order commits from multiple consumers create holes;
  within `AddressMatchTolerance=10` counts as continuous; gaps older than
  `ForceCompleteGapTimeoutMillis=120s` are auto-skipped (data loss — consumers must be idempotent);
  manual `ForceCommitGap` also available
- **Export**: `ExportAsync` scans an arbitrary address range to files (does not affect consumer position)

### 1.2 Components

| Component | Responsibility |
|---|---|
| `IFasterLogger<T>` | single log stream: Initialize / Write / BatchWrite / Read / Commit / ForceCommitGap / Export |
| `IFasterLoggerFactory` | creates/reuses loggers by name, shared RootPath |
| `AbpFasterOptions` | global config (RootPath + named configuration table) |
| `AbpFasterConfiguration` | per-stream config (file, memory params, task intervals, gap policy) |
| `Position` | logical range `[Address, NextAddress)` occupied by one write |
| internal `CompletedRange` | sorted set; merge contiguous ranges to advance the truncate watermark |

### 1.3 Four background tasks

1. **Commit** (2s): periodic `Log.CommitAsync()` for durability
2. **Scan** (continuous): iterator → bounded channel, with backpressure
3. **Complete** (3s): merge completed ranges, advance the truncate watermark
4. **Truncate** (5min): reclaim disk space up to the watermark

Threading: multi-threaded concurrent Enqueue on the write side (internal semaphore); mutex around
the range set; all metrics via Interlocked; channel is single-writer / multi-reader.

---

## 2. BadgerDB Capability Assessment

### 2.1 Concurrent writes (the key question)

**Verdict: concurrent writes from multiple goroutines are supported — safe to use as-is.**
From v4 source (`txn.go`) doc comments:

> "Badger supports concurrent execution of transactions, providing serializable
> snapshot isolation... Running transactions concurrently is OK. However, a
> transaction itself isn't thread safe, and should only be run serially."

Detailed rules:

| Scenario | Verdict |
|---|---|
| Multiple goroutines each holding their own write txn, writing concurrently | ✅ Supported; SSI guarantees, Jepsen-style bank test with `--race` nightly |
| One `Txn` object used concurrently by multiple goroutines (`Set` etc.) | ❌ Txn is not thread-safe; must be used serially per txn |
| **Blind writes** (write-only, no reads) | ✅ **Never conflict** (`NewWriteBatch` docs: "blind writes can never encounter transaction conflicts (ErrConflict)") |
| Read-then-write where a read key was modified by another txn | `ErrConflict` at commit (optimistic conflict detection) |
| Write-write on the same key (both blind) | No conflict; MVCC versions coexist, readers see the latest |
| Oversized `WriteBatch` | Internally **split into multiple transactions — the batch is not atomic as a whole**; `Cancel` does not roll back already-committed chunks |
| Multiple processes opening the same directory | ❌ Directory `LOCK` file — single process per DB dir (unrestricted concurrency within the process) |
| Durability | `SyncWrites` defaults to true; every commit fsyncs — committed means durable |

Implication for the log use case: **append = blind write = zero conflicts**, so concurrent write
safety is inherent. Each write txn has entry-count/size limits (~15% of memtable size; exceeding
returns `ErrTxnTooBig`), so large batches must be split by the caller.

### 2.2 Other relevant capabilities

- **Dependency status**: the `dgraph-io/badger` GitHub repo is archived, but active development
  continues at **`hypermodeinc/badger`** (v4.9.6, 2025-08) and the **Go module path stays
  `github.com/dgraph-io/badger/v4`** (go.mod unchanged — that is the canonical import).
  Pure Go, no cgo, Windows supported
- **Ordered iteration**: keys sort lexicographically; `uint64` big-endian encoding gives numeric
  order → natural fit for seq-based scans
- **`GetSequence`**: built-in monotonic integer allocation (lease/bandwidth scheme to reduce contention)
- **`Subscribe`**: watch key changes by prefix — usable as a low-latency "wake the scanner" hint
  instead of polling
- **`DropPrefix`**: efficient whole-prefix delete (needs write/compaction pause — heavyweight);
  **range delete** requires iterate + batched deletes
- **`NewStream`**: parallel iteration framework, useful for large-range scans/exports
- **Key size limit 1024 bytes**; large values go to the value log automatically (WiscKey design)
- **Backup/Restore**, TTL, in-memory mode all available

### 2.3 Semantic deltas vs FASTER Log (design implications)

| FASTER Log semantics | Badger equivalent | Handling |
|---|---|---|
| address = byte offset in file | logical `seq uint64` (self-managed allocation) | Position becomes `[Seq, NextSeq)`, Length = entry count |
| Enqueue is memory-visible only; periodic Commit persists | txn commit is durable immediately (SyncWrites) | two write modes: `immediate` (commit per write) / `buffered` (in-memory buffer + background flush; FASTER-like high throughput; recent window lost on crash) |
| iterator checkpoint persisted & recovered | consumer watermark stored as a meta key | restore watermark on restart |
| `TruncateUntilPageStart` | range delete of keys with `seq < N` | low-frequency background task + incremental batching |

---

## 3. Architecture

### 3.1 Layering

```
┌────────────────────────────────────────────────┐
│  teak (application-facing)                     │
│      TypedLogger[T]  ← thin generic wrapper    │
├────────────────────────────────────────────────┤
│  core layer (storage-agnostic mechanisms)      │
│   Logger iface (byte-level) │ Factory │ Options│
│   RangeMerger (gap/merge) │ Scanner (chan/BP)  │
│   Truncater │ background task wiring │ Metrics │
├────────────────────────────────────────────────┤
│  store layer (swappable storage adapter)       │
│   Store iface: Put / Scan / DeleteBelow / Meta │
│      └─ badgerstore (hypermodeinc/badger/v4)   │
└────────────────────────────────────────────────┘
```

Design principles:

- **core deals in raw `[]byte`**; encoding/decoding lives in the upper generic wrapper — this
  works around Go's restriction on generic methods in interfaces (the C# `IFasterLogger<T>`
  generic factory becomes: a byte-level Logger factory + a `NewTypedLogger[T]` wrapper function)
- **Store abstraction isolates Badger**: can later swap in pebble / a custom file-append layer;
  core mechanisms (gap, backpressure, truncation, recovery) stay storage-agnostic
- **One DB, many log streams**: the Factory holds a shared Badger instance; each named stream is
  isolated by key prefix (`t/<logName>/d/<seq>` data, `t/<logName>/m/<k>` metadata) — avoids the
  resource cost of many directories / many in-process DB instances

### 3.2 Package layout

```
teak/
├── go.mod                          # module github.com/cocosip/teak
├── core/
│   ├── logger.go                   # Logger interface + Position/Entry/Stats
│   ├── factory.go                  # Factory interface + default impl
│   ├── options.go                  # Options / LogConfig
│   ├── ranges.go                   # CompletedRange sorted set & merging
│   ├── scanner.go                  # background scan → bounded channel
│   ├── tasks.go                    # Commit/Complete/Truncate task wiring
│   └── metrics.go                  # counters
├── badgerstore/
│   ├── store.go                    # Store interface, Badger implementation
│   ├── keys.go                     # key encoding (prefix + big-endian seq)
│   └── seq.go                      # seq allocation (reserved watermark persistence)
├── typed/
│   └── typed.go                    # TypedLogger[T] + Codec iface (json/msgbin defaults)
└── examples/
    └── quickstart/main.go
```

### 3.3 Core interfaces (Go)

```go
// ---------- core/logger.go ----------

// Position is the logical range [Seq, NextSeq) occupied by one log entry.
// The consumer submits it back via Commit after processing, driving the
// truncate watermark forward.
type Position struct {
    Seq     uint64 // this entry's sequence (assigned at write time)
    NextSeq uint64 // sequence immediately after this entry
}

func (p Position) Count() uint64 { return p.NextSeq - p.Seq }

// Entry is one log record read back (byte-level at the core layer).
type Entry struct {
    Position Position
    Data     []byte
}

// Stats runtime metrics (mirrors the C# IFasterLogger statistics properties).
type Stats struct {
    Initialized        bool
    BeginSeq           uint64 // oldest un-truncated sequence
    CommittedUntilSeq  uint64 // highest durably written sequence
    TruncateBeforeSeq  uint64 // truncate watermark
    TotalWrites        uint64
    TotalReads         uint64
    TotalCommittedPos  uint64
    CurrentGapCount    int
    LargestGapSize     uint64
    CompletedRangeCnt  int
}

// Logger is one named log stream (byte-level, concurrency-safe).
type Logger interface {
    Name() string
    Initialize(ctx context.Context) error

    // Concurrency-safe: multiple goroutines may write simultaneously
    // (blind writes — no conflicts). In immediate mode, return means durable;
    // in buffered mode, it waits for the background flush window.
    Write(ctx context.Context, data []byte) (Position, error)
    BatchWrite(ctx context.Context, data [][]byte) ([]Position, error)

    // Take at most count entries from the prefetch channel; blocks until at
    // least 1 is available or ctx is cancelled / the logger closes.
    Read(ctx context.Context, count int) ([]Entry, error)

    // Consumer acks processed ranges (out-of-order commits allowed).
    Commit(ctx context.Context, positions ...Position) error

    // Manually force-skip a persistent gap (data loss; consumers must be idempotent).
    ForceCommitGap(ctx context.Context, start, end uint64) error

    Stats() Stats
    Close(ctx context.Context) error
}

// ---------- core/factory.go ----------

type Factory interface {
    // Open (or reuse) a named log stream; the underlying storage is shared.
    Open(ctx context.Context, name string) (Logger, error)
    Close(ctx context.Context) error
}

// ---------- typed/typed.go ----------

// Codec handles business-object encoding (mirrors C# IObjectSerializer).
type Codec[T any] interface {
    Encode(T) ([]byte, error)
    Decode([]byte) (T, error)
}

// TypedLogger[T] layers codecs on top of the byte-level Logger,
// mirroring IFasterLogger<T>.
type TypedLogger[T any] struct { ... }

func NewTyped[T any](lg core.Logger, codec Codec[T]) *TypedLogger[T]

func (l *TypedLogger[T]) Write(ctx context.Context, v T) (core.Position, error)
func (l *TypedLogger[T]) BatchWrite(ctx context.Context, vs []T) ([]core.Position, error)
func (l *TypedLogger[T]) Read(ctx context.Context, n int) ([]TypedEntry[T], error)
// Commit / ForceCommitGap / Stats / Close pass through
```

### 3.4 Key mechanisms

**Sequence allocation (badgerstore/seq.go)**

- In-process `atomic.Uint64` + startup recovery; reserve in batches (persist the watermark to
  `m/seq-lease` every 1024 seqs, say); a crash wastes at most one reserved window → creates holes,
  which log semantics tolerate by design (the gap mechanism exists for exactly this).
  We deliberately avoid `db.GetSequence` to keep the Store abstraction free of Badger leakage.

**Write path**

```
immediate: Write → (seq alloc) → dedicated write txn Set(key(seq), data) → Commit(fsync) → return Position
buffered : Write → (seq alloc) → in-memory channel → background aggregator flushes via
           WriteBatch periodically (high throughput; recent window may be lost on crash)
```

- `BatchWrite` commits the whole batch in one txn (auto-split when hitting maxBatchSize —
  beyond the split point atomicity is not guaranteed; must be documented)
- Key encoding: `t/<log>/d/<8-byte big-endian seq>` — lexicographic order = time order

**Read path (scanner.go)**

- A single goroutine scans from `BeginSeq` using a prefix iterator into a **bounded channel**
  (capacity = `ScanBufferSize`; waits when full → backpressure), same as the C# design
- Close semantics: `Close()` closes the channel; `Read` returns `ErrClosed`
- Crash-recovery semantics (iterator hits a corrupt page): skip to the last durably committed
  position with a warning (mirrors the C# unclean-shutdown recovery)

**Range merging (ranges.go)**

- `Commit(positions)` validates (drop stale / straddling entries) → sorted set
- Complete task (every `CompleteInterval`): merge from the current watermark; gaps within
  `SeqMatchTolerance` count as continuous; on a real gap record first-seen time, warn
  periodically after `GapTimeout`, auto-skip after `ForceCompleteGapAfter`; also force-skip
  when `MaxCompletedRanges` is exceeded
- Before advancing the watermark, persist the new watermark (meta key `m/truncate-before`) so a
  restart never regresses

**Truncation (tasks.go)**

- Truncate task (every `TruncateInterval`): once the watermark advances, delete keys with
  `seq < watermark` (iterate + WriteBatch deletes, incremental batching, non-blocking); optionally
  call `RunValueLogGC` to reclaim space

**Store interface (badgerstore)**

```go
// Root owns the shared Badger instance; Stream is one namespace and
// implements Store. Root.Stream returns the same *Stream per name, so
// sequence allocation stays unique per namespace.
type Store interface {
    // Allocate reserves n contiguous sequence numbers (starting at 1);
    // leases are persisted in windows, so a crash may leave holes but
    // never reuses a sequence.
    Allocate(n uint64) (uint64, error)
    Put(seq uint64, data []byte) error
    BatchPut(seqs []uint64, datas [][]byte) error
    ScanFrom(seq uint64, fn func(seq uint64, data []byte) error) error
    DeleteBelow(seq uint64, batchLimit int) (int, error)
    PutMeta(key string, value []byte) error
    GetMeta(key string) ([]byte, error)
    Sync() error
}
// Lifecycle (Close) belongs to Root, not to individual streams.
```

### 3.5 Configuration

```go
// Global options (mirrors AbpFasterOptions).
type Options struct {
    Dir        string          // data root (mirrors RootPath), default "data"
    BadgerOpts badger.Options  // pass-through tuning (memtable size, compression, log level…)
    Logs       map[string]*LogConfig
}

// Per-stream config (mirrors AbpFasterConfiguration, minus FASTER-specific
// memory/IO bit-width params).
type LogConfig struct {
    WriteMode             WriteMode // Immediate (default) / Buffered
    FlushInterval         int64     // buffered flush interval ms, default 2000 (mirrors CommitInterval)
    ScanBufferSize        int       // prefetch channel capacity, default 5000 (mirrors PreReadCapacity)
    CompleteInterval      int64     // merge task interval ms, default 3000
    TruncateInterval      int64     // truncate task interval ms, default 300000
    SeqMatchTolerance     uint64    // range-merge tolerance (entries), default 10
    GapTimeout            int64     // gap warning threshold ms, default 600000; 0 disables
    ForceCompleteGapAfter int64     // gap force-skip threshold ms, default 120000; 0 disables
    MaxCompletedRanges    int       // tracked-range cap, default 10000; 0 = unlimited
}
```

---

## 4. Risks & Trade-offs

| Risk | Assessment | Mitigation |
|---|---|---|
| Write amplification / throughput below FASTER Log | each Badger entry carries key + version metadata; LSM write amplification exceeds pure append | buffered write mode aggregates; keep keys ≤ 20 bytes; `NumVersionsToKeep=1` |
| Per-txn entry count/size limits | exceeding returns `ErrTxnTooBig` | BatchWrite auto-splits; document that split boundaries are not atomic |
| Range-delete cost | DropPrefix doesn't do ranges; iterate-delete has overhead | low-frequency background task + incremental batching + rate limiting |
| Crash wastes reserved sequence numbers | creates holes | log semantics tolerate holes by design; gap mechanism covers it |
| dgraph-io archived / moved | old module path is frozen | standardize on `github.com/hypermodeinc/badger/v4` |
| Windows platform | officially supported; locking/fsync behavior differs slightly | dev environment is Windows; covered by tests |

## 5. Decisions

All open questions are resolved in [decisions.md](decisions.md); confirmed 2026-09-20:
single consumption position (ADR-0009), `Immediate` as the default write mode (ADR-0004),
one shared DB with key-prefix isolation (ADR-0003), MIT license (ADR-0008), and module path
`github.com/cocosip/teak` (ADR-0007).
