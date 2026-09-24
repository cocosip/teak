package dispatch

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cocosip/teak/internal/store"
)

var benchmarkPayload = []byte("benchmark-payload")

// sequenceSource synthesizes an unbounded monotonic record sequence, so
// scheduler benchmarks never touch storage or rescan delivered sequences.
type sequenceSource struct {
	next uint64
}

func (s *sequenceSource) ScanBatch(from uint64, limit int) ([]store.Record, error) {
	records := make([]store.Record, 0, limit)
	start := from
	if start <= s.next {
		start = s.next + 1
	}
	for seq := start; len(records) < limit; seq++ {
		records = append(records, store.Record{Seq: seq, CreatedAt: time.Now(), Payload: benchmarkPayload})
		s.next = seq
	}
	return records, nil
}

func benchmarkDispatcher(b *testing.B) *Dispatcher {
	b.Helper()
	dispatcher, err := New(&sequenceSource{}, Config{
		PrefetchCapacity:  1024,
		MaxInFlight:       2048,
		VisibilityTimeout: time.Hour,
		RetryInitial:      time.Second,
		RetryMax:          time.Minute,
		RetryMultiplier:   2,
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(dispatcher.Close)
	// A fixed clock keeps leases from expiring, so each iteration measures the
	// per-operation scheduler work, including the in-flight expiry sweep.
	fixed := time.Unix(1_800_000_000, 0)
	dispatcher.now = func() time.Time { return fixed }
	return dispatcher
}

func BenchmarkSchedulerBatch100(b *testing.B) {
	dispatcher := benchmarkDispatcher(b)
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		deliveries, err := dispatcher.Read(ctx, 100)
		if err != nil {
			b.Fatal(err)
		}
		if err := dispatcher.Commit(deliveries, func([]uint64) error { return nil }); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchedulerCommitWithInFlight(b *testing.B) {
	for _, occupancy := range []int{16, 1024} {
		b.Run(fmt.Sprintf("InFlight=%d", occupancy), func(b *testing.B) {
			dispatcher := benchmarkDispatcher(b)
			ctx := context.Background()
			if _, err := dispatcher.Read(ctx, occupancy); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				deliveries, err := dispatcher.Read(ctx, 1)
				if err != nil {
					b.Fatal(err)
				}
				if err := dispatcher.Commit(deliveries, func([]uint64) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
