package teak_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/cocosip/teak"
)

func BenchmarkDurableWrite(b *testing.B) {
	_, log := openBenchmarkLog(b, "jobs")
	payload := []byte("benchmark-payload")
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := log.Write(context.Background(), payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAtomicBatchWrite100(b *testing.B) {
	_, log := openBenchmarkLog(b, "jobs")
	payloads := make([][]byte, 100)
	for index := range payloads {
		payloads[index] = []byte("benchmark-payload")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := log.BatchWrite(context.Background(), payloads); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConcurrentStreams(b *testing.B) {
	factory, _ := openBenchmarkLog(b, "bootstrap")
	logs := make([]teak.Log, 8)
	for index := range logs {
		log, err := factory.Open(context.Background(), fmt.Sprintf("stream-%d", index))
		if err != nil {
			b.Fatal(err)
		}
		logs[index] = log
	}
	var next atomic.Uint64
	var firstErr error
	var errOnce sync.Once
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			log := logs[next.Add(1)%uint64(len(logs))]
			if _, err := log.Write(context.Background(), []byte("benchmark-payload")); err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
		}
	})
	if firstErr != nil {
		b.Fatal(firstErr)
	}
}

func BenchmarkDeliveryFanOut(b *testing.B) {
	_, log := openBenchmarkLog(b, "jobs")
	remaining := b.N
	for remaining > 0 {
		count := min(remaining, 1000)
		payloads := make([][]byte, count)
		for index := range payloads {
			payloads[index] = []byte("benchmark-payload")
		}
		if _, err := log.BatchWrite(context.Background(), payloads); err != nil {
			b.Fatal(err)
		}
		remaining -= count
	}
	var firstErr error
	var errOnce sync.Once
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(parallel *testing.PB) {
		for parallel.Next() {
			deliveries, err := log.Read(context.Background(), 1)
			if err == nil {
				err = log.Commit(context.Background(), deliveries...)
			}
			if err != nil {
				errOnce.Do(func() { firstErr = err })
				return
			}
		}
	})
	if firstErr != nil {
		b.Fatal(firstErr)
	}
}

func BenchmarkOutOfOrderCommit32(b *testing.B) {
	_, log := openBenchmarkLog(b, "jobs")
	payloads := make([][]byte, 32)
	for index := range payloads {
		payloads[index] = []byte("benchmark-payload")
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := log.BatchWrite(context.Background(), payloads); err != nil {
			b.Fatal(err)
		}
		deliveries, err := log.Read(context.Background(), len(payloads))
		if err != nil {
			b.Fatal(err)
		}
		for left, right := 0, len(deliveries)-1; left < right; left, right = left+1, right-1 {
			deliveries[left], deliveries[right] = deliveries[right], deliveries[left]
		}
		if err := log.Commit(context.Background(), deliveries...); err != nil {
			b.Fatal(err)
		}
	}
}

func openBenchmarkLog(b *testing.B, name string) (teak.Factory, teak.Log) {
	b.Helper()
	options := teak.DefaultOptions(b.TempDir()).WithMaintenanceInterval(0)
	factory, err := teak.New(options)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := factory.Close(context.Background()); err != nil {
			b.Errorf("close factory: %v", err)
		}
	})
	log, err := factory.Open(context.Background(), name)
	if err != nil {
		b.Fatal(err)
	}
	return factory, log
}
