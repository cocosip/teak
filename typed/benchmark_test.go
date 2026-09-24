package typed_test

import (
	"context"
	"testing"

	"github.com/cocosip/teak"
	teaktyped "github.com/cocosip/teak/typed"
)

type benchmarkJob struct {
	ID    uint64 `json:"id"`
	Kind  string `json:"kind"`
	Stage int    `json:"stage"`
}

func openTypedBenchmarkLog(b *testing.B) *teaktyped.Log[benchmarkJob] {
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
	raw, err := factory.Open(context.Background(), "jobs")
	if err != nil {
		b.Fatal(err)
	}
	log, err := teaktyped.JSON[benchmarkJob](raw)
	if err != nil {
		b.Fatal(err)
	}
	return log
}

func BenchmarkTypedJSONWrite(b *testing.B) {
	log := openTypedBenchmarkLog(b)
	job := benchmarkJob{ID: 42, Kind: "index", Stage: 3}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := log.Write(context.Background(), job); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkTypedJSONRoundTrip(b *testing.B) {
	log := openTypedBenchmarkLog(b)
	job := benchmarkJob{ID: 42, Kind: "index", Stage: 3}
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := log.Write(ctx, job); err != nil {
			b.Fatal(err)
		}
		deliveries, err := log.Read(ctx, 1)
		if err != nil {
			b.Fatal(err)
		}
		if err := log.Commit(ctx, deliveries...); err != nil {
			b.Fatal(err)
		}
	}
}
