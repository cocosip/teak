package teak

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestManualMaintenanceReportsMetrics(t *testing.T) {
	factoryAPI, err := New(DefaultOptions(t.TempDir()).WithMaintenanceInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factoryAPI.Close(context.Background()) }()
	if err := factoryAPI.RunMaintenance(t.Context()); err != nil {
		t.Fatal(err)
	}
	stats := factoryAPI.Stats()
	if stats.ValueLogGCRuns != 1 || stats.ValueLogGCRewrites+stats.ValueLogGCNoRewrite != 1 ||
		stats.ValueLogGCErrors != 0 {
		t.Fatalf("maintenance stats = %+v", stats)
	}
}

func TestMaintenanceErrorIsLoggedAndCounted(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	factoryAPI, err := New(DefaultOptions(t.TempDir()).WithMaintenanceInterval(0).WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factoryAPI.Close(context.Background()) }()
	created := factoryAPI.(*factory)
	maintenanceErr := errors.New("gc failed")
	created.runGC = func(float64) error { return maintenanceErr }
	if err := created.RunMaintenance(t.Context()); !errors.Is(err, maintenanceErr) {
		t.Fatalf("maintenance error = %v", err)
	}
	stats := created.Stats()
	if stats.ValueLogGCRuns != 1 || stats.ValueLogGCErrors != 1 {
		t.Fatalf("maintenance stats = %+v", stats)
	}
	line := output.String()
	if !strings.Contains(line, "value-log-gc") || !strings.Contains(line, "gc failed") {
		t.Fatalf("maintenance log = %s", line)
	}
}

func TestMaintenanceWorkerRecoversPanicWithoutChangingQueue(t *testing.T) {
	var output bytes.Buffer
	logger := slog.New(slog.NewJSONHandler(&output, nil))
	factoryAPI, err := New(DefaultOptions(t.TempDir()).WithMaintenanceInterval(0).WithLogger(logger))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factoryAPI.Close(context.Background()) }()
	log, err := factoryAPI.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Write(t.Context(), []byte("must remain pending")); err != nil {
		t.Fatal(err)
	}

	created := factoryAPI.(*factory)
	created.runGC = func(float64) error { panic("maintenance panic") }
	ticks := make(chan time.Time, 1)
	ticks <- time.Now()
	created.maintenanceWorker(t.Context(), ticks)

	if stats := created.Stats(); stats.MaintenancePanics != 1 {
		t.Fatalf("maintenance stats = %+v", stats)
	}
	queueStats, err := log.Stats()
	if err != nil || queueStats.Pending != 1 {
		t.Fatalf("queue changed after maintenance panic: %+v, %v", queueStats, err)
	}
	line := output.String()
	if !strings.Contains(line, "maintenance panic") || !strings.Contains(line, "stack") ||
		strings.Contains(line, "must remain pending") {
		t.Fatalf("panic log = %s", line)
	}
}
