package teak_test

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cocosip/teak"
	"github.com/dgraph-io/badger/v4"
)

func openTestLog(t *testing.T, options teak.Options, name string) (teak.Factory, teak.Log) {
	t.Helper()
	factory, err := teak.New(options)
	if err != nil {
		t.Fatalf("new factory: %v", err)
	}
	t.Cleanup(func() {
		if err := factory.Close(context.Background()); err != nil {
			t.Errorf("close factory: %v", err)
		}
	})
	log, err := factory.Open(t.Context(), name)
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	return factory, log
}

func TestOutOfOrderCommitSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	factory, log := openTestLog(t, teak.DefaultOptions(dir), "jobs")
	positions, err := log.BatchWrite(t.Context(), [][]byte{[]byte("one"), []byte("two"), []byte("three")})
	if err != nil || len(positions) != 3 {
		t.Fatalf("batch write: %+v, %v", positions, err)
	}
	deliveries, err := log.Read(t.Context(), 3)
	if err != nil || len(deliveries) != 3 {
		t.Fatalf("read: %+v, %v", deliveries, err)
	}
	if err := log.Commit(t.Context(), deliveries[2], deliveries[0]); err != nil {
		t.Fatal(err)
	}
	if err := factory.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	factory, err = teak.New(teak.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := factory.Close(context.Background()); err != nil {
			t.Errorf("close reopened factory: %v", err)
		}
	}()
	log, err = factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	recovered, err := log.Read(t.Context(), 10)
	if err != nil || len(recovered) != 1 || recovered[0].Position.Seq != 2 || string(recovered[0].Payload) != "two" {
		t.Fatalf("recovered deliveries: %+v, %v", recovered, err)
	}
	if err := log.Commit(t.Context(), recovered...); err != nil {
		t.Fatal(err)
	}
	stats, err := log.Stats()
	if err != nil || stats.Pending != 0 || stats.DurableTail != 3 {
		t.Fatalf("stats after recovery: %+v, %v", stats, err)
	}
}

func TestDelayedFailureDoesNotBlockLaterRecord(t *testing.T) {
	options := teak.DefaultOptions(t.TempDir())
	options.DefaultLog.RetryBackoff.Initial = time.Hour
	options.DefaultLog.RetryBackoff.Max = time.Hour
	_, log := openTestLog(t, options, "jobs")
	if _, err := log.BatchWrite(t.Context(), [][]byte{[]byte("poison"), []byte("later")}); err != nil {
		t.Fatal(err)
	}
	first, err := log.Read(t.Context(), 1)
	if err != nil || first[0].Position.Seq != 1 {
		t.Fatalf("first read: %+v, %v", first, err)
	}
	if err := log.Retry(t.Context(), first...); err != nil {
		t.Fatal(err)
	}
	later, err := log.Read(t.Context(), 1)
	if err != nil || later[0].Position.Seq != 2 {
		t.Fatalf("later read: %+v, %v", later, err)
	}
	if err := log.Commit(t.Context(), later...); err != nil {
		t.Fatal(err)
	}
	stats, err := log.Stats()
	if err != nil || stats.Pending != 1 || stats.Retry != 1 || stats.Commits != 1 {
		t.Fatalf("stats: %+v, %v", stats, err)
	}
}

func TestRepeatedPoisonRetriesDoNotStarveFreshWork(t *testing.T) {
	options := teak.DefaultOptions(t.TempDir())
	options.DefaultLog = options.DefaultLog.
		WithPrefetchCapacity(8).
		WithMaxInFlight(4).
		WithRetryBackoff(teak.BackoffConfig{}.
			WithInitial(time.Nanosecond).
			WithMax(time.Nanosecond).
			WithMultiplier(1))
	_, log := openTestLog(t, options, "jobs")
	payloads := make([][]byte, 51)
	payloads[0] = []byte("poison")
	for index := 1; index < len(payloads); index++ {
		payloads[index] = []byte{byte(index)}
	}
	if _, err := log.BatchWrite(t.Context(), payloads); err != nil {
		t.Fatal(err)
	}
	completed := 0
	for iteration := 0; completed < 50 && iteration < 200; iteration++ {
		deliveries, err := log.Read(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if deliveries[0].Position.Seq == 1 {
			if err := log.Retry(t.Context(), deliveries...); err != nil {
				t.Fatal(err)
			}
			continue
		}
		if err := log.Commit(t.Context(), deliveries...); err != nil {
			t.Fatal(err)
		}
		completed++
	}
	if completed != 50 {
		t.Fatalf("completed fresh records = %d, want 50", completed)
	}
	stats, err := log.Stats()
	if err != nil || stats.Pending != 1 || stats.Retry != 1 {
		t.Fatalf("poison retry stats: %+v, %v", stats, err)
	}
}

func TestDeadLetterAndRequeuePreserveOrigin(t *testing.T) {
	_, log := openTestLog(t, teak.DefaultOptions(t.TempDir()), "jobs")
	if _, err := log.Write(t.Context(), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	deliveries, err := log.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.DeadLetter(t.Context(), deliveries[0], "invalid payload"); err != nil {
		t.Fatal(err)
	}
	dead, err := log.DeadLetters(t.Context(), teak.Position{}, 10)
	if err != nil || len(dead) != 1 || dead[0].Reason != "invalid payload" || dead[0].OriginSeq != 1 {
		t.Fatalf("dead letters: %+v, %v", dead, err)
	}
	forged := dead[0]
	forged.Position.Seq++
	if _, err := log.Requeue(t.Context(), forged); !errors.Is(err, teak.ErrInvalidDeadLetter) {
		t.Fatalf("requeue mutated dead letter = %v", err)
	}
	position, err := log.Requeue(t.Context(), dead[0])
	if err != nil || position.Seq != 2 {
		t.Fatalf("requeue: %+v, %v", position, err)
	}
	requeued, err := log.Read(t.Context(), 1)
	if err != nil || requeued[0].Position.Seq != 2 || requeued[0].OriginStream != "jobs" ||
		requeued[0].OriginSeq != 1 || string(requeued[0].Payload) != "payload" {
		t.Fatalf("requeued delivery: %+v, %v", requeued, err)
	}
}

func TestWriteErrorsUsePublicSentinels(t *testing.T) {
	_, log := openTestLog(t, teak.DefaultOptions(t.TempDir()), "jobs")
	payload := make([]byte, (32<<20)+1)
	if _, err := log.Write(t.Context(), payload); !errors.Is(err, teak.ErrBatchTooLarge) {
		t.Fatalf("oversized write = %v", err)
	}
	if _, err := log.BatchWrite(t.Context(), [][]byte{payload}); !errors.Is(err, teak.ErrBatchTooLarge) {
		t.Fatalf("oversized batch write = %v", err)
	}
}

func TestStatsMapsCorruptEnvelopeToPublicError(t *testing.T) {
	dir := t.TempDir()
	factory, err := teak.New(teak.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	log, err := factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := log.Write(t.Context(), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	if err := factory.Close(t.Context()); err != nil {
		t.Fatal(err)
	}

	db, err := badger.Open(badger.DefaultOptions(dir).WithValueDir(dir).WithNumVersionsToKeep(1).WithLogger(nil))
	if err != nil {
		t.Fatal(err)
	}
	key := binary.BigEndian.AppendUint64([]byte("t/jobs/d/"), 1)
	if err := db.Update(func(txn *badger.Txn) error { return txn.Set(key, []byte{0xff}) }); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := teak.New(teak.DefaultOptions(dir))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = reopened.Close(context.Background()) }()
	reopenedLog, err := reopened.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := reopenedLog.Stats(); !errors.Is(err, teak.ErrCorruptStorage) {
		t.Fatalf("stats corruption error = %v", err)
	}
}

func TestDeliveryCannotCommitAnotherRecordOrLog(t *testing.T) {
	factory, first := openTestLog(t, teak.DefaultOptions(t.TempDir()), "first")
	second, err := factory.Open(t.Context(), "second")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Write(t.Context(), []byte("first")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write(t.Context(), []byte("second")); err != nil {
		t.Fatal(err)
	}
	firstDelivery, err := first.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	secondDelivery, err := second.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Commit(t.Context(), secondDelivery...); !errors.Is(err, teak.ErrInvalidDelivery) {
		t.Fatalf("cross-log commit = %v", err)
	}
	forged := firstDelivery[0]
	forged.Position.Seq = 99
	if err := first.Commit(t.Context(), forged); !errors.Is(err, teak.ErrStaleDelivery) {
		t.Fatalf("forged commit = %v", err)
	}
	if err := first.Commit(t.Context(), firstDelivery...); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentProducersAndConsumers(t *testing.T) {
	options := teak.DefaultOptions(t.TempDir())
	options.DefaultLog.PrefetchCapacity = 16
	options.DefaultLog.MaxInFlight = 16
	_, log := openTestLog(t, options, "jobs")

	const producers = 4
	const perProducer = 25
	const total = producers * perProducer
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	errs := make(chan error, producers+4)
	var consumed atomic.Uint64
	var seen sync.Map
	var consumers sync.WaitGroup
	for range 4 {
		consumers.Add(1)
		go func() {
			defer consumers.Done()
			for {
				deliveries, err := log.Read(ctx, 1)
				if errors.Is(err, context.Canceled) {
					return
				}
				if err != nil {
					errs <- err
					return
				}
				if _, loaded := seen.LoadOrStore(deliveries[0].Position.Seq, struct{}{}); loaded {
					errs <- errors.New("duplicate delivery before lease expiry")
					return
				}
				if err := log.Commit(ctx, deliveries...); err != nil {
					errs <- err
					return
				}
				if consumed.Add(1) == total {
					cancel()
					return
				}
			}
		}()
	}

	var writers sync.WaitGroup
	for producer := range producers {
		writers.Add(1)
		go func() {
			defer writers.Done()
			for index := range perProducer {
				if _, err := log.Write(t.Context(), []byte{byte(producer), byte(index)}); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	writers.Wait()
	consumers.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := consumed.Load(); got != total {
		t.Fatalf("consumed = %d, want %d", got, total)
	}
	stats, err := log.Stats()
	if err != nil || stats.Pending != 0 || stats.Writes != total || stats.Commits != total {
		t.Fatalf("final stats: %+v, %v", stats, err)
	}
}

func TestCloseWakesBlockedReaderWithoutDeletingPending(t *testing.T) {
	_, log := openTestLog(t, teak.DefaultOptions(t.TempDir()), "jobs")
	started := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		close(started)
		_, err := log.Read(context.Background(), 1)
		result <- err
	}()
	<-started
	if err := log.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, teak.ErrClosed) {
			t.Fatalf("blocked read = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("blocked read did not exit")
	}
}

func TestClosedLogRemainsClosedForFactoryLifetime(t *testing.T) {
	factory, log := openTestLog(t, teak.DefaultOptions(t.TempDir()), "jobs")
	if err := log.Close(t.Context()); err != nil {
		t.Fatal(err)
	}
	reopened, err := factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if reopened != log {
		t.Fatal("factory returned a second handle for a cached log")
	}
	if _, err := reopened.Write(t.Context(), []byte("payload")); !errors.Is(err, teak.ErrClosed) {
		t.Fatalf("write through reopened closed log = %v", err)
	}
}

func TestOptionsAndNamesAreValidated(t *testing.T) {
	if _, err := teak.New(teak.Options{}); !errors.Is(err, teak.ErrInvalidOptions) {
		t.Fatalf("empty options = %v", err)
	}
	factory, err := teak.New(teak.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factory.Close(context.Background()) }()
	if _, err := factory.Open(t.Context(), "bad/name"); !errors.Is(err, teak.ErrInvalidName) {
		t.Fatalf("invalid name = %v", err)
	}
}

func TestFluentOptionsReturnIndependentCopies(t *testing.T) {
	base := teak.DefaultOptions("base").WithDefaultLog(
		teak.DefaultLogConfig().
			WithPrefetchCapacity(32).
			WithMaxInFlight(16).
			WithVisibilityTimeout(time.Minute).
			WithRetryBackoff(teak.BackoffConfig{}.
				WithInitial(2 * time.Second).
				WithMax(2 * time.Minute).
				WithMultiplier(3)),
	)
	first := base.WithDir("first").WithLog("jobs", teak.LogConfig{}.WithMaxInFlight(4))
	second := base.WithDir("second").WithLog("events", teak.LogConfig{}.WithPrefetchCapacity(8))

	if base.Dir != "base" || len(base.Logs) != 0 {
		t.Fatalf("base options mutated: %+v", base)
	}
	if first.Dir != "first" || first.Logs["jobs"].MaxInFlight != 4 {
		t.Fatalf("first options = %+v", first)
	}
	if second.Dir != "second" || second.Logs["events"].PrefetchCapacity != 8 {
		t.Fatalf("second options = %+v", second)
	}
	if _, exists := first.Logs["events"]; exists {
		t.Fatal("fluent option maps alias each other")
	}
}
