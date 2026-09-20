package dispatch

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/cocosip/teak/internal/store"
)

type fakeSource struct {
	mu      sync.Mutex
	records []store.Record
	err     error
}

func (s *fakeSource) ScanBatch(from uint64, limit int) ([]store.Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.err != nil {
		return nil, s.err
	}
	result := make([]store.Record, 0, limit)
	for _, record := range s.records {
		if record.Seq >= from {
			result = append(result, record)
		}
		if len(result) == limit {
			break
		}
	}
	return result, nil
}

func (s *fakeSource) add(seq uint64) {
	s.mu.Lock()
	s.records = append(s.records, store.Record{Seq: seq, CreatedAt: time.Now(), Payload: []byte{byte(seq)}})
	s.mu.Unlock()
}

func testConfig() Config {
	return Config{
		PrefetchCapacity:  2,
		MaxInFlight:       2,
		VisibilityTimeout: 10 * time.Second,
		RetryInitial:      time.Second,
		RetryMax:          time.Minute,
		RetryMultiplier:   2,
	}
}

func newTestDispatcher(t *testing.T, source Source, config Config) *Dispatcher {
	t.Helper()
	dispatcher, err := New(source, config)
	if err != nil {
		t.Fatalf("new dispatcher: %v", err)
	}
	t.Cleanup(dispatcher.Close)
	return dispatcher
}

func TestReadCommitAndOpaqueReceiptValidation(t *testing.T) {
	source := &fakeSource{}
	source.add(1)
	source.add(2)
	dispatcher := newTestDispatcher(t, source, testConfig())

	deliveries, err := dispatcher.Read(t.Context(), 2)
	if err != nil || len(deliveries) != 2 {
		t.Fatalf("read: len=%d err=%v", len(deliveries), err)
	}
	forged := deliveries[0]
	forged.Receipt[0] ^= 0xff
	called := false
	if err := dispatcher.Commit([]Delivery{forged}, func([]uint64) error {
		called = true
		return nil
	}); !errors.Is(err, ErrStaleDelivery) || called {
		t.Fatalf("forged commit: err=%v called=%v", err, called)
	}
	var committed []uint64
	if err := dispatcher.Commit(deliveries, func(seqs []uint64) error {
		committed = append(committed, seqs...)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(committed) != 2 || committed[0] != 1 || committed[1] != 2 {
		t.Fatalf("committed = %v", committed)
	}
	if err := dispatcher.Commit(deliveries, func([]uint64) error { return nil }); !errors.Is(err, ErrStaleDelivery) {
		t.Fatalf("duplicate commit = %v", err)
	}
}

func TestPersistFailureKeepsDeliveryInFlight(t *testing.T) {
	source := &fakeSource{}
	source.add(1)
	dispatcher := newTestDispatcher(t, source, testConfig())
	deliveries, err := dispatcher.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	persistErr := errors.New("storage failed")
	if err := dispatcher.Commit(deliveries, func([]uint64) error { return persistErr }); !errors.Is(err, persistErr) {
		t.Fatalf("commit error = %v", err)
	}
	if got := dispatcher.Snapshot().InFlight; got != 1 {
		t.Fatalf("in flight after failure = %d", got)
	}
	if err := dispatcher.Commit(deliveries, func([]uint64) error { return nil }); err != nil {
		t.Fatalf("commit retry: %v", err)
	}
}

func TestDeadLetterFailureKeepsDeliveryInFlight(t *testing.T) {
	source := &fakeSource{}
	source.add(1)
	dispatcher := newTestDispatcher(t, source, testConfig())
	deliveries, err := dispatcher.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	persistErr := errors.New("storage failed")
	if err := dispatcher.DeadLetter(deliveries[0], func(uint64) error { return persistErr }); !errors.Is(err, persistErr) {
		t.Fatalf("dead-letter error = %v", err)
	}
	if got := dispatcher.Snapshot().InFlight; got != 1 {
		t.Fatalf("in flight after failure = %d", got)
	}
	if err := dispatcher.DeadLetter(deliveries[0], func(seq uint64) error {
		if seq != 1 {
			t.Fatalf("dead-letter sequence = %d", seq)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFreshAndRetryLanesAlternate(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &fakeSource{}
		for seq := uint64(1); seq <= 3; seq++ {
			source.add(seq)
		}
		dispatcher := newTestDispatcher(t, source, testConfig())
		first, err := dispatcher.Read(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := dispatcher.Retry(first); err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Second)

		fresh, err := dispatcher.Read(t.Context(), 1)
		if err != nil || fresh[0].Record.Seq != 2 {
			t.Fatalf("first mixed-lane read = %+v, %v", fresh, err)
		}
		if err := dispatcher.Commit(fresh, func([]uint64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		retry, err := dispatcher.Read(t.Context(), 1)
		if err != nil || retry[0].Record.Seq != 1 || retry[0].Attempt != 2 {
			t.Fatalf("second mixed-lane read = %+v, %v", retry, err)
		}
	})
}

func TestRetryLaneRemainsBounded(t *testing.T) {
	source := &fakeSource{}
	for seq := uint64(1); seq <= 4; seq++ {
		source.add(seq)
	}
	config := testConfig()
	config.PrefetchCapacity = 2
	config.MaxInFlight = 1
	config.RetryInitial = 0
	config.RetryMax = 0
	dispatcher := newTestDispatcher(t, source, config)

	first, err := dispatcher.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := dispatcher.Retry(first); err != nil {
		t.Fatal(err)
	}
	second, err := dispatcher.Read(t.Context(), 1)
	if err != nil || second[0].Record.Seq != 2 {
		t.Fatalf("second delivery = %+v, %v", second, err)
	}
	if err := dispatcher.Retry(second); err != nil {
		t.Fatal(err)
	}
	if progress, err := dispatcher.fillFresh(); err != nil || !progress {
		t.Fatalf("fill more fresh records: progress=%v err=%v", progress, err)
	}
	cycled, err := dispatcher.Read(t.Context(), 1)
	if err != nil || cycled[0].Record.Seq == 3 {
		t.Fatalf("expected retry before next fresh record: %+v, %v", cycled, err)
	}
	if err := dispatcher.Retry(cycled); err != nil {
		t.Fatal(err)
	}
	third, err := dispatcher.Read(t.Context(), 1)
	if err != nil || third[0].Record.Seq != 3 {
		t.Fatalf("third delivery = %+v, %v", third, err)
	}
	if err := dispatcher.Retry(third); err != nil {
		t.Fatal(err)
	}
	if got := dispatcher.Snapshot().Retry; got != 3 {
		t.Fatalf("retry count = %d, want bounded limit 3", got)
	}
	retry, err := dispatcher.Read(t.Context(), 1)
	if err != nil || retry[0].Record.Seq == 4 {
		t.Fatalf("fresh record bypassed full retry bound: %+v, %v", retry, err)
	}
}

func TestLeaseExpiryAndExtension(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &fakeSource{}
		source.add(1)
		dispatcher := newTestDispatcher(t, source, testConfig())
		first, err := dispatcher.Read(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if err := dispatcher.Extend(5*time.Second, first); err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		second, err := dispatcher.Read(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 15*time.Second {
			t.Fatalf("redelivery after %v, want 15s", elapsed)
		}
		if second[0].Record.Seq != 1 || second[0].Attempt != 2 {
			t.Fatalf("redelivery = %+v", second[0])
		}
		if stats := dispatcher.Snapshot(); stats.LeaseExpirations != 1 {
			t.Fatalf("lease expirations = %d", stats.LeaseExpirations)
		}
	})
}

func TestInFlightLimitBackpressuresWithoutSkipping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &fakeSource{}
		source.add(1)
		source.add(2)
		config := testConfig()
		config.MaxInFlight = 1
		dispatcher := newTestDispatcher(t, source, config)
		first, err := dispatcher.Read(t.Context(), 1)
		if err != nil {
			t.Fatal(err)
		}
		result := make(chan []Delivery, 1)
		go func() {
			deliveries, readErr := dispatcher.Read(t.Context(), 1)
			if readErr != nil {
				t.Errorf("second read: %v", readErr)
			}
			result <- deliveries
		}()
		synctest.Wait()
		select {
		case <-result:
			t.Fatal("second read bypassed in-flight bound")
		default:
		}
		if err := dispatcher.Commit(first, func([]uint64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		second := <-result
		if len(second) != 1 || second[0].Record.Seq != 2 {
			t.Fatalf("second delivery = %+v", second)
		}
	})
}

func TestNotifyWakesReadForNewRecord(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &fakeSource{}
		dispatcher := newTestDispatcher(t, source, testConfig())
		result := make(chan []Delivery, 1)
		go func() {
			deliveries, err := dispatcher.Read(t.Context(), 1)
			if err != nil {
				t.Errorf("read: %v", err)
			}
			result <- deliveries
		}()
		synctest.Wait()
		source.add(1)
		dispatcher.Notify()
		synctest.Wait()
		deliveries := <-result
		if len(deliveries) != 1 || deliveries[0].Record.Seq != 1 {
			t.Fatalf("deliveries = %+v", deliveries)
		}
	})
}

func TestRestartRecoversEveryUncommittedRecord(t *testing.T) {
	source := &fakeSource{}
	source.add(1)
	firstDispatcher := newTestDispatcher(t, source, testConfig())
	first, err := firstDispatcher.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	firstDispatcher.Close()

	secondDispatcher := newTestDispatcher(t, source, testConfig())
	recovered, err := secondDispatcher.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if recovered[0].Record.Seq != first[0].Record.Seq || recovered[0].Attempt != 1 {
		t.Fatalf("recovered delivery = %+v, first = %+v", recovered[0], first[0])
	}
}

func TestCloseWakesBlockedRead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dispatcher := newTestDispatcher(t, &fakeSource{}, testConfig())
		result := make(chan error, 1)
		go func() {
			_, err := dispatcher.Read(t.Context(), 1)
			result <- err
		}()
		synctest.Wait()
		dispatcher.Close()
		synctest.Wait()
		if err := <-result; !errors.Is(err, ErrClosed) {
			t.Fatalf("blocked read after close = %v", err)
		}
	})
}

func TestCancellationAndClose(t *testing.T) {
	source := &fakeSource{}
	dispatcher := newTestDispatcher(t, source, testConfig())
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := dispatcher.Read(ctx, 1); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled read = %v", err)
	}
	dispatcher.Close()
	if _, err := dispatcher.Read(t.Context(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed read = %v", err)
	}
}

func TestSourceErrorsAreNotSkipped(t *testing.T) {
	sourceErr := errors.New("corrupt record")
	source := &fakeSource{err: sourceErr}
	dispatcher := newTestDispatcher(t, source, testConfig())
	if _, err := dispatcher.Read(t.Context(), 1); !errors.Is(err, sourceErr) {
		t.Fatalf("read error = %v", err)
	}
}

func TestInvalidConfiguration(t *testing.T) {
	config := testConfig()
	config.PrefetchCapacity = 0
	if _, err := New(&fakeSource{}, config); !errors.Is(err, ErrInvalidConfig) {
		t.Fatalf("new error = %v", err)
	}
}
