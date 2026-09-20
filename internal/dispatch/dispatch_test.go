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
	scans   int
	onScan  func()
}

func (s *fakeSource) ScanBatch(from uint64, limit int) ([]store.Record, error) {
	s.mu.Lock()
	s.scans++
	onScan := s.onScan
	s.onScan = nil
	if s.err != nil {
		err := s.err
		s.mu.Unlock()
		if onScan != nil {
			onScan()
		}
		return nil, err
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
	s.mu.Unlock()
	if onScan != nil {
		onScan()
	}
	return result, nil
}

func (s *fakeSource) add(seq uint64) {
	s.mu.Lock()
	s.records = append(s.records, store.Record{Seq: seq, CreatedAt: time.Now(), Payload: []byte{byte(seq)}})
	s.mu.Unlock()
}

func (s *fakeSource) scanCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.scans
}

func (s *fakeSource) notifyDuringNextScan(notify func()) {
	s.mu.Lock()
	s.onScan = notify
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

func TestReceiptCollisionDoesNotOverwriteActiveDelivery(t *testing.T) {
	source := &fakeSource{}
	source.add(1)
	source.add(2)
	dispatcher := newTestDispatcher(t, source, testConfig())
	receipts := []Receipt{{1}, {1}, {2}}
	dispatcher.newReceipt = func() (Receipt, error) {
		receipt := receipts[0]
		receipts = receipts[1:]
		return receipt, nil
	}

	deliveries, err := dispatcher.Read(t.Context(), 2)
	if err != nil || len(deliveries) != 2 {
		t.Fatalf("read: len=%d err=%v", len(deliveries), err)
	}
	if deliveries[0].Receipt == deliveries[1].Receipt || dispatcher.Snapshot().InFlight != 2 {
		t.Fatalf("colliding receipts overwrote an active delivery: %+v", deliveries)
	}
}

func TestReceiptFailureRollsBackPartialBatch(t *testing.T) {
	source := &fakeSource{}
	source.add(1)
	source.add(2)
	dispatcher := newTestDispatcher(t, source, testConfig())
	receiptCalls := 0
	dispatcher.newReceipt = func() (Receipt, error) {
		receiptCalls++
		if receiptCalls == 2 {
			return Receipt{}, errors.New("entropy unavailable")
		}
		return Receipt{byte(receiptCalls)}, nil
	}

	if _, err := dispatcher.Read(t.Context(), 2); err == nil {
		t.Fatal("read succeeded after receipt generation failure")
	}
	if stats := dispatcher.Snapshot(); stats.InFlight != 0 || stats.Ready != 2 || stats.Deliveries != 0 {
		t.Fatalf("partial read was not rolled back: %+v", stats)
	}
	dispatcher.newReceipt = randomReceipt
	deliveries, err := dispatcher.Read(t.Context(), 2)
	if err != nil || len(deliveries) != 2 || deliveries[0].Record.Seq != 1 || deliveries[1].Record.Seq != 2 {
		t.Fatalf("records unavailable after receipt rollback: %+v, %v", deliveries, err)
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

func TestDueRetryDoesNotPreventNextFreshScan(t *testing.T) {
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

	poison, err := dispatcher.Read(t.Context(), 1)
	if err != nil || poison[0].Record.Seq != 1 {
		t.Fatalf("first delivery = %+v, %v", poison, err)
	}
	if err := dispatcher.Retry(poison); err != nil {
		t.Fatal(err)
	}

	fresh, err := dispatcher.Read(t.Context(), 1)
	if err != nil || fresh[0].Record.Seq != 2 {
		t.Fatalf("last prefetched delivery = %+v, %v", fresh, err)
	}
	if err := dispatcher.Commit(fresh, func([]uint64) error { return nil }); err != nil {
		t.Fatal(err)
	}

	poison, err = dispatcher.Read(t.Context(), 1)
	if err != nil || poison[0].Record.Seq != 1 {
		t.Fatalf("retry before refill = %+v, %v", poison, err)
	}
	if err := dispatcher.Retry(poison); err != nil {
		t.Fatal(err)
	}

	fresh, err = dispatcher.Read(t.Context(), 1)
	if err != nil || fresh[0].Record.Seq != 3 {
		t.Fatalf("first delivery after refill = %+v, %v", fresh, err)
	}
}

func TestEmptyScanIsCachedUntilNotify(t *testing.T) {
	source := &fakeSource{}
	dispatcher := newTestDispatcher(t, source, testConfig())

	if progress, err := dispatcher.fillFresh(); err != nil || progress {
		t.Fatalf("first empty scan: progress=%v err=%v", progress, err)
	}
	if progress, err := dispatcher.fillFresh(); err != nil || progress {
		t.Fatalf("cached empty scan: progress=%v err=%v", progress, err)
	}
	if scans := source.scanCount(); scans != 1 {
		t.Fatalf("empty scan count = %d, want 1", scans)
	}

	source.add(1)
	dispatcher.Notify()
	if progress, err := dispatcher.fillFresh(); err != nil || !progress {
		t.Fatalf("scan after notify: progress=%v err=%v", progress, err)
	}
	if scans := source.scanCount(); scans != 2 {
		t.Fatalf("scan count after notify = %d, want 2", scans)
	}
}

func TestNotifyDuringScanKeepsTailInvalidated(t *testing.T) {
	source := &fakeSource{}
	dispatcher := newTestDispatcher(t, source, testConfig())
	source.notifyDuringNextScan(dispatcher.Notify)

	if progress, err := dispatcher.fillFresh(); err != nil || progress {
		t.Fatalf("notified empty scan: progress=%v err=%v", progress, err)
	}
	if progress, err := dispatcher.fillFresh(); err != nil || progress {
		t.Fatalf("scan after concurrent notify: progress=%v err=%v", progress, err)
	}
	if scans := source.scanCount(); scans != 2 {
		t.Fatalf("scan count after concurrent notify = %d, want 2", scans)
	}
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

func TestRetryDelayMultiplierOneKeepsConstantDelay(t *testing.T) {
	dispatcher := &Dispatcher{config: Config{RetryInitial: 3 * time.Second, RetryMax: time.Minute, RetryMultiplier: 1}}
	for attempt := uint32(1); attempt <= 5; attempt++ {
		if got := dispatcher.retryDelay(attempt); got != 3*time.Second {
			t.Fatalf("retryDelay(%d) = %v, want constant 3s", attempt, got)
		}
	}
}

func TestRetryDelayGrowsExponentiallyAndCaps(t *testing.T) {
	dispatcher := &Dispatcher{config: Config{RetryInitial: time.Second, RetryMax: time.Minute, RetryMultiplier: 2}}
	want := map[uint32]time.Duration{
		1: time.Second,
		2: 2 * time.Second,
		3: 4 * time.Second,
		4: 8 * time.Second,
		5: 16 * time.Second,
		6: 32 * time.Second,
		7: time.Minute,
		8: time.Minute,
	}
	for attempt, expected := range want {
		if got := dispatcher.retryDelay(attempt); got != expected {
			t.Fatalf("retryDelay(%d) = %v, want %v", attempt, got, expected)
		}
	}
}

func TestBackpressureCountedOncePerBlockedRead(t *testing.T) {
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
		blocked := make(chan []Delivery, 1)
		go func() {
			deliveries, readErr := dispatcher.Read(t.Context(), 1)
			if readErr != nil {
				t.Errorf("blocked read: %v", readErr)
			}
			blocked <- deliveries
		}()
		synctest.Wait()
		if stats := dispatcher.Snapshot(); stats.Backpressure != 1 {
			t.Fatalf("backpressure events while blocked = %d, want exactly 1", stats.Backpressure)
		}
		if err := dispatcher.Commit(first, func([]uint64) error { return nil }); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		second := <-blocked
		if len(second) != 1 || second[0].Record.Seq != 2 {
			t.Fatalf("second delivery = %+v", second)
		}
		if stats := dispatcher.Snapshot(); stats.Backpressure != 1 {
			t.Fatalf("backpressure events after wake = %d, want still 1", stats.Backpressure)
		}
	})
}

func TestSnapshotReapsExpiredLeases(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &fakeSource{}
		source.add(1)
		dispatcher := newTestDispatcher(t, source, testConfig())
		if _, err := dispatcher.Read(t.Context(), 1); err != nil {
			t.Fatal(err)
		}
		time.Sleep(11 * time.Second)
		stats := dispatcher.Snapshot()
		if stats.InFlight != 0 || stats.Retry != 1 || stats.LeaseExpirations != 1 {
			t.Fatalf("stats after expiry = %+v, want in-flight 0, retry 1, one expiration", stats)
		}
	})
}
