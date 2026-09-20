package badgerstore

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
)

func openTestStream(t *testing.T, name string) (*Root, *Stream) {
	t.Helper()
	root, err := Open(t.TempDir(), DefaultOptions(""))
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	t.Cleanup(func() { _ = root.Close() })
	st, err := root.Stream(name)
	if err != nil {
		t.Fatalf("stream %q: %v", name, err)
	}
	return root, st
}

func mustAllocate(t *testing.T, st *Stream, n uint64) uint64 {
	t.Helper()
	start, err := st.Allocate(n)
	if err != nil {
		t.Fatalf("allocate %d: %v", n, err)
	}
	return start
}

// scanEntry is a local test helper; core.Entry arrives in M2.
type scanEntry struct {
	seq  uint64
	data []byte
}

func scanAll(t *testing.T, st *Stream, from uint64) []scanEntry {
	t.Helper()
	var got []scanEntry
	err := st.ScanFrom(from, func(seq uint64, data []byte) error {
		got = append(got, scanEntry{seq: seq, data: bytes.Clone(data)})
		return nil
	})
	if err != nil {
		t.Fatalf("scan from %d: %v", from, err)
	}
	return got
}

func TestKeyOrderingAndParsing(t *testing.T) {
	const log = "s1"
	k1 := dataKey(log, 1)
	k2 := dataKey(log, 2)
	kmax := dataKey(log, math.MaxUint64)

	if !bytesLess(k1, k2) || !bytesLess(k2, kmax) {
		t.Error("data keys are not ordered by sequence")
	}
	for _, seq := range []uint64{1, 2, math.MaxUint64, 1 << 40} {
		got, ok := parseDataSeq(log, dataKey(log, seq))
		if !ok || got != seq {
			t.Errorf("parseDataSeq round-trip %d: got %d, ok=%v", seq, got, ok)
		}
	}
	if _, ok := parseDataSeq(log, metaKey(log, "seq-lease")); ok {
		t.Error("meta key parsed as data key")
	}
	if _, ok := parseDataSeq("other", dataKey(log, 1)); ok {
		t.Error("wrong-stream data key parsed")
	}
	// The data prefix sorts before every data key, so Seek(prefix) lands on
	// the first entry.
	if !bytesLess(dataPrefix(log), dataKey(log, 0)) {
		t.Error("data prefix must sort before data keys")
	}
}

func bytesLess(a, b []byte) bool { return bytes.Compare(a, b) < 0 }

func TestStreamNameValidation(t *testing.T) {
	root, _ := openTestStream(t, "boot")
	for _, name := range []string{"", "a/b", "a b", "中文", strings.Repeat("a", 101)} {
		if _, err := root.Stream(name); !errors.Is(err, ErrInvalidStreamName) {
			t.Errorf("Stream(%q): want ErrInvalidStreamName, got %v", name, err)
		}
	}
	for _, name := range []string{"a", "log-1", "a.b_c", strings.Repeat("a", 100)} {
		if _, err := root.Stream(name); err != nil {
			t.Errorf("Stream(%q): unexpected error %v", name, err)
		}
	}
}

func TestPutScan(t *testing.T) {
	_, st := openTestStream(t, "s")

	start := mustAllocate(t, st, 5)
	if start != 1 {
		t.Fatalf("first allocation = %d, want 1", start)
	}
	for i := uint64(1); i <= 5; i++ {
		if err := st.Put(i, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	all := scanAll(t, st, 1)
	if len(all) != 5 {
		t.Fatalf("scanned %d entries, want 5", len(all))
	}
	for i, e := range all {
		if want := fmt.Sprintf("v%d", i+1); string(e.data) != want || e.seq != uint64(i+1) {
			t.Errorf("entry %d: got (%d, %q), want (%d, %q)", i, e.seq, e.data, i+1, want)
		}
	}
	if from3 := scanAll(t, st, 3); len(from3) != 3 || from3[0].seq != 3 {
		t.Errorf("ScanFrom(3): got %d entries, first seq %d", len(from3), from3[0].seq)
	}
	if tail := scanAll(t, st, 100); len(tail) != 0 {
		t.Errorf("ScanFrom(100): got %d entries, want 0", len(tail))
	}
}

func TestScanStop(t *testing.T) {
	_, st := openTestStream(t, "s")
	for i := uint64(1); i <= 5; i++ {
		if err := st.Put(i, nil); err != nil {
			t.Fatal(err)
		}
	}
	n := 0
	err := st.ScanFrom(1, func(uint64, []byte) error {
		n++
		if n == 2 {
			return ErrStopScan
		}
		return nil
	})
	if err != nil {
		t.Fatalf("ScanFrom returned %v, want nil after ErrStopScan", err)
	}
	if n != 2 {
		t.Fatalf("callback ran %d times, want 2", n)
	}
}

func TestBatchPutAndHoles(t *testing.T) {
	_, st := openTestStream(t, "s")
	mustAllocate(t, st, 4)
	if err := st.Put(1, []byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := st.Put(3, []byte("three")); err != nil {
		t.Fatal(err)
	}
	if got := scanAll(t, st, 1); len(got) != 2 || got[0].seq != 1 || got[1].seq != 3 {
		t.Fatalf("holes not skipped: %+v", got)
	}
	if err := st.BatchPut([]uint64{2, 4}, [][]byte{[]byte("two"), []byte("four")}); err != nil {
		t.Fatalf("BatchPut: %v", err)
	}
	all := scanAll(t, st, 1)
	if len(all) != 4 {
		t.Fatalf("after BatchPut: %d entries, want 4", len(all))
	}
	for i, e := range all {
		if e.seq != uint64(i+1) {
			t.Errorf("entry %d has seq %d", i, e.seq)
		}
	}
	if err := st.BatchPut([]uint64{9}, nil); err == nil {
		t.Error("BatchPut length mismatch not detected")
	}
}

func TestDeleteBelow(t *testing.T) {
	_, st := openTestStream(t, "s")
	for i := uint64(1); i <= 10; i++ {
		if err := st.Put(i, []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	removed, err := st.DeleteBelow(6, 2) // small limit forces multiple commits
	if err != nil {
		t.Fatalf("DeleteBelow: %v", err)
	}
	if removed != 5 {
		t.Fatalf("removed %d entries, want 5", removed)
	}
	rest := scanAll(t, st, 1)
	if len(rest) != 5 || rest[0].seq != 6 {
		t.Fatalf("after delete: %d entries, first seq %d, want 5 entries from seq 6", len(rest), rest[0].seq)
	}
	if again, _ := st.DeleteBelow(6, 2); again != 0 {
		t.Errorf("repeated DeleteBelow removed %d, want 0", again)
	}
	if beyond, _ := st.DeleteBelow(math.MaxUint64, 4); beyond != 5 {
		t.Errorf("DeleteBelow(MaxUint64) removed %d, want 5", beyond)
	}
	if left := scanAll(t, st, 1); len(left) != 0 {
		t.Errorf("%d entries left, want 0", len(left))
	}
}

func TestMeta(t *testing.T) {
	_, st := openTestStream(t, "s")
	if _, err := st.GetMeta("nope"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("GetMeta missing: got %v, want ErrNotFound", err)
	}
	if err := st.PutMeta("k", []byte("v1")); err != nil {
		t.Fatal(err)
	}
	v, err := st.GetMeta("k")
	if err != nil || string(v) != "v1" {
		t.Fatalf("GetMeta: (%q, %v)", v, err)
	}
	if err := st.PutMeta("k", []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if v, _ = st.GetMeta("k"); string(v) != "v2" {
		t.Fatalf("GetMeta after overwrite: %q", v)
	}
}

type leaseRecorder struct {
	highs []uint64
	fail  bool
}

func (r *leaseRecorder) persist(high uint64) error {
	if r.fail {
		return errors.New("boom")
	}
	r.highs = append(r.highs, high)
	return nil
}

func TestSeqAllocator(t *testing.T) {
	rec := &leaseRecorder{}
	a := newSeqAllocator(rec.persist, 4, 0)

	if start, _ := a.next(2); start != 1 {
		t.Fatalf("first allocation = %d, want 1", start)
	}
	if start, _ := a.next(3); start != 3 {
		t.Fatalf("second allocation = %d, want 3", start)
	}
	if start, _ := a.next(1); start != 6 {
		t.Fatalf("third allocation = %d, want 6", start)
	}
	if !equalU64(rec.highs, []uint64{4, 8}) {
		t.Fatalf("lease highs = %v, want [4 8]", rec.highs)
	}

	rec.fail = true
	if _, err := a.next(5); err == nil {
		t.Fatal("allocation with failing persist succeeded")
	}
	if start, err := a.next(1); err != nil || start != 7 {
		t.Fatalf("allocation of already-leased range = (%d, %v), want (7, nil)", start, err)
	}
	rec.fail = false
	if start, _ := a.next(5); start != 8 {
		t.Fatalf("retry allocation = %d, want 8", start)
	}
	if !equalU64(rec.highs, []uint64{4, 8, 12}) {
		t.Fatalf("lease highs = %v, want [4 8 12]", rec.highs)
	}

	// Recovery from a durable lease: the abandoned tail is skipped, never reused.
	b := newSeqAllocator(func(uint64) error { return nil }, 4, 8)
	if start, _ := b.next(1); start != 9 {
		t.Fatalf("recovered allocation = %d, want 9", start)
	}
	if _, err := newSeqAllocator(func(uint64) error { return nil }, 4, math.MaxUint64).next(1); !errors.Is(err, ErrSeqExhausted) {
		t.Fatalf("exhausted space: got %v, want ErrSeqExhausted", err)
	}
}

func equalU64(a, b []uint64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestSeqLeaseReopen(t *testing.T) {
	dir := t.TempDir()
	root, err := Open(dir, DefaultOptions(""))
	if err != nil {
		t.Fatal(err)
	}
	st, err := root.Stream("s")
	if err != nil {
		t.Fatal(err)
	}
	if start := mustAllocate(t, st, 5); start != 1 {
		t.Fatalf("first allocation = %d", start)
	}
	if err := st.Put(1, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen: the durable lease (1024) is honored — sequence 1 is never
	// handed out again and allocation resumes past the lease.
	root, err = Open(dir, DefaultOptions(""))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Errorf("close reopened root: %v", err)
		}
	}()
	st, err = root.Stream("s")
	if err != nil {
		t.Fatal(err)
	}
	if start := mustAllocate(t, st, 1); start != defaultSeqLease+1 {
		t.Fatalf("allocation after reopen = %d, want %d", start, defaultSeqLease+1)
	}
	all := scanAll(t, st, 1)
	if len(all) != 1 || all[0].seq != 1 || string(all[0].data) != "hello" {
		t.Fatalf("data lost across reopen: %+v", all)
	}
}

func TestConcurrentAllocateAndPut(t *testing.T) {
	_, st := openTestStream(t, "s")

	const workers = 8
	const perWorker = 25
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				start, err := st.Allocate(1)
				if err != nil {
					errs <- err
					return
				}
				if err := st.Put(start, []byte(fmt.Sprintf("w%d-%d", w, i))); err != nil {
					errs <- err
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}

	got := scanAll(t, st, 1)
	if len(got) != workers*perWorker {
		t.Fatalf("scanned %d entries, want %d", len(got), workers*perWorker)
	}
	for i, e := range got {
		if e.seq != uint64(i+1) {
			t.Fatalf("entry %d has seq %d: sequences are not contiguous/unique", i, e.seq)
		}
	}
}

func TestRootStreamReuse(t *testing.T) {
	root, first := openTestStream(t, "s")
	again, err := root.Stream("s")
	if err != nil {
		t.Fatal(err)
	}
	if first != again {
		t.Fatal("Root.Stream returned distinct instances for the same name")
	}
	if first.Name() != "s" {
		t.Errorf("Name() = %q", first.Name())
	}
}

func TestStreamIsolation(t *testing.T) {
	root, s1 := openTestStream(t, "s1")
	s2, err := root.Stream("s2")
	if err != nil {
		t.Fatal(err)
	}
	if err := s1.Put(1, []byte("s1-data")); err != nil {
		t.Fatal(err)
	}
	if err := s2.Put(1, []byte("s2-data")); err != nil {
		t.Fatal(err)
	}
	if got := scanAll(t, s1, 1); len(got) != 1 || string(got[0].data) != "s1-data" {
		t.Fatalf("s1 sees foreign data: %+v", got)
	}
	if got := scanAll(t, s2, 1); len(got) != 1 || string(got[0].data) != "s2-data" {
		t.Fatalf("s2 sees foreign data: %+v", got)
	}
	if removed, err := s1.DeleteBelow(2, 1); err != nil || removed != 1 {
		t.Fatalf("s1 DeleteBelow: (%d, %v)", removed, err)
	}
	if got := scanAll(t, s2, 1); len(got) != 1 {
		t.Fatalf("s2 lost data after s1 delete: %+v", got)
	}
}
