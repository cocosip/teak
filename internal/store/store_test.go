package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/dgraph-io/badger/v4"
)

func openTestStream(t *testing.T, name string) (*Root, *Stream) {
	t.Helper()
	root, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open root: %v", err)
	}
	t.Cleanup(func() {
		if err := root.Close(); err != nil {
			t.Errorf("close root: %v", err)
		}
	})
	stream, err := root.Stream(name)
	if err != nil {
		t.Fatalf("open stream: %v", err)
	}
	return root, stream
}

func TestKeyOrderingAndParsing(t *testing.T) {
	prefix := pendingPrefix("s")
	if bytes.Compare(pendingKey("s", 1), pendingKey("s", 2)) >= 0 ||
		bytes.Compare(pendingKey("s", 2), pendingKey("s", math.MaxUint64)) >= 0 {
		t.Fatal("pending keys do not preserve sequence order")
	}
	for _, seq := range []uint64{0, 1, 1 << 40, math.MaxUint64} {
		got, ok := parseSequence(prefix, pendingKey("s", seq))
		if !ok || got != seq {
			t.Fatalf("parse sequence %d: got %d, ok=%v", seq, got, ok)
		}
	}
	if _, ok := parseSequence(prefix, deadKey("s", 1)); ok {
		t.Fatal("dead-letter key parsed as pending")
	}
}

func TestEnvelopeRoundTripAndValidation(t *testing.T) {
	const origin = "origin"
	now := time.Unix(123, 456).UTC()
	pending := pendingEnvelope{CreatedAt: now, OriginStream: origin, OriginSeq: 9, Payload: []byte("payload")}
	encoded, err := encodePending(pending)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodePending(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if !got.CreatedAt.Equal(now) || got.OriginStream != origin || got.OriginSeq != 9 || string(got.Payload) != "payload" {
		t.Fatalf("pending round trip: %+v", got)
	}
	dead := deadEnvelope{
		CreatedAt: now, DeadLetteredAt: now.Add(time.Second), OriginStream: origin, OriginSeq: 9,
		Payload: []byte("payload"), Reason: "failed",
	}
	deadBytes, err := encodeDead(dead)
	if err != nil {
		t.Fatal(err)
	}
	deadGot, err := decodeDead(deadBytes)
	if err != nil || deadGot.Reason != "failed" || !bytes.Equal(deadGot.Payload, dead.Payload) {
		t.Fatalf("dead round trip: (%+v, %v)", deadGot, err)
	}
	for _, invalid := range [][]byte{nil, {99}, encoded[:len(encoded)-1], append(encoded, 0)} {
		if _, err := decodePending(invalid); err == nil {
			t.Fatalf("decoded invalid envelope %x", invalid)
		}
	}
}

func TestAppendBatchAndReopenHasNoSequenceHole(t *testing.T) {
	dir := t.TempDir()
	now := time.Unix(100, 0).UTC()
	root, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := root.Stream("jobs")
	if err != nil {
		t.Fatal(err)
	}
	records, err := stream.AppendBatch([][]byte{[]byte("one"), []byte("two")}, now)
	if err != nil {
		t.Fatal(err)
	}
	if records[0].Seq != 1 || records[1].Seq != 2 {
		t.Fatalf("sequences = %d,%d", records[0].Seq, records[1].Seq)
	}
	if err := root.Close(); err != nil {
		t.Fatal(err)
	}

	root, err = Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := root.Close(); err != nil {
			t.Errorf("close reopened root: %v", err)
		}
	}()
	stream, err = root.Stream("jobs")
	if err != nil {
		t.Fatal(err)
	}
	record, err := stream.Append([]byte("three"), now)
	if err != nil {
		t.Fatal(err)
	}
	if record.Seq != 3 {
		t.Fatalf("sequence after reopen = %d, want 3", record.Seq)
	}
	all, err := stream.ScanBatch(1, 10)
	if err != nil || len(all) != 3 {
		t.Fatalf("scan after reopen: len=%d err=%v", len(all), err)
	}
}

func TestFailedAppendDoesNotAdvanceTail(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	stream.beforeCommit = func(operation string) error {
		if operation == "append" {
			return errors.New("injected failure")
		}
		return nil
	}
	if _, err := stream.Append([]byte("lost"), time.Now()); err == nil {
		t.Fatal("injected append unexpectedly succeeded")
	}
	stream.beforeCommit = nil
	record, err := stream.Append([]byte("kept"), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if record.Seq != 1 {
		t.Fatalf("sequence after failed append = %d, want 1", record.Seq)
	}
	all, err := stream.ScanBatch(1, 10)
	if err != nil || len(all) != 1 || string(all[0].Payload) != "kept" {
		t.Fatalf("records after failed append: %+v, %v", all, err)
	}
}

func TestOversizedBatchIsAtomic(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	payloads := [][]byte{
		bytes.Repeat([]byte{'x'}, maxBatchValueSize/2+1),
		bytes.Repeat([]byte{'y'}, maxBatchValueSize/2),
	}
	if _, err := stream.AppendBatch(payloads, time.Now()); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("oversized batch error = %v, want ErrBatchTooLarge", err)
	}
	counts, err := stream.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Tail != 0 || counts.Pending != 0 {
		t.Fatalf("failed batch changed state: %+v", counts)
	}
}

func TestBadgerTransactionLimitMapsToBatchTooLarge(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	payloads := make([][]byte, stream.db.MaxBatchCount())
	if _, err := stream.AppendBatch(payloads, time.Now()); !errors.Is(err, ErrBatchTooLarge) {
		t.Fatalf("transaction-limit error = %v, want ErrBatchTooLarge", err)
	}
	counts, err := stream.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Pending != 0 || counts.Tail != 0 {
		t.Fatalf("transaction-limit failure changed storage: %+v", counts)
	}
}

func TestConcurrentAppendIsContiguous(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	const workers = 8
	const perWorker = 25
	var wait sync.WaitGroup
	errs := make(chan error, workers)
	for worker := range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			for index := range perWorker {
				if _, err := stream.Append([]byte(fmt.Sprintf("%d-%d", worker, index)), time.Now()); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	records, err := stream.ScanBatch(1, workers*perWorker+1)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != workers*perWorker {
		t.Fatalf("records = %d", len(records))
	}
	for index, record := range records {
		if record.Seq != uint64(index+1) {
			t.Fatalf("record %d sequence = %d", index, record.Seq)
		}
	}
}

func TestScanBatchOwnsPayloadAndRejectsCorruption(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	if _, err := stream.Append([]byte("one"), time.Now()); err != nil {
		t.Fatal(err)
	}
	first, err := stream.ScanBatch(1, 1)
	if err != nil {
		t.Fatal(err)
	}
	first[0].Payload[0] = 'X'
	again, err := stream.ScanBatch(1, 1)
	if err != nil || string(again[0].Payload) != "one" {
		t.Fatalf("stored payload aliased scan result: %+v, %v", again, err)
	}
	if err := stream.db.Update(func(txn *badger.Txn) error {
		return txn.Set(pendingKey(stream.name, 1), []byte{99})
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.ScanBatch(1, 1); !errors.Is(err, ErrUnsupportedEnvelope) {
		t.Fatalf("corrupt scan error = %v", err)
	}
}

func TestCommitDeletesRecordsIndependently(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	if _, err := stream.AppendBatch([][]byte{[]byte("one"), []byte("two"), []byte("three")}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Commit([]uint64{2}); err != nil {
		t.Fatal(err)
	}
	records, err := stream.ScanBatch(1, 10)
	if err != nil || len(records) != 2 || records[0].Seq != 1 || records[1].Seq != 3 {
		t.Fatalf("out-of-order commit result: %+v, %v", records, err)
	}
	if err := stream.Commit([]uint64{2}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("duplicate commit = %v", err)
	}
}

func TestCommitBatchIsAtomicOnMissingRecord(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	if _, err := stream.AppendBatch([][]byte{[]byte("one"), []byte("two")}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := stream.Commit([]uint64{1, 99}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("commit error = %v", err)
	}
	records, err := stream.ScanBatch(1, 10)
	if err != nil || len(records) != 2 {
		t.Fatalf("partial commit occurred: %+v, %v", records, err)
	}
}

func TestDeadLetterAndRequeueAreAtomic(t *testing.T) {
	const streamName = "jobs"
	_, stream := openTestStream(t, streamName)
	now := time.Unix(100, 0).UTC()
	if _, err := stream.Append([]byte("payload"), now); err != nil {
		t.Fatal(err)
	}
	stream.beforeCommit = func(operation string) error {
		if operation == "dead-letter" {
			return errors.New("injected failure")
		}
		return nil
	}
	if err := stream.DeadLetter(1, "bad", now.Add(time.Second)); err == nil {
		t.Fatal("injected dead-letter unexpectedly succeeded")
	}
	stream.beforeCommit = nil
	if pending, _ := stream.ScanBatch(1, 10); len(pending) != 1 {
		t.Fatalf("pending record lost on failed transfer: %+v", pending)
	}
	if dead, _ := stream.DeadLetters(1, 10); len(dead) != 0 {
		t.Fatalf("dead record leaked on failed transfer: %+v", dead)
	}
	if err := stream.DeadLetter(1, "bad", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	dead, err := stream.DeadLetters(1, 10)
	if err != nil || len(dead) != 1 || dead[0].OriginStream != streamName || dead[0].OriginSeq != 1 || dead[0].Reason != "bad" {
		t.Fatalf("dead letter: %+v, %v", dead, err)
	}
	requeued, err := stream.Requeue(1, now.Add(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if requeued.Seq != 2 || requeued.OriginStream != streamName || requeued.OriginSeq != 1 {
		t.Fatalf("requeued record = %+v", requeued)
	}
	if dead, _ = stream.DeadLetters(1, 10); len(dead) != 0 {
		t.Fatalf("dead record remains after requeue: %+v", dead)
	}
}

func TestStreamValidationIsolationAndReuse(t *testing.T) {
	root, first := openTestStream(t, "first")
	again, err := root.Stream("first")
	if err != nil || again != first {
		t.Fatalf("stream reuse: %p %p %v", first, again, err)
	}
	for _, name := range []string{"", "bad/name", "bad name", "中文"} {
		if _, err := root.Stream(name); !errors.Is(err, ErrInvalidStreamName) {
			t.Fatalf("stream %q: %v", name, err)
		}
	}
	second, err := root.Stream("second")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := first.Append([]byte("first"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Append([]byte("second"), time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := first.Commit([]uint64{1}); err != nil {
		t.Fatal(err)
	}
	secondRecords, err := second.ScanBatch(1, 10)
	if err != nil || len(secondRecords) != 1 || string(secondRecords[0].Payload) != "second" {
		t.Fatalf("stream isolation: %+v, %v", secondRecords, err)
	}
}

func TestUnsupportedSchemaFailsOpen(t *testing.T) {
	root, stream := openTestStream(t, "jobs")
	if err := root.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey(stream.name, schemaMetaKey), []byte{99})
	}); err != nil {
		t.Fatal(err)
	}
	root.mu.Lock()
	delete(root.streams, "jobs")
	root.mu.Unlock()
	if _, err := root.Stream("jobs"); !errors.Is(err, ErrUnsupportedSchema) {
		t.Fatalf("unsupported schema error = %v", err)
	}
}

func TestTailRegressionFailsOpen(t *testing.T) {
	root, stream := openTestStream(t, "jobs")
	if _, err := stream.AppendBatch([][]byte{[]byte("one"), []byte("two")}, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := root.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey(stream.name, tailMetaKey), binary.BigEndian.AppendUint64(nil, 1))
	}); err != nil {
		t.Fatal(err)
	}
	root.mu.Lock()
	delete(root.streams, "jobs")
	root.mu.Unlock()
	if _, err := root.Stream("jobs"); !errors.Is(err, ErrTailRegression) {
		t.Fatalf("tail regression error = %v", err)
	}
}

func TestDeadLetterRefusesConflictingState(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	now := time.Unix(100, 0).UTC()
	if _, err := stream.Append([]byte("pending"), now); err != nil {
		t.Fatal(err)
	}
	dead, err := encodeDead(deadEnvelope{
		CreatedAt: now, DeadLetteredAt: now, OriginStream: "jobs", OriginSeq: 1,
		Payload: []byte("existing"), Reason: "existing",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.db.Update(func(txn *badger.Txn) error {
		return txn.Set(deadKey(stream.name, 1), dead)
	}); err != nil {
		t.Fatal(err)
	}
	if err := stream.DeadLetter(1, "replacement", now); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("dead-letter conflict = %v", err)
	}
	pending, err := stream.ScanBatch(1, 10)
	if err != nil || len(pending) != 1 || string(pending[0].Payload) != "pending" {
		t.Fatalf("pending changed after conflict: %+v, %v", pending, err)
	}
	deadLetters, err := stream.DeadLetters(1, 10)
	if err != nil || len(deadLetters) != 1 || string(deadLetters[0].Payload) != "existing" {
		t.Fatalf("dead letter changed after conflict: %+v, %v", deadLetters, err)
	}
}

func TestCounts(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	now := time.Unix(100, 0).UTC()
	if _, err := stream.AppendBatch([][]byte{[]byte("one"), []byte("two")}, now); err != nil {
		t.Fatal(err)
	}
	if err := stream.DeadLetter(2, "bad", now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	counts, err := stream.Counts()
	if err != nil {
		t.Fatal(err)
	}
	if counts.Tail != 2 || counts.Pending != 1 || counts.DeadLetter != 1 || counts.OldestSeq != 1 || !counts.OldestTime.Equal(now) {
		t.Fatalf("counts = %+v", counts)
	}
}

func TestTailEncodingIsBigEndian(t *testing.T) {
	_, stream := openTestStream(t, "jobs")
	if _, err := stream.Append([]byte("one"), time.Now()); err != nil {
		t.Fatal(err)
	}
	err := stream.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey(stream.name, tailMetaKey))
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		if binary.BigEndian.Uint64(value) != 1 {
			return fmt.Errorf("tail = %x", value)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
