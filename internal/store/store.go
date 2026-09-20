// Package store implements Teak's durable Badger persistence layer.
package store

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
)

const (
	tailMetaKey       = "tail"
	schemaMetaKey     = "schema"
	maxBatchValueSize = 32 << 20
)

var (
	// ErrInvalidStreamName reports a stream name outside the supported character set.
	ErrInvalidStreamName = errors.New("store: invalid stream name")
	// ErrNotFound reports a missing pending or dead-letter record.
	ErrNotFound = errors.New("store: record not found")
	// ErrSeqExhausted reports that no further uint64 sequence can be assigned.
	ErrSeqExhausted = errors.New("store: sequence space exhausted")
	// ErrClosed reports use after the storage root has closed.
	ErrClosed = errors.New("store: closed")
	// ErrInvalidBatchSize reports a non-positive scan limit.
	ErrInvalidBatchSize = errors.New("store: invalid batch size")
	// ErrBatchTooLarge reports a batch beyond Teak's atomic transaction limit.
	ErrBatchTooLarge = errors.New("store: batch too large")
	// ErrUnsupportedSchema reports an unknown stream schema version.
	ErrUnsupportedSchema = errors.New("store: unsupported schema version")
	// ErrNoRewrite reports that value-log GC found no eligible file.
	ErrNoRewrite = errors.New("store: no value log rewrite needed")
	// ErrTailRegression reports metadata that could cause a stored sequence to be reused.
	ErrTailRegression = errors.New("store: durable tail is behind stored records")
	// ErrStateConflict reports mutually exclusive queue states for one sequence.
	ErrStateConflict = errors.New("store: conflicting record state")
)

// Record is a copied pending record returned by ScanBatch.
type Record struct {
	Seq          uint64
	CreatedAt    time.Time
	OriginStream string
	OriginSeq    uint64
	Payload      []byte
}

// DeadRecord is a copied dead-letter record.
type DeadRecord struct {
	Seq            uint64
	CreatedAt      time.Time
	DeadLetteredAt time.Time
	OriginStream   string
	OriginSeq      uint64
	Payload        []byte
	Reason         string
}

// Counts describes persistent records for one stream.
type Counts struct {
	Tail       uint64
	Pending    uint64
	DeadLetter uint64
	OldestSeq  uint64
	OldestTime time.Time
}

// Root owns one Badger database and all named streams within it.
type Root struct {
	db      *badger.DB
	mu      sync.Mutex
	closed  bool
	streams map[string]*Stream
}

// Open opens a Badger database with synchronous writes forced on.
func Open(dir string) (*Root, error) {
	if dir == "" {
		return nil, errors.New("store: directory is required")
	}
	opts := badger.DefaultOptions(dir).
		WithValueDir(dir).
		WithNumVersionsToKeep(1).
		WithSyncWrites(true).
		WithLogger(nil)
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("store: open %q: %w", dir, err)
	}
	return &Root{db: db, streams: make(map[string]*Stream)}, nil
}

// Stream returns the process-unique handle for name and validates its schema.
func (r *Root) Stream(name string) (*Stream, error) {
	if err := validStreamName(name); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if stream, ok := r.streams[name]; ok {
		return stream, nil
	}
	stream := &Stream{name: name, db: r.db}
	if err := stream.load(); err != nil {
		return nil, err
	}
	r.streams[name] = stream
	return stream, nil
}

// Close closes the shared Badger database.
func (r *Root) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	r.mu.Unlock()
	return r.db.Close()
}

// RunValueLogGC attempts one Badger value-log garbage collection pass.
func (r *Root) RunValueLogGC(discardRatio float64) error {
	err := r.db.RunValueLogGC(discardRatio)
	if errors.Is(err, badger.ErrNoRewrite) {
		return ErrNoRewrite
	}
	return err
}

// Stream is the persistence handle for one named queue.
type Stream struct {
	name string
	db   *badger.DB
	mu   sync.Mutex
	tail uint64

	beforeCommit func(operation string) error
}

// Name returns the stream name.
func (s *Stream) Name() string { return s.name }

func (s *Stream) load() error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey(s.name, schemaMetaKey))
		switch {
		case errors.Is(err, badger.ErrKeyNotFound):
			if err := txn.Set(metaKey(s.name, schemaMetaKey), []byte{schemaVersion}); err != nil {
				return err
			}
		case err != nil:
			return err
		default:
			value, copyErr := item.ValueCopy(nil)
			if copyErr != nil {
				return copyErr
			}
			if len(value) != 1 || value[0] != schemaVersion {
				return fmt.Errorf("%w for stream %q", ErrUnsupportedSchema, s.name)
			}
		}

		item, err = txn.Get(metaKey(s.name, tailMetaKey))
		tailFound := true
		if errors.Is(err, badger.ErrKeyNotFound) {
			s.tail = 0
			tailFound = false
		} else {
			if err != nil {
				return err
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			if len(value) != seqLen {
				return fmt.Errorf("%w: tail for stream %q", ErrCorruptEnvelope, s.name)
			}
			s.tail = binary.BigEndian.Uint64(value)
		}
		for _, prefix := range [][]byte{pendingPrefix(s.name), deadPrefix(s.name)} {
			maximum, found, err := maxStoredSequence(txn, prefix)
			if err != nil {
				return err
			}
			if found && (!tailFound || maximum > s.tail) {
				return fmt.Errorf("%w for stream %q: stored=%d tail=%d", ErrTailRegression, s.name, maximum, s.tail)
			}
		}
		return nil
	})
}

func maxStoredSequence(txn *badger.Txn, prefix []byte) (uint64, bool, error) {
	options := badger.DefaultIteratorOptions
	options.PrefetchValues = false
	options.Reverse = true
	iterator := txn.NewIterator(options)
	defer iterator.Close()
	iterator.Seek(sequenceKey(prefix, math.MaxUint64))
	if !iterator.ValidForPrefix(prefix) {
		return 0, false, nil
	}
	seq, ok := parseSequence(prefix, iterator.Item().Key())
	if !ok {
		return 0, false, ErrCorruptEnvelope
	}
	return seq, true, nil
}

// Append atomically stores one record and advances the durable tail.
func (s *Stream) Append(payload []byte, now time.Time) (Record, error) {
	records, err := s.AppendBatch([][]byte{payload}, now)
	if err != nil {
		return Record{}, err
	}
	return records[0], nil
}

// AppendBatch atomically stores a contiguous batch and advances the durable tail.
func (s *Stream) AppendBatch(payloads [][]byte, now time.Time) ([]Record, error) {
	if len(payloads) == 0 {
		return []Record{}, nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if uint64(len(payloads)) > math.MaxUint64-s.tail {
		return nil, ErrSeqExhausted
	}
	totalSize := 0
	for _, payload := range payloads {
		if len(payload) > maxBatchValueSize-totalSize {
			return nil, ErrBatchTooLarge
		}
		totalSize += len(payload)
	}
	records := make([]Record, len(payloads))
	encoded := make([][]byte, len(payloads))
	for index, payload := range payloads {
		seq := s.tail + uint64(index) + 1
		value, err := encodePending(pendingEnvelope{CreatedAt: now.UTC(), Payload: payload})
		if err != nil {
			return nil, err
		}
		encoded[index] = value
		records[index] = Record{Seq: seq, CreatedAt: now.UTC(), Payload: append([]byte(nil), payload...)}
	}
	newTail := records[len(records)-1].Seq
	err := s.db.Update(func(txn *badger.Txn) error {
		for index, record := range records {
			if err := txn.Set(pendingKey(s.name, record.Seq), encoded[index]); err != nil {
				return err
			}
		}
		if err := txn.Set(metaKey(s.name, tailMetaKey), binary.BigEndian.AppendUint64(nil, newTail)); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			return s.beforeCommit("append")
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store: append to %q: %w", s.name, err)
	}
	s.tail = newTail
	return records, nil
}

// ScanBatch returns at most limit pending records with sequence >= from.
func (s *Stream) ScanBatch(from uint64, limit int) ([]Record, error) {
	if limit <= 0 {
		return nil, ErrInvalidBatchSize
	}
	records := make([]Record, 0, limit)
	prefix := pendingPrefix(s.name)
	err := s.db.View(func(txn *badger.Txn) error {
		options := badger.DefaultIteratorOptions
		options.PrefetchSize = limit
		iterator := txn.NewIterator(options)
		defer iterator.Close()
		for iterator.Seek(pendingKey(s.name, from)); iterator.ValidForPrefix(prefix) && len(records) < limit; iterator.Next() {
			item := iterator.Item()
			seq, ok := parseSequence(prefix, item.Key())
			if !ok {
				return ErrCorruptEnvelope
			}
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			envelope, err := decodePending(value)
			if err != nil {
				return fmt.Errorf("store: decode pending %q/%d: %w", s.name, seq, err)
			}
			records = append(records, Record{
				Seq:          seq,
				CreatedAt:    envelope.CreatedAt,
				OriginStream: envelope.OriginStream,
				OriginSeq:    envelope.OriginSeq,
				Payload:      envelope.Payload,
			})
		}
		return nil
	})
	return records, err
}

// Commit atomically deletes the selected pending records.
func (s *Stream) Commit(seqs []uint64) error {
	if len(seqs) == 0 {
		return nil
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for _, seq := range seqs {
			if _, err := txn.Get(pendingKey(s.name, seq)); errors.Is(err, badger.ErrKeyNotFound) {
				return fmt.Errorf("%w: %s/%d", ErrNotFound, s.name, seq)
			} else if err != nil {
				return err
			}
			if err := txn.Delete(pendingKey(s.name, seq)); err != nil {
				return err
			}
		}
		if s.beforeCommit != nil {
			return s.beforeCommit("commit")
		}
		return nil
	})
}

// DeadLetter atomically replaces one pending record with a dead-letter record.
func (s *Stream) DeadLetter(seq uint64, reason string, now time.Time) error {
	return s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(pendingKey(s.name, seq))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("%w: %s/%d", ErrNotFound, s.name, seq)
		}
		if err != nil {
			return err
		}
		if _, err := txn.Get(deadKey(s.name, seq)); err == nil {
			return fmt.Errorf("%w: %s/%d exists as pending and dead letter", ErrStateConflict, s.name, seq)
		} else if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		pending, err := decodePending(value)
		if err != nil {
			return err
		}
		originStream := pending.OriginStream
		originSeq := pending.OriginSeq
		if originStream == "" {
			originStream = s.name
			originSeq = seq
		}
		dead, err := encodeDead(deadEnvelope{
			CreatedAt:      pending.CreatedAt,
			DeadLetteredAt: now.UTC(),
			OriginStream:   originStream,
			OriginSeq:      originSeq,
			Payload:        pending.Payload,
			Reason:         reason,
		})
		if err != nil {
			return err
		}
		if err := txn.Set(deadKey(s.name, seq), dead); err != nil {
			return err
		}
		if err := txn.Delete(pendingKey(s.name, seq)); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			return s.beforeCommit("dead-letter")
		}
		return nil
	})
}

// Requeue atomically moves a dead-letter record to a new pending sequence.
func (s *Stream) Requeue(seq uint64, now time.Time) (Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.tail == math.MaxUint64 {
		return Record{}, ErrSeqExhausted
	}
	newSeq := s.tail + 1
	var result Record
	err := s.db.Update(func(txn *badger.Txn) error {
		item, err := txn.Get(deadKey(s.name, seq))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("%w: dead letter %s/%d", ErrNotFound, s.name, seq)
		}
		if err != nil {
			return err
		}
		value, err := item.ValueCopy(nil)
		if err != nil {
			return err
		}
		dead, err := decodeDead(value)
		if err != nil {
			return err
		}
		pending := pendingEnvelope{
			CreatedAt:    now.UTC(),
			OriginStream: dead.OriginStream,
			OriginSeq:    dead.OriginSeq,
			Payload:      dead.Payload,
		}
		encoded, err := encodePending(pending)
		if err != nil {
			return err
		}
		if err := txn.Set(pendingKey(s.name, newSeq), encoded); err != nil {
			return err
		}
		if err := txn.Set(metaKey(s.name, tailMetaKey), binary.BigEndian.AppendUint64(nil, newSeq)); err != nil {
			return err
		}
		if err := txn.Delete(deadKey(s.name, seq)); err != nil {
			return err
		}
		if s.beforeCommit != nil {
			return s.beforeCommit("requeue")
		}
		result = Record{
			Seq:          newSeq,
			CreatedAt:    pending.CreatedAt,
			OriginStream: pending.OriginStream,
			OriginSeq:    pending.OriginSeq,
			Payload:      append([]byte(nil), pending.Payload...),
		}
		return nil
	})
	if err != nil {
		return Record{}, err
	}
	s.tail = newSeq
	return result, nil
}

// DeadLetters returns a bounded ordered page of dead-letter records.
func (s *Stream) DeadLetters(from uint64, limit int) ([]DeadRecord, error) {
	if limit <= 0 {
		return nil, ErrInvalidBatchSize
	}
	records := make([]DeadRecord, 0, limit)
	prefix := deadPrefix(s.name)
	err := s.db.View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		for iterator.Seek(deadKey(s.name, from)); iterator.ValidForPrefix(prefix) && len(records) < limit; iterator.Next() {
			seq, ok := parseSequence(prefix, iterator.Item().Key())
			if !ok {
				return ErrCorruptEnvelope
			}
			value, err := iterator.Item().ValueCopy(nil)
			if err != nil {
				return err
			}
			envelope, err := decodeDead(value)
			if err != nil {
				return fmt.Errorf("store: decode dead letter %q/%d: %w", s.name, seq, err)
			}
			records = append(records, DeadRecord{
				Seq:            seq,
				CreatedAt:      envelope.CreatedAt,
				DeadLetteredAt: envelope.DeadLetteredAt,
				OriginStream:   envelope.OriginStream,
				OriginSeq:      envelope.OriginSeq,
				Payload:        envelope.Payload,
				Reason:         envelope.Reason,
			})
		}
		return nil
	})
	return records, err
}

// Counts returns persistent queue counts and oldest pending metadata.
func (s *Stream) Counts() (Counts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	counts := Counts{Tail: s.tail}
	err := s.db.View(func(txn *badger.Txn) error {
		for _, target := range []struct {
			prefix []byte
			count  *uint64
		}{
			{pendingPrefix(s.name), &counts.Pending},
			{deadPrefix(s.name), &counts.DeadLetter},
		} {
			options := badger.DefaultIteratorOptions
			options.PrefetchValues = target.count == &counts.Pending
			iterator := txn.NewIterator(options)
			for iterator.Seek(target.prefix); iterator.ValidForPrefix(target.prefix); iterator.Next() {
				*target.count++
				if target.count == &counts.Pending && counts.OldestSeq == 0 {
					seq, ok := parseSequence(target.prefix, iterator.Item().Key())
					if !ok {
						iterator.Close()
						return ErrCorruptEnvelope
					}
					value, err := iterator.Item().ValueCopy(nil)
					if err != nil {
						iterator.Close()
						return err
					}
					envelope, err := decodePending(value)
					if err != nil {
						iterator.Close()
						return err
					}
					counts.OldestSeq = seq
					counts.OldestTime = envelope.CreatedAt
				}
			}
			iterator.Close()
		}
		return nil
	})
	return counts, err
}

func validStreamName(name string) error {
	if name == "" || len(name) > 100 {
		return fmt.Errorf("%w: %q", ErrInvalidStreamName, name)
	}
	for index := range len(name) {
		character := name[index]
		switch {
		case character >= 'a' && character <= 'z', character >= 'A' && character <= 'Z', character >= '0' && character <= '9':
		case character == '.', character == '_', character == '-':
		default:
			return fmt.Errorf("%w: %q", ErrInvalidStreamName, name)
		}
	}
	return nil
}

// CheckContext reports cancellation before a durable operation starts.
func CheckContext(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}
