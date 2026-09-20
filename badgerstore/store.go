// Package badgerstore implements teak's storage layer on BadgerDB
// (github.com/hypermodeinc/badger/v4). A single Badger instance holds any
// number of named log streams, isolated by key prefix (see keys.go).
package badgerstore

import (
	"errors"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
)

var (
	// ErrInvalidStreamName is returned for stream names outside
	// [A-Za-z0-9._-]{1,100}.
	ErrInvalidStreamName = errors.New("badgerstore: invalid stream name")

	// ErrNotFound is returned by GetMeta when the key does not exist.
	ErrNotFound = errors.New("badgerstore: key not found")

	// ErrStopScan can be returned from a ScanFrom callback to stop iteration
	// early; ScanFrom itself returns nil in that case.
	ErrStopScan = errors.New("badgerstore: stop scan")
)

// Store is the storage contract for one named log stream. All methods are
// safe for concurrent use by multiple goroutines.
type Store interface {
	// Allocate reserves n contiguous sequence numbers and returns the first
	// one. Numbers start at 1. Only the lease high-water mark is persisted,
	// so a crash may leave holes but never reuses a sequence.
	Allocate(n uint64) (uint64, error)

	// Put stores one entry at seq. Under normal operation nothing is ever
	// overwritten, because sequences are never reused.
	Put(seq uint64, data []byte) error

	// BatchPut stores the entries in a single transaction. It fails with
	// badger.ErrTxnTooBig when the batch exceeds Badger's per-transaction
	// limits; callers are expected to chunk large batches themselves.
	// The data slices must not be modified after the call.
	BatchPut(seqs []uint64, datas [][]byte) error

	// ScanFrom calls fn for every stored entry with sequence >= seq, in
	// ascending order. Holes are skipped. The data slice is only valid for
	// the duration of the callback. Returning ErrStopScan from fn stops the
	// scan cleanly.
	ScanFrom(seq uint64, fn func(seq uint64, data []byte) error) error

	// DeleteBelow removes every entry with sequence < seq, committing at
	// most batchLimit deletes per transaction, and returns how many entries
	// were removed. Metadata is never touched.
	DeleteBelow(seq uint64, batchLimit int) (int, error)

	// PutMeta durably stores a metadata value for the stream.
	PutMeta(key string, value []byte) error

	// GetMeta returns the metadata value, or ErrNotFound.
	GetMeta(key string) ([]byte, error)

	// Sync fsyncs the underlying Badger instance (shared by all streams).
	Sync() error
}

// Root owns a single Badger instance. One Root serves any number of streams;
// Stream returns the same *Stream for a given name for the lifetime of the
// Root, so sequence allocation stays unique per namespace.
type Root struct {
	db      *badger.DB
	mu      sync.Mutex
	streams map[string]*Stream
}

// DefaultOptions returns Badger options suited to the log workload. Pass
// overrides on top of it via the With* builders.
func DefaultOptions(dir string) badger.Options {
	return badger.DefaultOptions(dir).
		WithNumVersionsToKeep(1). // entries are never overwritten; meta keys only need their latest version
		WithLogger(nil)           // silence Badger's internal INFO logging; override with your own logger
}

// Open opens (creating if needed) the Badger directory at dir unless opts
// already carries one. The directory is locked: a second process cannot open
// it concurrently.
func Open(dir string, opts badger.Options) (*Root, error) {
	if opts.Dir == "" {
		opts.Dir = dir
	}
	if opts.ValueDir == "" {
		opts.ValueDir = dir
	}
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("badgerstore: open %q: %w", dir, err)
	}
	return &Root{db: db, streams: make(map[string]*Stream)}, nil
}

// Stream returns the named stream, creating it on first use. The name must
// match [A-Za-z0-9._-]{1,100}.
func (r *Root) Stream(name string) (*Stream, error) {
	if err := validStreamName(name); err != nil {
		return nil, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.streams[name]; ok {
		return s, nil
	}

	s := &Stream{
		name:       name,
		dataPrefix: dataPrefix(name),
		db:         r.db,
	}
	high, err := loadSeqLease(r.db, s)
	if err != nil {
		return nil, err
	}
	s.alloc = newSeqAllocator(func(leaseHigh uint64) error {
		return s.PutMeta(seqLeaseMetaKey, encodeSeqLease(leaseHigh))
	}, defaultSeqLease, high)

	r.streams[name] = s
	return s, nil
}

// Close closes the shared Badger instance and every stream derived from it.
func (r *Root) Close() error {
	return r.db.Close()
}

// Stream is one named log stream; it implements Store.
type Stream struct {
	name       string
	dataPrefix []byte
	db         *badger.DB
	alloc      *seqAllocator
}

var _ Store = (*Stream)(nil)

// Name returns the stream's name.
func (s *Stream) Name() string { return s.name }

// Allocate reserves n contiguous sequence numbers; see Store.
func (s *Stream) Allocate(n uint64) (uint64, error) {
	return s.alloc.next(n)
}

// Put stores one entry; see Store.
func (s *Stream) Put(seq uint64, data []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(dataKey(s.name, seq), data)
	})
}

// BatchPut stores the entries in one transaction; see Store.
func (s *Stream) BatchPut(seqs []uint64, datas [][]byte) error {
	if len(seqs) != len(datas) {
		return errors.New("badgerstore: BatchPut length mismatch")
	}
	return s.db.Update(func(txn *badger.Txn) error {
		for i, seq := range seqs {
			if err := txn.Set(dataKey(s.name, seq), datas[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// ScanFrom iterates entries from seq onward; see Store.
func (s *Stream) ScanFrom(seq uint64, fn func(seq uint64, data []byte) error) error {
	return s.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(dataKey(s.name, seq)); it.ValidForPrefix(s.dataPrefix); it.Next() {
			item := it.Item()
			isq, ok := parseDataSeq(s.name, item.Key())
			if !ok {
				continue
			}
			err := item.Value(func(v []byte) error {
				return fn(isq, v)
			})
			if err != nil {
				if errors.Is(err, ErrStopScan) {
					return nil
				}
				return err
			}
		}
		return nil
	})
}

// DeleteBelow removes entries older than the watermark; see Store.
func (s *Stream) DeleteBelow(seq uint64, batchLimit int) (int, error) {
	if batchLimit <= 0 {
		batchLimit = 1
	}
	removed := 0
	chunk := make([][]byte, 0, batchLimit)
	flush := func() error {
		if len(chunk) == 0 {
			return nil
		}
		keys := chunk
		chunk = make([][]byte, 0, batchLimit)
		if err := s.db.Update(func(txn *badger.Txn) error {
			for _, k := range keys {
				if err := txn.Delete(k); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}
		removed += len(keys)
		return nil
	}

	err := s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = false // keys only
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(s.dataPrefix); it.ValidForPrefix(s.dataPrefix); it.Next() {
			isq, ok := parseDataSeq(s.name, it.Item().Key())
			if !ok {
				continue
			}
			if isq >= seq {
				return nil // ascending order: nothing below left
			}
			chunk = append(chunk, it.Item().KeyCopy(nil))
			if len(chunk) >= batchLimit {
				if err := flush(); err != nil {
					return err
				}
			}
		}
		return nil
	})
	if err != nil {
		return removed, err
	}
	if err := flush(); err != nil {
		return removed, err
	}
	return removed, nil
}

// PutMeta durably stores a metadata value; see Store.
func (s *Stream) PutMeta(key string, value []byte) error {
	return s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(metaKey(s.name, key), value)
	})
}

// GetMeta reads a metadata value; see Store.
func (s *Stream) GetMeta(key string) ([]byte, error) {
	return s.getMeta(s.db, key)
}

func (s *Stream) getMeta(db *badger.DB, key string) ([]byte, error) {
	var out []byte
	err := db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(metaKey(s.name, key))
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		out, err = item.ValueCopy(nil)
		return err
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Sync fsyncs the shared Badger instance; see Store.
func (s *Stream) Sync() error {
	return s.db.Sync()
}

func validStreamName(name string) error {
	if name == "" || len(name) > 100 {
		return fmt.Errorf("%w: %q", ErrInvalidStreamName, name)
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		switch {
		case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9':
		case c == '.', c == '_', c == '-':
		default:
			return fmt.Errorf("%w: %q", ErrInvalidStreamName, name)
		}
	}
	return nil
}

func loadSeqLease(db *badger.DB, s *Stream) (uint64, error) {
	v, err := s.getMeta(db, seqLeaseMetaKey)
	if errors.Is(err, ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	high, ok := decodeSeqLease(v)
	if !ok {
		return 0, fmt.Errorf("badgerstore: corrupt seq lease in stream %q", s.name)
	}
	return high, nil
}
