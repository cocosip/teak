// Package teak provides an embedded durable at-least-once work queue.
package teak

import (
	"context"
	"errors"
	"time"
)

var (
	// ErrClosed reports an operation after a log or factory started closing.
	ErrClosed = errors.New("teak: closed")
	// ErrInvalidOptions reports unsupported queue configuration.
	ErrInvalidOptions = errors.New("teak: invalid options")
	// ErrInvalidName reports an unsupported log name.
	ErrInvalidName = errors.New("teak: invalid log name")
	// ErrInvalidCount reports a non-positive read or listing count.
	ErrInvalidCount = errors.New("teak: invalid count")
	// ErrInvalidDelivery reports a delivery from another log or without a valid receipt.
	ErrInvalidDelivery = errors.New("teak: invalid delivery")
	// ErrInvalidDeadLetter reports a dead letter from another log or without a valid identity.
	ErrInvalidDeadLetter = errors.New("teak: invalid dead letter")
	// ErrStaleDelivery reports an expired or already completed delivery.
	ErrStaleDelivery = errors.New("teak: stale delivery")
	// ErrInvalidExtension reports a non-positive lease extension.
	ErrInvalidExtension = errors.New("teak: invalid lease extension")
	// ErrNotFound reports a missing pending or dead-letter record.
	ErrNotFound = errors.New("teak: record not found")
	// ErrBatchTooLarge reports a write batch beyond the atomic transaction limit.
	ErrBatchTooLarge = errors.New("teak: batch too large")
	// ErrCorruptStorage reports inconsistent metadata or an unreadable persisted envelope.
	ErrCorruptStorage = errors.New("teak: corrupt storage")
)

// Position identifies one sequence in a named log.
type Position struct {
	Seq uint64
}

// Delivery is a process-local lease for one durable pending record.
type Delivery struct {
	Position     Position
	Payload      []byte
	Attempt      uint32
	Deadline     time.Time
	OriginStream string
	OriginSeq    uint64

	receipt [16]byte
	owner   *logStream
}

// DeadLetter describes one persistent dead-letter record.
type DeadLetter struct {
	Position       Position
	Payload        []byte
	Reason         string
	CreatedAt      time.Time
	DeadLetteredAt time.Time
	OriginStream   string
	OriginSeq      uint64

	owner       *logStream
	identitySeq uint64
}

// Stats combines persistent record counts with process-local delivery state.
type Stats struct {
	DurableTail        uint64
	Pending            uint64
	Ready              uint64
	InFlight           uint64
	Retry              uint64
	DeadLetter         uint64
	Writes             uint64
	Deliveries         uint64
	Commits            uint64
	Retries            uint64
	LeaseExpirations   uint64
	DuplicateAttempts  uint64
	BackpressureEvents uint64
	StorageErrors      uint64
	OldestPendingSeq   uint64
	OldestPendingAge   time.Duration
}

// FactoryStats reports optional value-log maintenance activity.
type FactoryStats struct {
	ValueLogGCRuns      uint64
	ValueLogGCRewrites  uint64
	ValueLogGCNoRewrite uint64
	ValueLogGCErrors    uint64
	MaintenancePanics   uint64
}

// Factory owns one Badger database and its named logs.
type Factory interface {
	Open(ctx context.Context, name string) (Log, error)
	RunMaintenance(ctx context.Context) error
	Stats() FactoryStats
	Close(ctx context.Context) error
}

// Log is a named competing-consumer work queue.
type Log interface {
	Name() string
	Write(ctx context.Context, data []byte) (Position, error)
	BatchWrite(ctx context.Context, data [][]byte) ([]Position, error)
	Read(ctx context.Context, count int) ([]Delivery, error)
	Commit(ctx context.Context, deliveries ...Delivery) error
	Retry(ctx context.Context, deliveries ...Delivery) error
	Extend(ctx context.Context, extension time.Duration, deliveries ...Delivery) error
	DeadLetter(ctx context.Context, delivery Delivery, reason string) error
	DeadLetters(ctx context.Context, from Position, count int) ([]DeadLetter, error)
	Requeue(ctx context.Context, deadLetter DeadLetter) (Position, error)
	Stats() (Stats, error)
	Close(ctx context.Context) error
}
