// Package typed provides codec-backed generic wrappers for Teak logs.
package typed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/cocosip/teak"
)

// ErrCodecPanic reports a panic raised by a caller-provided codec.
var ErrCodecPanic = errors.New("typed: codec panic")

// Codec converts typed values to and from the byte-oriented Teak API.
type Codec[T any] interface {
	Encode(value T) ([]byte, error)
	Decode(data []byte) (T, error)
}

// JSONCodec encodes values with encoding/json.
type JSONCodec[T any] struct{}

// CodecPanicError describes a recovered codec panic without acknowledging queue data.
type CodecPanicError struct {
	Operation string
	Value     any
	Stack     []byte
}

func (e *CodecPanicError) Error() string {
	return fmt.Sprintf("%v during %s: %v", ErrCodecPanic, e.Operation, e.Value)
}

// Unwrap returns ErrCodecPanic.
func (e *CodecPanicError) Unwrap() error { return ErrCodecPanic }

// Encode marshals value as JSON.
func (JSONCodec[T]) Encode(value T) ([]byte, error) { return json.Marshal(value) }

// Decode unmarshals one JSON value.
func (JSONCodec[T]) Decode(data []byte) (T, error) {
	var value T
	err := json.Unmarshal(data, &value)
	return value, err
}

// Delivery combines a decoded value with its queue metadata.
type Delivery[T any] struct {
	Position     teak.Position
	Value        T
	Attempt      uint32
	Deadline     time.Time
	OriginStream string
	OriginSeq    uint64

	raw teak.Delivery
}

// DeadLetter combines a decoded value with persistent dead-letter metadata.
type DeadLetter[T any] struct {
	Position       teak.Position
	Value          T
	Reason         string
	CreatedAt      time.Time
	DeadLetteredAt time.Time
	OriginStream   string
	OriginSeq      uint64

	raw teak.DeadLetter
}

// DecodeError retains every raw delivery from a failed read so callers can retry or dead-letter it.
type DecodeError struct {
	Deliveries []teak.Delivery
	Index      int
	Err        error
}

func (e *DecodeError) Error() string {
	return fmt.Sprintf("typed: decode delivery %d: %v", e.Index, e.Err)
}

// Unwrap returns the codec error.
func (e *DecodeError) Unwrap() error { return e.Err }

// DeadLetterDecodeError retains a raw dead-letter page that failed to decode.
type DeadLetterDecodeError struct {
	DeadLetters []teak.DeadLetter
	Index       int
	Err         error
}

func (e *DeadLetterDecodeError) Error() string {
	return fmt.Sprintf("typed: decode dead letter %d: %v", e.Index, e.Err)
}

// Unwrap returns the codec error.
func (e *DeadLetterDecodeError) Unwrap() error { return e.Err }

// Log applies a codec while delegating queue semantics to a byte log.
type Log[T any] struct {
	raw   teak.Log
	codec Codec[T]
}

// New wraps a byte log with codec.
func New[T any](raw teak.Log, codec Codec[T]) (*Log[T], error) {
	if raw == nil || codec == nil {
		return nil, errors.New("typed: raw log and codec are required")
	}
	return &Log[T]{raw: raw, codec: codec}, nil
}

// JSON wraps a byte log with the standard JSON codec.
func JSON[T any](raw teak.Log) *Log[T] {
	log, err := New[T](raw, JSONCodec[T]{})
	if err != nil {
		panic(err)
	}
	return log
}

// Raw returns the underlying byte log for operational handling of DecodeError deliveries.
func (l *Log[T]) Raw() teak.Log { return l.raw }

// Name returns the underlying log name.
func (l *Log[T]) Name() string { return l.raw.Name() }

// Write encodes and durably appends one value.
func (l *Log[T]) Write(ctx context.Context, value T) (teak.Position, error) {
	data, err := safeEncode(l.codec, value)
	if err != nil {
		return teak.Position{}, fmt.Errorf("typed: encode: %w", err)
	}
	return l.raw.Write(ctx, data)
}

// BatchWrite encodes all values before atomically appending the batch.
func (l *Log[T]) BatchWrite(ctx context.Context, values []T) ([]teak.Position, error) {
	data := make([][]byte, len(values))
	for index, value := range values {
		encoded, err := safeEncode(l.codec, value)
		if err != nil {
			return nil, fmt.Errorf("typed: encode value %d: %w", index, err)
		}
		data[index] = encoded
	}
	return l.raw.BatchWrite(ctx, data)
}

// Read leases and decodes up to count values.
func (l *Log[T]) Read(ctx context.Context, count int) ([]Delivery[T], error) {
	raw, err := l.raw.Read(ctx, count)
	if err != nil {
		return nil, err
	}
	deliveries := make([]Delivery[T], len(raw))
	for index, delivery := range raw {
		value, decodeErr := safeDecode(l.codec, delivery.Payload)
		if decodeErr != nil {
			return nil, &DecodeError{Deliveries: raw, Index: index, Err: decodeErr}
		}
		deliveries[index] = Delivery[T]{
			Position:     delivery.Position,
			Value:        value,
			Attempt:      delivery.Attempt,
			Deadline:     delivery.Deadline,
			OriginStream: delivery.OriginStream,
			OriginSeq:    delivery.OriginSeq,
			raw:          delivery,
		}
	}
	return deliveries, nil
}

// Commit durably completes deliveries.
func (l *Log[T]) Commit(ctx context.Context, deliveries ...Delivery[T]) error {
	return l.raw.Commit(ctx, unwrap(deliveries)...)
}

// Retry schedules delayed redelivery.
func (l *Log[T]) Retry(ctx context.Context, deliveries ...Delivery[T]) error {
	return l.raw.Retry(ctx, unwrap(deliveries)...)
}

// Extend extends valid delivery leases.
func (l *Log[T]) Extend(ctx context.Context, extension time.Duration, deliveries ...Delivery[T]) error {
	return l.raw.Extend(ctx, extension, unwrap(deliveries)...)
}

// DeadLetter atomically transfers a delivery out of the active queue.
func (l *Log[T]) DeadLetter(ctx context.Context, delivery Delivery[T], reason string) error {
	return l.raw.DeadLetter(ctx, delivery.raw, reason)
}

// DeadLetters returns and decodes a persistent dead-letter page.
func (l *Log[T]) DeadLetters(ctx context.Context, from teak.Position, count int) ([]DeadLetter[T], error) {
	raw, err := l.raw.DeadLetters(ctx, from, count)
	if err != nil {
		return nil, err
	}
	deadLetters := make([]DeadLetter[T], len(raw))
	for index, deadLetter := range raw {
		value, decodeErr := safeDecode(l.codec, deadLetter.Payload)
		if decodeErr != nil {
			return nil, &DeadLetterDecodeError{DeadLetters: raw, Index: index, Err: decodeErr}
		}
		deadLetters[index] = DeadLetter[T]{
			Position:       deadLetter.Position,
			Value:          value,
			Reason:         deadLetter.Reason,
			CreatedAt:      deadLetter.CreatedAt,
			DeadLetteredAt: deadLetter.DeadLetteredAt,
			OriginStream:   deadLetter.OriginStream,
			OriginSeq:      deadLetter.OriginSeq,
			raw:            deadLetter,
		}
	}
	return deadLetters, nil
}

// Requeue atomically restores a dead letter under a new pending sequence.
func (l *Log[T]) Requeue(ctx context.Context, deadLetter DeadLetter[T]) (teak.Position, error) {
	return l.raw.Requeue(ctx, deadLetter.raw)
}

// Stats returns the underlying queue statistics.
func (l *Log[T]) Stats() (teak.Stats, error) { return l.raw.Stats() }

// Close closes the underlying log.
func (l *Log[T]) Close(ctx context.Context) error { return l.raw.Close(ctx) }

func unwrap[T any](deliveries []Delivery[T]) []teak.Delivery {
	raw := make([]teak.Delivery, len(deliveries))
	for index, delivery := range deliveries {
		raw[index] = delivery.raw
	}
	return raw
}

func safeEncode[T any](codec Codec[T], value T) (data []byte, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			data = nil
			err = &CodecPanicError{Operation: "encode", Value: recovered, Stack: debug.Stack()}
		}
	}()
	return codec.Encode(value)
}

func safeDecode[T any](codec Codec[T], data []byte) (value T, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			var zero T
			value = zero
			err = &CodecPanicError{Operation: "decode", Value: recovered, Stack: debug.Stack()}
		}
	}()
	return codec.Decode(data)
}
