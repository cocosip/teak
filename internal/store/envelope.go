package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"
)

const (
	schemaVersion   = 1
	envelopeVersion = 1
	pendingHeader   = 1 + 8 + 8 + 4 + 8
	deadHeader      = 1 + 8 + 8 + 8 + 4 + 8 + 4
)

var (
	// ErrCorruptEnvelope reports malformed persisted record data.
	ErrCorruptEnvelope = errors.New("store: corrupt envelope")
	// ErrUnsupportedEnvelope reports an unknown persisted record version.
	ErrUnsupportedEnvelope = errors.New("store: unsupported envelope version")
	// ErrValueTooLarge reports a value that cannot be represented by the envelope format.
	ErrValueTooLarge = errors.New("store: value too large")
)

type pendingEnvelope struct {
	CreatedAt    time.Time
	OriginStream string
	OriginSeq    uint64
	Payload      []byte
}

type deadEnvelope struct {
	CreatedAt      time.Time
	DeadLetteredAt time.Time
	OriginStream   string
	OriginSeq      uint64
	Payload        []byte
	Reason         string
}

func encodePending(value pendingEnvelope) ([]byte, error) {
	if len(value.OriginStream) > math.MaxUint32 || len(value.Payload) > math.MaxUint32 {
		return nil, ErrValueTooLarge
	}
	out := make([]byte, 0, pendingHeader+len(value.OriginStream)+len(value.Payload))
	out = append(out, envelopeVersion)
	out = binary.BigEndian.AppendUint64(out, uint64(value.CreatedAt.UnixNano()))
	out = binary.BigEndian.AppendUint64(out, value.OriginSeq)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value.OriginStream)))
	out = append(out, value.OriginStream...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(value.Payload)))
	out = append(out, value.Payload...)
	return out, nil
}

// decodePending decodes one pending envelope. The returned payload aliases
// data, so callers must pass a buffer they exclusively own, such as an
// Item.ValueCopy result.
func decodePending(data []byte) (pendingEnvelope, error) {
	if len(data) == 0 {
		return pendingEnvelope{}, ErrCorruptEnvelope
	}
	if data[0] != envelopeVersion {
		return pendingEnvelope{}, fmt.Errorf("%w: %d", ErrUnsupportedEnvelope, data[0])
	}
	reader := envelopeReader{data: data[1:]}
	created, ok := reader.uint64()
	if !ok {
		return pendingEnvelope{}, ErrCorruptEnvelope
	}
	originSeq, ok := reader.uint64()
	if !ok {
		return pendingEnvelope{}, ErrCorruptEnvelope
	}
	origin, ok := reader.string32()
	if !ok {
		return pendingEnvelope{}, ErrCorruptEnvelope
	}
	payload, ok := reader.bytes64()
	if !ok || len(reader.data) != 0 {
		return pendingEnvelope{}, ErrCorruptEnvelope
	}
	return pendingEnvelope{
		CreatedAt:    time.Unix(0, int64(created)).UTC(),
		OriginStream: origin,
		OriginSeq:    originSeq,
		Payload:      payload,
	}, nil
}

func encodeDead(value deadEnvelope) ([]byte, error) {
	if len(value.OriginStream) > math.MaxUint32 || len(value.Payload) > math.MaxUint32 || len(value.Reason) > math.MaxUint32 {
		return nil, ErrValueTooLarge
	}
	out := make([]byte, 0, deadHeader+len(value.OriginStream)+len(value.Payload)+len(value.Reason))
	out = append(out, envelopeVersion)
	out = binary.BigEndian.AppendUint64(out, uint64(value.CreatedAt.UnixNano()))
	out = binary.BigEndian.AppendUint64(out, uint64(value.DeadLetteredAt.UnixNano()))
	out = binary.BigEndian.AppendUint64(out, value.OriginSeq)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value.OriginStream)))
	out = append(out, value.OriginStream...)
	out = binary.BigEndian.AppendUint64(out, uint64(len(value.Payload)))
	out = append(out, value.Payload...)
	out = binary.BigEndian.AppendUint32(out, uint32(len(value.Reason)))
	out = append(out, value.Reason...)
	return out, nil
}

// decodeDead decodes one dead-letter envelope. The returned payload aliases
// data, so callers must pass a buffer they exclusively own, such as an
// Item.ValueCopy result.
func decodeDead(data []byte) (deadEnvelope, error) {
	if len(data) == 0 {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	if data[0] != envelopeVersion {
		return deadEnvelope{}, fmt.Errorf("%w: %d", ErrUnsupportedEnvelope, data[0])
	}
	reader := envelopeReader{data: data[1:]}
	created, ok := reader.uint64()
	if !ok {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	deadAt, ok := reader.uint64()
	if !ok {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	originSeq, ok := reader.uint64()
	if !ok {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	origin, ok := reader.string32()
	if !ok {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	payload, ok := reader.bytes64()
	if !ok {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	reason, ok := reader.string32()
	if !ok || len(reader.data) != 0 {
		return deadEnvelope{}, ErrCorruptEnvelope
	}
	return deadEnvelope{
		CreatedAt:      time.Unix(0, int64(created)).UTC(),
		DeadLetteredAt: time.Unix(0, int64(deadAt)).UTC(),
		OriginStream:   origin,
		OriginSeq:      originSeq,
		Payload:        payload,
		Reason:         reason,
	}, nil
}

type envelopeReader struct {
	data []byte
}

func (r *envelopeReader) uint64() (uint64, bool) {
	if len(r.data) < 8 {
		return 0, false
	}
	value := binary.BigEndian.Uint64(r.data[:8])
	r.data = r.data[8:]
	return value, true
}

func (r *envelopeReader) string32() (string, bool) {
	if len(r.data) < 4 {
		return "", false
	}
	size := binary.BigEndian.Uint32(r.data[:4])
	r.data = r.data[4:]
	if uint64(size) > uint64(len(r.data)) {
		return "", false
	}
	value := string(r.data[:size])
	r.data = r.data[size:]
	return value, true
}

func (r *envelopeReader) bytes64() ([]byte, bool) {
	size, ok := r.uint64()
	if !ok || size > uint64(len(r.data)) {
		return nil, false
	}
	value := r.data[:size]
	r.data = r.data[size:]
	return value, true
}
