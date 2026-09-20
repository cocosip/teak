package typed_test

import (
	"context"
	"errors"
	"testing"

	"github.com/cocosip/teak"
	"github.com/cocosip/teak/typed"
)

const firstName = "one"

type job struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type panicCodec struct {
	decode bool
}

func (c panicCodec) Encode(job) ([]byte, error) {
	if !c.decode {
		panic("encode failed")
	}
	return []byte("encoded"), nil
}

func (c panicCodec) Decode([]byte) (job, error) {
	if c.decode {
		panic("decode failed")
	}
	return job{}, nil
}

func TestJSONRoundTrip(t *testing.T) {
	factory, err := teak.New(teak.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := factory.Close(context.Background()); err != nil {
			t.Errorf("close factory: %v", err)
		}
	}()
	raw, err := factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	log := typed.JSON[job](raw)
	positions, err := log.BatchWrite(t.Context(), []job{{ID: 1, Name: firstName}, {ID: 2, Name: "two"}})
	if err != nil || len(positions) != 2 {
		t.Fatalf("write: %+v, %v", positions, err)
	}
	deliveries, err := log.Read(t.Context(), 2)
	if err != nil || deliveries[0].Value.Name != firstName || deliveries[1].Value.ID != 2 {
		t.Fatalf("read: %+v, %v", deliveries, err)
	}
	if err := log.Commit(t.Context(), deliveries...); err != nil {
		t.Fatal(err)
	}
}

func TestDecodeErrorRetainsRawDelivery(t *testing.T) {
	factory, err := teak.New(teak.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factory.Close(context.Background()) }()
	raw, err := factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := raw.Write(t.Context(), []byte("not-json")); err != nil {
		t.Fatal(err)
	}
	log := typed.JSON[job](raw)
	_, err = log.Read(t.Context(), 1)
	var decodeErr *typed.DecodeError
	if !errors.As(err, &decodeErr) || len(decodeErr.Deliveries) != 1 || decodeErr.Index != 0 {
		t.Fatalf("decode error = %#v", err)
	}
	if err := log.Raw().DeadLetter(t.Context(), decodeErr.Deliveries[0], "invalid JSON"); err != nil {
		t.Fatal(err)
	}
	stats, err := log.Stats()
	if err != nil || stats.Pending != 0 || stats.DeadLetter != 1 {
		t.Fatalf("stats: %+v, %v", stats, err)
	}
}

func TestTypedDeadLetterAndRequeue(t *testing.T) {
	factory, err := teak.New(teak.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factory.Close(context.Background()) }()
	raw, err := factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	log := typed.JSON[job](raw)
	if _, err := log.Write(t.Context(), job{ID: 1, Name: firstName}); err != nil {
		t.Fatal(err)
	}
	deliveries, err := log.Read(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := log.DeadLetter(t.Context(), deliveries[0], "failed"); err != nil {
		t.Fatal(err)
	}
	deadLetters, err := log.DeadLetters(t.Context(), teak.Position{}, 10)
	if err != nil || len(deadLetters) != 1 || deadLetters[0].Value.Name != firstName {
		t.Fatalf("dead letters: %+v, %v", deadLetters, err)
	}
	position, err := log.Requeue(t.Context(), deadLetters[0])
	if err != nil || position.Seq != 2 {
		t.Fatalf("requeue: %+v, %v", position, err)
	}
}

func TestCodecPanicsBecomeRecoverableErrors(t *testing.T) {
	factory, err := teak.New(teak.DefaultOptions(t.TempDir()))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = factory.Close(context.Background()) }()
	raw, err := factory.Open(t.Context(), "jobs")
	if err != nil {
		t.Fatal(err)
	}
	encodeLog, err := typed.New[job](raw, panicCodec{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := encodeLog.Write(t.Context(), job{}); !errors.Is(err, typed.ErrCodecPanic) {
		t.Fatalf("encode panic error = %v", err)
	}
	stats, err := raw.Stats()
	if err != nil || stats.Pending != 0 {
		t.Fatalf("encode panic persisted data: %+v, %v", stats, err)
	}
	if _, err := raw.Write(t.Context(), []byte("payload")); err != nil {
		t.Fatal(err)
	}
	decodeLog, err := typed.New[job](raw, panicCodec{decode: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLog.Read(t.Context(), 1); !errors.Is(err, typed.ErrCodecPanic) {
		t.Fatalf("decode panic error = %v", err)
	}
	stats, err = raw.Stats()
	if err != nil || stats.Pending != 1 || stats.InFlight != 1 {
		t.Fatalf("decode panic lost delivery: %+v, %v", stats, err)
	}
}
