package teak

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cocosip/teak/internal/dispatch"
	"github.com/cocosip/teak/internal/store"
)

type logStream struct {
	store      *store.Stream
	dispatcher *dispatch.Dispatcher
	logger     *slog.Logger

	lifeMu  sync.Mutex
	life    *sync.Cond
	closing bool
	active  int

	writes        atomic.Uint64
	commits       atomic.Uint64
	storageErrors atomic.Uint64
}

var _ Log = (*logStream)(nil)

func newLogStream(persistence *store.Stream, scheduler *dispatch.Dispatcher, logger *slog.Logger) *logStream {
	stream := &logStream{store: persistence, dispatcher: scheduler, logger: logger}
	stream.life = sync.NewCond(&stream.lifeMu)
	return stream
}

func (l *logStream) Name() string { return l.store.Name() }

func (l *logStream) Write(ctx context.Context, data []byte) (Position, error) {
	if err := l.begin(ctx); err != nil {
		return Position{}, err
	}
	defer l.end()
	record, err := l.store.Append(data, time.Now())
	if err != nil {
		return Position{}, l.storageError("write", 0, err)
	}
	l.writes.Add(1)
	l.dispatcher.Notify()
	return Position{Seq: record.Seq}, nil
}

func (l *logStream) BatchWrite(ctx context.Context, data [][]byte) ([]Position, error) {
	if err := l.begin(ctx); err != nil {
		return nil, err
	}
	defer l.end()
	records, err := l.store.AppendBatch(data, time.Now())
	if err != nil {
		return nil, l.storageError("batch-write", 0, err)
	}
	positions := make([]Position, len(records))
	for index, record := range records {
		positions[index] = Position{Seq: record.Seq}
	}
	l.writes.Add(uint64(len(records)))
	if len(records) > 0 {
		l.dispatcher.Notify()
	}
	return positions, nil
}

func (l *logStream) Read(ctx context.Context, count int) ([]Delivery, error) {
	if err := l.begin(ctx); err != nil {
		return nil, err
	}
	defer l.end()
	internal, err := l.dispatcher.Read(ctx, count)
	if err != nil {
		return nil, mapError(err)
	}
	deliveries := make([]Delivery, len(internal))
	for index, delivery := range internal {
		deliveries[index] = l.wrap(delivery)
	}
	return deliveries, nil
}

func (l *logStream) Commit(ctx context.Context, deliveries ...Delivery) error {
	if err := l.begin(ctx); err != nil {
		return err
	}
	defer l.end()
	internal, err := l.unwrap(deliveries)
	if err != nil {
		return err
	}
	err = l.dispatcher.Commit(internal, func(seqs []uint64) error {
		if err := l.store.Commit(seqs); err != nil {
			return l.storageError("commit", firstSequence(seqs), err)
		}
		return nil
	})
	if err != nil {
		return mapError(err)
	}
	l.commits.Add(uint64(len(deliveries)))
	return nil
}

func (l *logStream) Retry(ctx context.Context, deliveries ...Delivery) error {
	if err := l.begin(ctx); err != nil {
		return err
	}
	defer l.end()
	internal, err := l.unwrap(deliveries)
	if err != nil {
		return err
	}
	return mapError(l.dispatcher.Retry(internal))
}

func (l *logStream) Extend(ctx context.Context, extension time.Duration, deliveries ...Delivery) error {
	if err := l.begin(ctx); err != nil {
		return err
	}
	defer l.end()
	internal, err := l.unwrap(deliveries)
	if err != nil {
		return err
	}
	return mapError(l.dispatcher.Extend(extension, internal))
}

func (l *logStream) DeadLetter(ctx context.Context, delivery Delivery, reason string) error {
	if err := l.begin(ctx); err != nil {
		return err
	}
	defer l.end()
	internal, err := l.unwrap([]Delivery{delivery})
	if err != nil {
		return err
	}
	err = l.dispatcher.DeadLetter(internal[0], func(seq uint64) error {
		if err := l.store.DeadLetter(seq, reason, time.Now()); err != nil {
			return l.storageError("dead-letter", seq, err)
		}
		return nil
	})
	return mapError(err)
}

func (l *logStream) DeadLetters(ctx context.Context, from Position, count int) ([]DeadLetter, error) {
	if err := l.begin(ctx); err != nil {
		return nil, err
	}
	defer l.end()
	if count <= 0 {
		return nil, ErrInvalidCount
	}
	records, err := l.store.DeadLetters(from.Seq, count)
	if err != nil {
		return nil, l.storageError("list-dead-letters", from.Seq, mapError(err))
	}
	result := make([]DeadLetter, len(records))
	for index, record := range records {
		result[index] = DeadLetter{
			Position:       Position{Seq: record.Seq},
			Payload:        append([]byte(nil), record.Payload...),
			Reason:         record.Reason,
			CreatedAt:      record.CreatedAt,
			DeadLetteredAt: record.DeadLetteredAt,
			OriginStream:   record.OriginStream,
			OriginSeq:      record.OriginSeq,
			owner:          l,
		}
	}
	return result, nil
}

func (l *logStream) Requeue(ctx context.Context, deadLetter DeadLetter) (Position, error) {
	if err := l.begin(ctx); err != nil {
		return Position{}, err
	}
	defer l.end()
	if deadLetter.owner != l {
		return Position{}, ErrInvalidDeadLetter
	}
	record, err := l.store.Requeue(deadLetter.Position.Seq, time.Now())
	if err != nil {
		return Position{}, l.storageError("requeue", deadLetter.Position.Seq, mapError(err))
	}
	l.dispatcher.Notify()
	return Position{Seq: record.Seq}, nil
}

func (l *logStream) Stats() (Stats, error) {
	if err := l.begin(context.Background()); err != nil {
		return Stats{}, err
	}
	defer l.end()
	persistent, err := l.store.Counts()
	if err != nil {
		return Stats{}, l.storageError("stats", 0, err)
	}
	local := l.dispatcher.Snapshot()
	stats := Stats{
		DurableTail:        persistent.Tail,
		Pending:            persistent.Pending,
		Ready:              local.Ready,
		InFlight:           local.InFlight,
		Retry:              local.Retry,
		DeadLetter:         persistent.DeadLetter,
		Writes:             l.writes.Load(),
		Deliveries:         local.Deliveries,
		Commits:            l.commits.Load(),
		Retries:            local.Retries,
		LeaseExpirations:   local.LeaseExpirations,
		DuplicateAttempts:  local.DuplicateAttempts,
		BackpressureEvents: local.Backpressure,
		StorageErrors:      l.storageErrors.Load(),
		OldestPendingSeq:   persistent.OldestSeq,
	}
	if !persistent.OldestTime.IsZero() {
		stats.OldestPendingAge = time.Since(persistent.OldestTime)
		if stats.OldestPendingAge < 0 {
			stats.OldestPendingAge = 0
		}
	}
	return stats, nil
}

func (l *logStream) Close(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	return l.close()
}

func (l *logStream) close() error {
	l.lifeMu.Lock()
	if l.closing {
		for l.active > 0 {
			l.life.Wait()
		}
		l.lifeMu.Unlock()
		return nil
	}
	l.closing = true
	l.dispatcher.Close()
	for l.active > 0 {
		l.life.Wait()
	}
	l.lifeMu.Unlock()
	return nil
}

func (l *logStream) begin(ctx context.Context) error {
	if err := checkContext(ctx); err != nil {
		return err
	}
	l.lifeMu.Lock()
	defer l.lifeMu.Unlock()
	if l.closing {
		return ErrClosed
	}
	l.active++
	return nil
}

func (l *logStream) end() {
	l.lifeMu.Lock()
	l.active--
	if l.active == 0 {
		l.life.Broadcast()
	}
	l.lifeMu.Unlock()
}

func (l *logStream) wrap(delivery dispatch.Delivery) Delivery {
	return Delivery{
		Position:     Position{Seq: delivery.Record.Seq},
		Payload:      append([]byte(nil), delivery.Record.Payload...),
		Attempt:      delivery.Attempt,
		Deadline:     delivery.Deadline,
		OriginStream: delivery.Record.OriginStream,
		OriginSeq:    delivery.Record.OriginSeq,
		receipt:      delivery.Receipt,
		owner:        l,
	}
}

func (l *logStream) unwrap(deliveries []Delivery) ([]dispatch.Delivery, error) {
	internal := make([]dispatch.Delivery, len(deliveries))
	for index, delivery := range deliveries {
		if delivery.owner != l {
			return nil, ErrInvalidDelivery
		}
		internal[index] = dispatch.Delivery{
			Record:  store.Record{Seq: delivery.Position.Seq},
			Receipt: delivery.receipt,
			Attempt: delivery.Attempt,
		}
	}
	return internal, nil
}

func (l *logStream) storageError(operation string, seq uint64, err error) error {
	l.storageErrors.Add(1)
	if l.logger != nil {
		attributes := []any{"operation", operation, "stream", l.Name(), "error", err}
		if seq != 0 {
			attributes = append(attributes, "sequence", seq)
		}
		l.logger.Error("teak storage operation failed", attributes...)
	}
	return err
}

func firstSequence(seqs []uint64) uint64 {
	if len(seqs) == 0 {
		return 0
	}
	return seqs[0]
}
