// Package dispatch schedules pending records for at-least-once delivery.
package dispatch

import (
	"container/heap"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/cocosip/teak/internal/store"
)

var (
	// ErrClosed reports an operation on a closed dispatcher.
	ErrClosed = errors.New("dispatch: closed")
	// ErrInvalidCount reports a non-positive read count.
	ErrInvalidCount = errors.New("dispatch: invalid read count")
	// ErrInvalidConfig reports invalid queue or lease configuration.
	ErrInvalidConfig = errors.New("dispatch: invalid configuration")
	// ErrStaleDelivery reports a receipt that is unknown, expired, or already completed.
	ErrStaleDelivery = errors.New("dispatch: stale delivery")
	// ErrInvalidExtension reports a non-positive lease extension.
	ErrInvalidExtension = errors.New("dispatch: invalid lease extension")
)

// Source provides bounded ordered access to durable pending records.
type Source interface {
	ScanBatch(from uint64, limit int) ([]store.Record, error)
}

// Config controls bounded scheduling and delivery timing.
type Config struct {
	PrefetchCapacity  int
	MaxInFlight       int
	VisibilityTimeout time.Duration
	RetryInitial      time.Duration
	RetryMax          time.Duration
	RetryMultiplier   float64
}

// Receipt is an unguessable process-local delivery identity.
type Receipt [16]byte

// Delivery is one leased record returned to the queue facade.
type Delivery struct {
	Record   store.Record
	Receipt  Receipt
	Attempt  uint32
	Deadline time.Time
}

// Stats reports process-local dispatcher state and counters.
type Stats struct {
	Ready             uint64
	Retry             uint64
	InFlight          uint64
	Deliveries        uint64
	Retries           uint64
	LeaseExpirations  uint64
	DuplicateAttempts uint64
	Backpressure      uint64
}

type state uint8

const receiptCollisionLimit = 16

const (
	stateFresh state = iota + 1
	stateRetry
	stateInFlight
)

type queuedRecord struct {
	record  store.Record
	attempt uint32
	readyAt time.Time
	index   int
}

type flight struct {
	record   store.Record
	receipt  Receipt
	attempt  uint32
	deadline time.Time
}

// Dispatcher owns the bounded process-local state for one stream.
type Dispatcher struct {
	source Source
	config Config

	mu          sync.Mutex
	scanMu      sync.Mutex
	closed      bool
	cursor      uint64
	scanDone    bool
	fresh       []*queuedRecord
	retries     retryHeap
	inFlight    map[Receipt]flight
	states      map[uint64]state
	preferRetry bool
	signal      chan struct{}
	stats       Stats

	now        func() time.Time
	newReceipt func() (Receipt, error)
}

// New constructs a dispatcher without starting background goroutines.
func New(source Source, config Config) (*Dispatcher, error) {
	if source == nil || config.PrefetchCapacity <= 0 || config.MaxInFlight <= 0 ||
		config.VisibilityTimeout <= 0 || config.RetryInitial < 0 || config.RetryMax < config.RetryInitial ||
		config.RetryMultiplier < 1 {
		return nil, ErrInvalidConfig
	}
	return &Dispatcher{
		source:     source,
		config:     config,
		cursor:     1,
		fresh:      make([]*queuedRecord, 0, config.PrefetchCapacity),
		retries:    make(retryHeap, 0),
		inFlight:   make(map[Receipt]flight),
		states:     make(map[uint64]state),
		signal:     make(chan struct{}, 1),
		now:        time.Now,
		newReceipt: randomReceipt,
	}, nil
}

// Notify wakes blocked readers after durable records are appended or requeued.
func (d *Dispatcher) Notify() {
	d.mu.Lock()
	d.notifyLocked()
	d.mu.Unlock()
}

// Read blocks until it can lease at least one record, the context ends, or the dispatcher closes.
func (d *Dispatcher) Read(ctx context.Context, count int) ([]Delivery, error) {
	if count <= 0 {
		return nil, ErrInvalidCount
	}
	for {
		d.mu.Lock()
		if d.closed {
			d.mu.Unlock()
			return nil, ErrClosed
		}
		now := d.now()
		d.expireLocked(now)
		deliveries, err := d.takeLocked(count, now)
		if err != nil {
			d.mu.Unlock()
			return nil, err
		}
		if len(deliveries) > 0 {
			d.mu.Unlock()
			return deliveries, nil
		}
		wakeAt := d.nextWakeLocked()
		if len(d.inFlight) >= d.config.MaxInFlight {
			d.stats.Backpressure++
		}
		d.mu.Unlock()

		progress, err := d.fillFresh()
		if err != nil {
			return nil, err
		}
		if progress {
			continue
		}
		if err := d.wait(ctx, wakeAt); err != nil {
			return nil, err
		}
	}
}

// Commit validates deliveries, persists their deletion, then releases their in-flight state.
func (d *Dispatcher) Commit(deliveries []Delivery, persist func([]uint64) error) error {
	if len(deliveries) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.expireLocked(d.now())
	seqs, err := d.validateLocked(deliveries)
	if err != nil {
		return err
	}
	if err := persist(seqs); err != nil {
		return err
	}
	for _, delivery := range deliveries {
		delete(d.inFlight, delivery.Receipt)
		delete(d.states, delivery.Record.Seq)
	}
	d.notifyLocked()
	return nil
}

// Retry releases deliveries into the bounded delayed-retry lane.
func (d *Dispatcher) Retry(deliveries []Delivery) error {
	if len(deliveries) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	now := d.now()
	d.expireLocked(now)
	if _, err := d.validateLocked(deliveries); err != nil {
		return err
	}
	for _, delivery := range deliveries {
		current := d.inFlight[delivery.Receipt]
		delete(d.inFlight, delivery.Receipt)
		d.states[current.record.Seq] = stateRetry
		heap.Push(&d.retries, &queuedRecord{
			record:  current.record,
			attempt: nextAttempt(current.attempt),
			readyAt: now.Add(d.retryDelay(current.attempt)),
		})
		d.stats.Retries++
	}
	d.notifyLocked()
	return nil
}

// Extend adds duration to valid delivery deadlines.
func (d *Dispatcher) Extend(extension time.Duration, deliveries []Delivery) error {
	if extension <= 0 {
		return ErrInvalidExtension
	}
	if len(deliveries) == 0 {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.expireLocked(d.now())
	if _, err := d.validateLocked(deliveries); err != nil {
		return err
	}
	for _, delivery := range deliveries {
		current := d.inFlight[delivery.Receipt]
		current.deadline = current.deadline.Add(extension)
		d.inFlight[delivery.Receipt] = current
	}
	d.notifyLocked()
	return nil
}

// DeadLetter validates a delivery and atomically persists its transfer before releasing it.
func (d *Dispatcher) DeadLetter(delivery Delivery, persist func(uint64) error) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return ErrClosed
	}
	d.expireLocked(d.now())
	if _, err := d.validateLocked([]Delivery{delivery}); err != nil {
		return err
	}
	if err := persist(delivery.Record.Seq); err != nil {
		return err
	}
	delete(d.inFlight, delivery.Receipt)
	delete(d.states, delivery.Record.Seq)
	d.notifyLocked()
	return nil
}

// Snapshot returns a consistent copy of dispatcher state and counters.
func (d *Dispatcher) Snapshot() Stats {
	d.mu.Lock()
	defer d.mu.Unlock()
	stats := d.stats
	stats.Ready = uint64(len(d.fresh))
	stats.Retry = uint64(len(d.retries))
	stats.InFlight = uint64(len(d.inFlight))
	return stats
}

// Close wakes blocked readers and rejects future operations without changing durable records.
func (d *Dispatcher) Close() {
	d.mu.Lock()
	if !d.closed {
		d.closed = true
		d.notifyLocked()
	}
	d.mu.Unlock()
}

func (d *Dispatcher) fillFresh() (bool, error) {
	d.scanMu.Lock()
	defer d.scanMu.Unlock()

	d.mu.Lock()
	if d.closed || d.scanDone {
		d.mu.Unlock()
		return false, nil
	}
	free := d.config.PrefetchCapacity - len(d.fresh)
	from := d.cursor
	if free <= 0 {
		d.stats.Backpressure++
		d.mu.Unlock()
		return false, nil
	}
	d.mu.Unlock()

	records, err := d.source.ScanBatch(from, free)
	if err != nil {
		return false, err
	}
	if len(records) == 0 {
		return false, nil
	}

	d.mu.Lock()
	defer d.mu.Unlock()
	progress := false
	for _, record := range records {
		if record.Seq < d.cursor {
			continue
		}
		progress = true
		if record.Seq == math.MaxUint64 {
			d.scanDone = true
		} else {
			d.cursor = record.Seq + 1
		}
		if _, tracked := d.states[record.Seq]; tracked {
			d.stats.DuplicateAttempts++
			continue
		}
		d.states[record.Seq] = stateFresh
		d.fresh = append(d.fresh, &queuedRecord{record: record, attempt: 1})
	}
	return progress, nil
}

func (d *Dispatcher) takeLocked(count int, now time.Time) ([]Delivery, error) {
	availableSlots := d.config.MaxInFlight - len(d.inFlight)
	if count > availableSlots {
		count = availableSlots
	}
	deliveries := make([]Delivery, 0, count)
	selected := make([]selectedRecord, 0, count)
	originalPreference := d.preferRetry
	for len(deliveries) < count {
		retryReady := len(d.retries) > 0 && !d.retries[0].readyAt.After(now)
		trackedLimit := d.config.PrefetchCapacity + d.config.MaxInFlight
		freshReady := len(d.fresh) > 0 && len(d.retries)+len(d.inFlight) < trackedLimit
		if !retryReady && !freshReady {
			break
		}

		receipt, err := d.uniqueReceiptLocked()
		if err != nil {
			d.rollbackSelectionLocked(selected, originalPreference)
			return nil, fmt.Errorf("dispatch: create receipt: %w", err)
		}

		var item *queuedRecord
		var previousState state
		switch {
		case retryReady && freshReady && d.preferRetry:
			item = heap.Pop(&d.retries).(*queuedRecord)
			previousState = stateRetry
			d.preferRetry = false
		case retryReady && freshReady:
			item = d.popFreshLocked()
			previousState = stateFresh
			d.preferRetry = true
		case retryReady:
			item = heap.Pop(&d.retries).(*queuedRecord)
			previousState = stateRetry
		case freshReady:
			item = d.popFreshLocked()
			previousState = stateFresh
		}
		deadline := now.Add(d.config.VisibilityTimeout)
		current := flight{record: item.record, receipt: receipt, attempt: item.attempt, deadline: deadline}
		d.inFlight[receipt] = current
		d.states[item.record.Seq] = stateInFlight
		selected = append(selected, selectedRecord{item: item, previousState: previousState, receipt: receipt})
		deliveries = append(deliveries, Delivery{
			Record: store.Record{
				Seq:          item.record.Seq,
				CreatedAt:    item.record.CreatedAt,
				OriginStream: item.record.OriginStream,
				OriginSeq:    item.record.OriginSeq,
				Payload:      append([]byte(nil), item.record.Payload...),
			},
			Receipt:  receipt,
			Attempt:  item.attempt,
			Deadline: deadline,
		})
		d.stats.Deliveries++
	}
	return deliveries, nil
}

type selectedRecord struct {
	item          *queuedRecord
	previousState state
	receipt       Receipt
}

func (d *Dispatcher) uniqueReceiptLocked() (Receipt, error) {
	for range receiptCollisionLimit {
		receipt, err := d.newReceipt()
		if err != nil {
			return Receipt{}, err
		}
		if _, exists := d.inFlight[receipt]; !exists {
			return receipt, nil
		}
	}
	return Receipt{}, errors.New("receipt collision limit reached")
}

func (d *Dispatcher) rollbackSelectionLocked(selected []selectedRecord, preference bool) {
	for index := len(selected) - 1; index >= 0; index-- {
		current := selected[index]
		delete(d.inFlight, current.receipt)
		d.states[current.item.record.Seq] = current.previousState
		if current.previousState == stateRetry {
			heap.Push(&d.retries, current.item)
		} else {
			d.fresh = append([]*queuedRecord{current.item}, d.fresh...)
		}
	}
	d.preferRetry = preference
	d.stats.Deliveries -= uint64(len(selected))
}

func (d *Dispatcher) popFreshLocked() *queuedRecord {
	item := d.fresh[0]
	copy(d.fresh, d.fresh[1:])
	d.fresh[len(d.fresh)-1] = nil
	d.fresh = d.fresh[:len(d.fresh)-1]
	return item
}

func (d *Dispatcher) expireLocked(now time.Time) {
	for receipt, current := range d.inFlight {
		if current.deadline.After(now) {
			continue
		}
		delete(d.inFlight, receipt)
		d.states[current.record.Seq] = stateRetry
		heap.Push(&d.retries, &queuedRecord{
			record:  current.record,
			attempt: nextAttempt(current.attempt),
			readyAt: now,
		})
		d.stats.LeaseExpirations++
	}
}

func (d *Dispatcher) validateLocked(deliveries []Delivery) ([]uint64, error) {
	seen := make(map[Receipt]struct{}, len(deliveries))
	seqs := make([]uint64, len(deliveries))
	for index, delivery := range deliveries {
		if _, duplicate := seen[delivery.Receipt]; duplicate {
			return nil, ErrStaleDelivery
		}
		seen[delivery.Receipt] = struct{}{}
		current, ok := d.inFlight[delivery.Receipt]
		if !ok || current.record.Seq != delivery.Record.Seq || current.attempt != delivery.Attempt {
			return nil, ErrStaleDelivery
		}
		seqs[index] = current.record.Seq
	}
	return seqs, nil
}

func (d *Dispatcher) retryDelay(attempt uint32) time.Duration {
	delay := d.config.RetryInitial
	for current := uint32(1); current < attempt && delay < d.config.RetryMax; current++ {
		next := time.Duration(float64(delay) * d.config.RetryMultiplier)
		if next <= delay || next > d.config.RetryMax {
			return d.config.RetryMax
		}
		delay = next
	}
	return delay
}

func (d *Dispatcher) nextWakeLocked() time.Time {
	var wake time.Time
	if len(d.retries) > 0 {
		wake = d.retries[0].readyAt
	}
	for _, current := range d.inFlight {
		if wake.IsZero() || current.deadline.Before(wake) {
			wake = current.deadline
		}
	}
	return wake
}

func (d *Dispatcher) wait(ctx context.Context, wakeAt time.Time) error {
	if wakeAt.IsZero() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-d.signal:
			return nil
		}
	}
	delay := time.Until(wakeAt)
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-d.signal:
		return nil
	case <-timer.C:
		return nil
	}
}

func (d *Dispatcher) notifyLocked() {
	select {
	case d.signal <- struct{}{}:
	default:
	}
}

func randomReceipt() (Receipt, error) {
	var receipt Receipt
	_, err := rand.Read(receipt[:])
	return receipt, err
}

func nextAttempt(attempt uint32) uint32 {
	if attempt == math.MaxUint32 {
		return attempt
	}
	return attempt + 1
}

type retryHeap []*queuedRecord

func (h retryHeap) Len() int { return len(h) }

func (h retryHeap) Less(first, second int) bool {
	if h[first].readyAt.Equal(h[second].readyAt) {
		return h[first].record.Seq < h[second].record.Seq
	}
	return h[first].readyAt.Before(h[second].readyAt)
}

func (h retryHeap) Swap(first, second int) {
	h[first], h[second] = h[second], h[first]
	h[first].index = first
	h[second].index = second
}

func (h *retryHeap) Push(value any) {
	item := value.(*queuedRecord)
	item.index = len(*h)
	*h = append(*h, item)
}

func (h *retryHeap) Pop() any {
	old := *h
	last := len(old) - 1
	item := old[last]
	old[last] = nil
	item.index = -1
	*h = old[:last]
	return item
}
