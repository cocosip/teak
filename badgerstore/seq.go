package badgerstore

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"sync"
)

// defaultSeqLease is how many sequence numbers each persisted lease covers.
// A crash abandons the unused tail of a lease, which shows up as holes in the
// log — tolerated by design, since the gap mechanism in core exists for
// exactly this.
const defaultSeqLease = 1024

const seqLeaseMetaKey = "seq-lease"

// ErrSeqExhausted is returned when the uint64 sequence space runs out.
var ErrSeqExhausted = errors.New("badgerstore: sequence space exhausted")

// seqAllocator hands out strictly increasing sequence numbers starting at 1.
// Numbers are leased in windows: only the lease high-water mark is persisted,
// so allocation itself never touches a transaction. A crash may skip the
// unused tail of a lease, but a sequence is never handed out twice.
type seqAllocator struct {
	mu       sync.Mutex
	nextSeq  uint64 // next sequence to hand out (starts at 1)
	leasedTo uint64 // highest sequence covered by a durable lease
	lease    uint64 // window size per lease write
	persist  func(high uint64) error
}

func newSeqAllocator(persist func(high uint64) error, lease, durableHigh uint64) *seqAllocator {
	return &seqAllocator{
		nextSeq:  durableHigh + 1,
		leasedTo: durableHigh,
		lease:    lease,
		persist:  persist,
	}
}

// next returns the start of the range [start, start+n) and records it as
// handed out. When n == 0 it returns 0 without consuming anything.
func (a *seqAllocator) next(n uint64) (uint64, error) {
	if n == 0 {
		return 0, nil
	}
	a.mu.Lock()
	defer a.mu.Unlock()

	start := a.nextSeq
	if start == 0 || n > math.MaxUint64-start {
		return 0, ErrSeqExhausted
	}
	end := start + n - 1
	if end > a.leasedTo {
		high := a.leasedTo + a.lease
		if high < end {
			high = end
		}
		if err := a.persist(high); err != nil {
			return 0, fmt.Errorf("badgerstore: persist seq lease: %w", err)
		}
		a.leasedTo = high
	}
	a.nextSeq = end + 1
	return start, nil
}

func encodeSeqLease(high uint64) []byte {
	return binary.BigEndian.AppendUint64(nil, high)
}

func decodeSeqLease(b []byte) (uint64, bool) {
	if len(b) != seqLen {
		return 0, false
	}
	return binary.BigEndian.Uint64(b), true
}
