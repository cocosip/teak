package teak

import (
	"errors"
	"fmt"
	"testing"

	"github.com/cocosip/teak/internal/store"
)

func TestMapErrorWrapsPublicSentinels(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want error
	}{
		{"sequence exhausted", store.ErrSeqExhausted, ErrSequenceExhausted},
		{"batch too large", store.ErrBatchTooLarge, ErrBatchTooLarge},
		{"not found", store.ErrNotFound, ErrNotFound},
		{"closed", store.ErrClosed, ErrClosed},
		{"invalid count", store.ErrInvalidBatchSize, ErrInvalidCount},
		{"invalid name", store.ErrInvalidStreamName, ErrInvalidName},
		{"corrupt state", fmt.Errorf("wrapped: %w", store.ErrStateConflict), ErrCorruptStorage},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if mapped := mapError(test.err); !errors.Is(mapped, test.want) {
				t.Fatalf("mapError(%v) = %v, want %v", test.err, mapped, test.want)
			}
		})
	}
}

func TestMapErrorKeepsUnknownErrors(t *testing.T) {
	unknown := errors.New("boom")
	if mapped := mapError(unknown); mapped != unknown {
		t.Fatalf("mapError(%v) = %v, want unchanged", unknown, mapped)
	}
}
