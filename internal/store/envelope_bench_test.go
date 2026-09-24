package store

import (
	"bytes"
	"testing"
	"time"
)

func benchmarkPendingEnvelope() pendingEnvelope {
	return pendingEnvelope{
		CreatedAt:    time.Unix(1_800_000_000, 0).UTC(),
		OriginStream: "origin",
		OriginSeq:    7,
		Payload:      bytes.Repeat([]byte("t"), 1024),
	}
}

func BenchmarkPendingEnvelopeEncode(b *testing.B) {
	envelope := benchmarkPendingEnvelope()
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := encodePending(envelope); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPendingEnvelopeDecode(b *testing.B) {
	encoded, err := encodePending(benchmarkPendingEnvelope())
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := decodePending(encoded); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeadEnvelopeRoundTrip(b *testing.B) {
	envelope := deadEnvelope{
		CreatedAt:      time.Unix(1_800_000_000, 0).UTC(),
		DeadLetteredAt: time.Unix(1_800_000_100, 0).UTC(),
		OriginStream:   "origin",
		OriginSeq:      7,
		Payload:        bytes.Repeat([]byte("t"), 1024),
		Reason:         "unsupported job version",
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		encoded, err := encodeDead(envelope)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := decodeDead(encoded); err != nil {
			b.Fatal(err)
		}
	}
}
