package store

import (
	"bytes"
	"encoding/binary"
)

const seqLen = 8

func pendingPrefix(stream string) []byte {
	return []byte("t/" + stream + "/d/")
}

func pendingKey(stream string, seq uint64) []byte {
	return sequenceKey(pendingPrefix(stream), seq)
}

func deadPrefix(stream string) []byte {
	return []byte("t/" + stream + "/x/")
}

func deadKey(stream string, seq uint64) []byte {
	return sequenceKey(deadPrefix(stream), seq)
}

func sequenceKey(prefix []byte, seq uint64) []byte {
	key := make([]byte, len(prefix), len(prefix)+seqLen)
	copy(key, prefix)
	return binary.BigEndian.AppendUint64(key, seq)
}

func metaKey(stream, name string) []byte {
	return []byte("t/" + stream + "/m/" + name)
}

func parseSequence(prefix, key []byte) (uint64, bool) {
	if len(key) != len(prefix)+seqLen || !bytes.Equal(key[:len(prefix)], prefix) {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(prefix):]), true
}
