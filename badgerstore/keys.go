package badgerstore

import (
	"encoding/binary"
)

// Key layout inside the shared Badger instance:
//
//	t/<log>/d/<8-byte big-endian seq>   log entries, ordered by sequence
//	t/<log>/m/<key>                     stream metadata (seq lease, watermarks)
//
// Stream names are restricted to [A-Za-z0-9._-]+ so '/' unambiguously
// separates the layout components and prefix scans stay exact.

const seqLen = 8

func dataPrefix(log string) []byte {
	return []byte("t/" + log + "/d/")
}

func dataKey(log string, seq uint64) []byte {
	k := make([]byte, 0, 2+len(log)+3+seqLen)
	k = append(k, "t/"...)
	k = append(k, log...)
	k = append(k, "/d/"...)
	return binary.BigEndian.AppendUint64(k, seq)
}

func metaKey(log, key string) []byte {
	k := make([]byte, 0, 2+len(log)+3+len(key))
	k = append(k, "t/"...)
	k = append(k, log...)
	k = append(k, "/m/"...)
	k = append(k, key...)
	return k
}

// parseDataSeq extracts the sequence number from a data key of the given
// stream. It returns false for anything else (meta keys, other streams).
func parseDataSeq(log string, key []byte) (uint64, bool) {
	if len(key) != 2+len(log)+3+seqLen {
		return 0, false
	}
	if string(key[:2]) != "t/" ||
		string(key[2:2+len(log)]) != log ||
		string(key[2+len(log):2+len(log)+3]) != "/d/" {
		return 0, false
	}
	return binary.BigEndian.Uint64(key[len(key)-seqLen:]), true
}
