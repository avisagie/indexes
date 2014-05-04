package btree

import "bytes"

func keyLess(ki, kj []byte) bool {
	return bytes.Compare(ki, kj) < 0
}

func prefixMatches(k, prefix []byte) bool {
	if len(k) < len(prefix) {
		return false
	}
	return 0 == bytes.Compare(k[:len(prefix)], prefix)
}

func copyBytes(b []byte) []byte {
	return append([]byte{}, b...)
}
