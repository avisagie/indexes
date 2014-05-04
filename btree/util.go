package btree

import "bytes"

func keyLess(ki, kj []byte) bool {
	return bytes.Compare(ki, kj) < 0
}

func prefixMatches(k, prefix []byte) bool {
	return bytes.HasPrefix(k, prefix)
}

func copyBytes(b []byte) []byte {
	return append([]byte{}, b...)
}
