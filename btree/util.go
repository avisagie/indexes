package btree

import "bytes"

func keyLess(ki, kj []byte) bool {
	// if the key is nil or empty, it is defined to be the
	// smallest one in the page.
	switch {
	case kj == nil || len(kj) == 0:
		return false
	case ki == nil || len(ki) == 0:
		return true
	}

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
