package btree

import (
	"bytes"
	"unsafe"
)

func keyLess(ki, kj []byte) bool {
	return bytes.Compare(ki, kj) < 0
}

func prefixMatches(k, prefix []byte) bool {
	return bytes.HasPrefix(k, prefix)
}

func copyBytes(b []byte) []byte {
	return append([]byte{}, b...)
}

func readInt32(data []byte, offset int) int32 {
	return int32(*(*uint32)(unsafe.Pointer(&data[offset])))
}

func readInt32c(data []byte, offset int) int32

func writeInt32c(data []byte, offset int, v int32)

func writeInt32(data []byte, offset int, i int32) {
	data[offset] = byte(i & 0xFF)
	data[offset+1] = byte((i >> 8) & 0xFF)
	data[offset+2] = byte((i >> 16) & 0xFF)
	data[offset+3] = byte((i >> 24) & 0xFF)
}
