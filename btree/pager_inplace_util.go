package btree

import (
	"reflect"
	"unsafe"
)

type pageEntry struct {
	offset, length uint16
	ref            int32
}

var pageEntrySize = int(unsafe.Sizeof(pageEntry{}))

func getPageEntries(bytes []byte) (ret []pageEntry) {
	h := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	h.Data = uintptr(unsafe.Pointer(&bytes[0]))
	h.Cap = len(bytes) / pageEntrySize
	h.Len = h.Cap
	return
}

func entryOffset(entry int) int {
	return pageEntrySize * entry
}

func entryEnd(entry int) int {
	return pageEntrySize * (entry + 1)
}
