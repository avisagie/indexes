package malloc

// #include <malloc.h>
import "C"
import (
	"reflect"
	"unsafe"
)

var pointers map[unsafe.Pointer]struct{}

func init() {
	pointers = make(map[unsafe.Pointer]struct{})
}

func Malloc(size int) (ret []byte) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&ret))
	s.Data = uintptr(unsafe.Pointer(C.malloc(C.size_t(size))))
	s.Len = size
	s.Cap = size
	return
}

func Free(buf []byte) {
	s := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	C.free(unsafe.Pointer(s.Data))
}
