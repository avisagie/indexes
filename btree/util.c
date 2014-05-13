#include "runtime.h"

// experiment to use plan 9 C. It is slower. Probably because the go
// unsafe version gets inlined. Perhaps making the whole ReadKey
// function in C might help...

void ·readInt32c(Slice b, intptr offset, int32 ret) {
  ret = *( (int32*)(b.array+offset) );
  FLUSH(&ret);
}

void ·writeInt32c(Slice b, intptr offset, int32 v) {
  *( (int32*)(b.array+offset) ) = v;
}
