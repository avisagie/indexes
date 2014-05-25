package btree

import (
	"bytes"
	"testing"
)

func TestInplacePageReadWrite(t *testing.T) {
	data := make([]byte, 20)

	b := readInt32(data, 0)
	if b != int32(0) {
		t.Fatal("not zero", b)
	}

	writeInt32(data, 3, int32(-123456789))
	a := readInt32(data, 3)
	if a != int32(-123456789) {
		t.Fatal("not what we wrote:", a)
	}
}

func TestInplacePageFind(t *testing.T) {
	p := newInplacePager()
	h := newInplacePage(true, p)
	h.Insert([]byte{0, 0}, 0)
	h.Insert([]byte{1, 0}, 1)
	h.Insert([]byte{2}, 2)
	h.Insert([]byte{2, 0}, 3)
	h.Insert([]byte{3, 0}, 4)

	if !keyLess([]byte{2}, []byte{3}) {
		t.Error("keyLess is broken")
	}

	if keyLess([]byte{2, 1}, []byte{2, 0}) {
		t.Error("keyLess is broken")
	}

	if keyLess([]byte{2, 1}, []byte{}) {
		t.Error("keyLess is broken")
	}

	if keyLess([]byte{}, []byte{}) {
		t.Error("keyLess is broken")
	}

	if !keyLess([]byte{}, []byte{1}) {
		t.Error("keyLess is broken")
	}

	for i := 0; i < h.Size(); i++ {
		k, _ := h.GetKey(i)
		t.Log(i, []byte{2}, "<", k, "=", keyLess([]byte{2}, k))
	}

	t.Log(h.offsets, h.nextOffset)
	pos := h.find([]byte{2})
	if pos != 2 {
		t.Error("find is broken", pos)
	}
}

func TestInplacePageSearchEmpty(t *testing.T) {
	p := newInplacePager()
	h := newInplacePage(false, p)

	t.Log(h.offsets, h.nextOffset)
	k, ok := h.Search([]byte{0, 0, 0, 0, 0, 0, 0, 2})
	t.Log(ok, k)
	if ok || len(k.Get()) != 0 || k.Ref() != -1 {
		t.Fatal(ok, k)
	}
}

func TestInplacePageSearch(t *testing.T) {
	p := newInplacePager()
	h := newInplacePage(false, p)
	x := []keyRef{
		{[]byte{0, 0, 0, 0, 0, 0, 0, 2}, 2},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, 0},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 1}, 1},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 3}, 3},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 4}, 4},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 5}, 5},
	}

	for _, k := range x {
		if !h.Insert(k.key, k.ref) {
			t.Fatal("Could not insert")
		}
		t.Log(h.offsets, h.nextOffset, inMemoryPageSize)
	}

	for i := 0; i < h.Size(); i++ {
		k, r := h.GetKey(i)
		t.Log(i, "->", k, r)
	}

	k, ok := h.Search([]byte{0, 0, 0, 0, 0, 0, 0, 2})
	t.Log(ok, k)
	if !ok || bytes.Compare(k.Get(), []byte{0, 0, 0, 0, 0, 0, 0, 2}) != 0 || k.Ref() != 2 {
		t.Fatal(ok, k)
	}

	k, ok = h.Search([]byte{0, 0, 0, 0, 0, 0, 0, 5})
	t.Log(ok, k)
	if !(ok && k.Ref() == 5) {
		t.Fatal(ok, k)
	}

	k, ok = h.Search([]byte{0, 0, 0, 0, 0, 0, 0, 6})
	t.Log(ok, k)
	if ok || k.Ref() != 5 {
		t.Fatal(ok, k)
	}
}

func TestInplacePageInsert(t *testing.T) {
	p := newInplacePager()
	h := newInplacePage(true, p)

	if h.Size() != 0 {
		t.Fatal(h)
	}

	h.Insert([]byte{1, 2, 3}, 0)
	k, r := h.GetKey(0)
	if h.Size() != 1 || len(k) != 3 || r != 0 {
		t.Fatal(h)
	}

	h.Insert([]byte{1, 2, 3, 4}, 0)
	k, r = h.GetKey(1)
	if h.Size() != 2 || len(k) != 4 || r != 0 {
		t.Fatal(h)
	}

	h.Insert([]byte{1, 2, 3}, 1)
	k, r = h.GetKey(0)
	if h.Size() != 2 || len(k) != 3 || r != 1 {
		t.Fatal(h)
	}

	h.Insert([]byte{1, 2}, 2)
	k, r = h.GetKey(0)
	if h.Size() != 3 || len(k) != 2 || r != 2 {
		t.Fatal(h)
	}

	h.Insert([]byte{1, 2, 3}, 3)
	k, r = h.GetKey(1)
	if h.Size() != 3 || len(k) != 3 || r != 3 {
		t.Fatal(h)
	}

	t.Log(h)
}
