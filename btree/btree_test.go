package btree

import (
	"bytes"
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/gpaul/indexes"
)

func TestBtreeCreate(t *testing.T) {
	var index indexes.Index
	index = NewInMemoryBtree()
	t.Log(index)
	if err := index.(*Btree).CheckConsistency(); err != nil {
		t.Fatal(err)
	}
}

func TestKeys(t *testing.T) {
	key1 := []byte{5, 5, 5}
	key2 := []byte{6, 5, 5}
	var empty []byte
	if keyLess(key2, key1) {
		t.Error("1")
	}
	if !keyLess(key1, key2) {
		t.Error("2")
	}
	if keyLess(key1, key1) {
		t.Error("3")
	}
	if keyLess(key1, empty) || keyLess(key2, empty) {
		t.Error("4")
	}
	if !keyLess(empty, key1) || !keyLess(empty, key2) {
		t.Error("5")
	}
}

func TestBtreeSearchEmpty(t *testing.T) {
	index := NewInMemoryBtree()
	ok, value := index.Get([]byte{1, 2, 3})
	if ok {
		t.Error("Did not expect to find anything")
	}
	if value != nil {
		t.Error("Should not have a value")
	}
	if err := index.(*Btree).CheckConsistency(); err != nil {
		t.Fatal(err)
	}
}

func TestBtreeInsert1(t *testing.T) {
	index := NewInMemoryBtree()
	ok, value := index.Get([]byte{1, 2, 3})
	if ok {
		t.Fatal("Did not expect to find anything")
	}
	if value != nil {
		t.Fatal("Should not have a value")
	}

	k, v := []byte{1, 2, 3}, []byte{4, 5, 6}
	replaced := index.Put(k, v)
	if replaced {
		t.Fatal("Empty btree, could not have inserted anything")
	}

	ok, value = index.Get([]byte{1, 2, 3})
	if !ok {
		t.Fatal("Expected to find it", index)
	}
	if bytes.Compare(v, value) != 0 {
		t.Fatal("Got wrong value out")
	}

	ok, value = index.Get([]byte{5})
	if ok {
		t.Fatal("Did not expect to find anything")
	}
	if value != nil {
		t.Fatal("Should not have a value")
	}

	replaced = index.Put(k, v)
	if !replaced {
		t.Fatal("Expected it to have been replaced")
	}

	replaced = index.Put([]byte{3, 2, 1}, v)
	if replaced {
		t.Fatal("Expected it to NOT have been replaced")
	}

	ok, value = index.Get([]byte{1, 2, 3})
	if !ok {
		t.Fatal("Expected to find it")
	}
	if bytes.Compare(v, value) != 0 {
		t.Fatal("Got wrong value out")
	}

	ok, value = index.Get([]byte{5})
	if ok {
		t.Fatal("Did not expect to find anything")
	}
	if value != nil {
		t.Fatal("Should not have a value")
	}

	t.Log(index)
}

func TestBtreeAppend(t *testing.T) {
	index := NewInMemoryBtree()
	index.Put([]byte{1, 2, 3}, []byte{4, 5, 6})
	index.Put([]byte{1, 2, 1}, []byte{6, 5, 4})
	index.Append([]byte{1, 2, 3}, []byte{7, 8, 9})

	ok, value := index.Get([]byte{1, 2, 3})
	expected := []byte{4, 5, 6, 7, 8, 9}
	if !ok || bytes.Compare(value, expected) != 0 {
		t.Fatal("Expected", expected, ", got", ok, value)
	}

	t.Log(index)

	if err := index.(*Btree).CheckConsistency(); err != nil {
		t.Fatal(err)
	}
}

func TestBtreeOverride(t *testing.T) {
	index := NewInMemoryBtree()
	ok, value := index.Get([]byte{1, 2, 3})
	if ok {
		t.Fatal("Did not expect to find anything")
	}
	if value != nil {
		t.Fatal("Should not have a value")
	}
	if err := index.(*Btree).CheckConsistency(); err != nil {
		t.Fatal(err)
	}
}

func pfxm(t *testing.T, k, pfx []byte, must bool) {
	if prefixMatches(k, pfx) != must {
		t.Fatal("k =", k, "pfx =", pfx, "expected", must, "got", !must)
	}
}

func TestPrefixMatches(t *testing.T) {
	pfxm(t, []byte{}, []byte{}, true)
	pfxm(t, []byte{1}, []byte{1}, true)
	pfxm(t, []byte{1, 2}, []byte{1}, true)
	pfxm(t, []byte{1, 2}, []byte{1, 2}, true)
	pfxm(t, []byte{1, 2, 3, 4, 5, 6}, []byte{1, 2}, true)
	pfxm(t, []byte{1, 2}, []byte{1, 2, 3}, false)
	pfxm(t, []byte{1, 2}, []byte{2, 1}, false)
	pfxm(t, []byte{1, 2}, []byte{2}, false)
}

func TestShortIter(t *testing.T) {
	index := NewInMemoryBtree()
	index.Put([]byte{1, 2, 3}, []byte{1, 2, 3})
	index.Put([]byte{1, 2, 4}, []byte{1, 2, 3})
	index.Put([]byte{1, 2, 5}, []byte{1, 2, 3})

	t.Log("[]byte{1, 2}:")
	count := 0
	it := index.Start([]byte{1, 2})
	for {
		ok, k, v := it.Next()
		if !ok {
			break
		}
		t.Log("  ", k, "=>", v)
		count += 1
	}
	if count != 3 {
		t.Fatal("Expected 3, got", count)
	}

	t.Log("[]byte{2}:")
	count = 0
	it = index.Start([]byte{2})
	for {
		ok, k, v := it.Next()
		if !ok {
			break
		}
		t.Log("  ", k, "=>", v)
		count += 1
	}
	if count != 0 {
		t.Fatal("Expected 0, got", count)
	}

	t.Log("[]byte{1, 2, 5}:")
	count = 0
	it = index.Start([]byte{1, 2, 5})
	for {
		ok, k, v := it.Next()
		if !ok {
			break
		}
		t.Log("  ", k, "=>", v)
		count += 1
	}
	if count != 1 {
		t.Fatal("Expected 1, got", count)
	}

	for ii := 0; ii < 100; ii++ {
		// After iteration it must remain in the done state and not do anything else
		ok, k, v := it.Next()
		if ok || k != nil || v != nil {
			t.Fatal(ok, k, v)
		}
	}
}

func fill(t *testing.T, index indexes.Index) (keys [][]byte) {
	buffer := &bytes.Buffer{}
	count := int32(0)

	keys = make([][]byte, 0)

	for ; count < int32(30*pageSize/(4+4+4)+5); count++ {
		binary.Write(buffer, binary.LittleEndian, count)
		b := copyBytes(buffer.Bytes())
		keys = append(keys, b)
		buffer.Reset()
	}

	// randomize ish
	for i, _ := range keys {
		j := rand.Int31n(int32(len(keys)))
		keys[i], keys[j] = keys[j], keys[i]
	}

	for _, b := range keys {
		index.Put(b, b)
	}

	//	pager := index.(*Btree).pager.(*ramPager)
	//	for i, p := range pager.pages {
	//		t.Log(i, len(p.keys), p.keys)
	//	}

	for _, k := range keys {
		ok, v := index.Get(k)
		if !ok || bytes.Compare(k, v) != 0 {
			t.Fatal("Expected", k, "got", v)
		}
	}

	if err := index.(*Btree).CheckConsistency(); err != nil {
		t.Fatal(err)
	}

	return
}

func TestLarger(t *testing.T) {
	index := NewInMemoryBtree()
	keys := fill(t, index)

	// iteration
	x := 0
	iter := index.Start([]byte{})
	prev := []byte{}
	for {
		ok, k, v := iter.Next()
		if !ok {
			break
		}
		if !keyLess(prev, k) {
			t.Fatal("Expected ", prev, " < ", k)
		}
		if bytes.Compare(k, v) != 0 {
			t.Fatal("Inserted values equal to keys. Got ", k, "=>", v)
		}
		x++
	}

	if x != len(keys) {
		t.Fatal("Expected", len(keys), "got", x)
	}
}

func TestIteration2(t *testing.T) {
	index := NewInMemoryBtree()
	fill(t, index)

	iter := index.Start([]byte{4})
	for {
		ok, k, v := iter.Next()
		if !ok {
			break
		}
		if k[0] != 4 {
			t.Fatal("Expected something with prefix 4")
		}
		t.Log(k, "=>", v)
	}
}

func TestBulk(t *testing.T) {
	index1 := NewInMemoryBtree()
	fill(t, index1)

	index2 := NewInMemoryBtree()
	bt := index2.(*Btree)

	iter := index1.Start([]byte{})
	for {
		ok, k, v := iter.Next()
		if !ok {
			break
		}
		bt.PutNext(k, v)
	}

	if err := bt.CheckConsistency(); err != nil {
		t.Fatal(err)
	}

	iter1 := index1.Start([]byte{})
	iter2 := index2.Start([]byte{})
	for count := 1; ; count++ {
		ok1, k1, v1 := iter1.Next()
		ok2, k2, v2 := iter2.Next()

		if ok1 != ok2 || bytes.Compare(k1, k2) != 0 || bytes.Compare(v1, v2) != 0 {
			t.Fatal("Not the same:", ok1, ok2, k1, k2, v1, v2)
		}

		if !ok1 {
			break
		}
	}
	t.Log("Bulk filled used pages:", len(bt.pager.(*inplacePager).pages))
	t.Log("Random filled used pages:", len(index1.(*Btree).pager.(*inplacePager).pages))
}
