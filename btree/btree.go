// Simple B+-Tree package. Meant for in memory indexes, and builds it
// in a log structure for values for optimal use of GC time. Also
// supports appending to an existing key's value.
package btree

import (
	"fmt"
	"io"

	"github.com/avisagie/indexes"
)

// B+ Tree. Consists of pages. Satisfies indexes.Index.
type Btree struct {
	pager Pager
	root  int
	size  int64
}

type btreeIter struct {
	prefix   []byte
	pageIter PageIter
	page     Page
	b        *Btree
	done     bool
}

func (i *btreeIter) Next() (key []byte, value []byte, ok bool) {
	if i.done {
		return
	}

	key, ref, ok := i.pageIter.Next()
	if ok {
		return key, i.page.GetValue(ref), ok
	}

	// !ok can mean we're done iterating or that we're at the end
	// of this page. TODO ponder another return value that
	// signifies being done iterating explicitly.

	n := i.page.NextPage()
	if n == -1 {
		i.done = true
		return
	}

	i.page = i.b.pager.Get(n)
	i.pageIter = i.page.Start(i.prefix)
	key, ref, ok = i.pageIter.Next()
	if !ok {
		i.done = true
		return
	}
	return key, i.page.GetValue(ref), ok
}

func NewInMemoryBtree() indexes.Index {
	bt := &Btree{newInplacePager(), 0, 0}

	const internalNode = false
	ref, root := bt.pager.New(internalNode)
	bt.root = ref

	const leafNode = true
	ref, _ = bt.pager.New(leafNode)
	root.SetFirst(ref)

	return bt
}

func (b *Btree) search(key []byte) (k Key, pageRefs []int, ok bool) {
	pageRefs = make([]int, 0, 8)
	ref := b.root

	// keep track of the pageRefs we visit searching down the
	// tree.
	pageRefs = append(pageRefs, ref)

	for {
		p := b.pager.Get(ref)
		k, ok = p.Search(key)
		ref = k.Ref()

		// if it is a leaf, we're done
		if p.IsLeaf() {
			break
		}

		pageRefs = append(pageRefs, ref)
	}

	return
}

func (b *Btree) Get(key []byte) (value []byte, ok bool) {
	if len(key) == 0 {
		panic("Illegal key nil")
	}

	k, pageRefs, ok := b.search(key)
	if ok {
		page := b.pager.Get(pageRefs[len(pageRefs)-1])
		value = page.GetValue(k.Ref())
	}

	return
}

func (b *Btree) Start(prefix []byte) (it indexes.Iter) {
	_, pageRefs, _ := b.search(prefix)

	ref := pageRefs[len(pageRefs)-1]
	page := b.pager.Get(ref)

	return &btreeIter{prefix, page.Start(prefix), page, b, false}
}

func (b *Btree) split(key []byte, ref int, pageRefs []int) {
	pageRef := pageRefs[len(pageRefs)-1]
	page := b.pager.Get(pageRef)

	parentRef := pageRefs[len(pageRefs)-2]
	parent := b.pager.Get(parentRef)

	// Split the page
	newPageRef, newPage := b.pager.New(page.IsLeaf())
	splitKey := page.Split(newPageRef, newPage)

	newPage.SetNextPage(page.NextPage())
	page.SetNextPage(newPageRef)

	// Insert the key, decide in which of the resulting pages it
	// must go. Don't bother checking ok, after split there must
	// be space.
	if keyLess(key, splitKey) {
		page.Insert(key, ref)
	} else {
		newPage.Insert(key, ref)
	}

	ok := parent.Insert(splitKey, newPageRef)
	if !ok {
		if parentRef == b.root {
			if len(pageRefs) != 2 {
				panic("insane")
			}
			oldRootRef := b.root
			newRootRef, newRoot := b.pager.New(false)
			newRoot.SetFirst(oldRootRef)
			b.root = newRootRef
			b.split(splitKey, newPageRef, []int{newRootRef, parentRef})
		} else {
			b.split(splitKey, newPageRef, pageRefs[:len(pageRefs)-1])
		}
	}
}

func (b *Btree) Put(key []byte, valuev []byte) (replaced bool) {
	if len(key) == 0 || len(valuev) == 0 {
		panic("Illegal nil key or value")
	}

	_, pageRefs, replaced := b.search(key)
	pageRef := pageRefs[len(pageRefs)-1]
	page := b.pager.Get(pageRef)
	if replaced {
		// TODO do not waste a slot in p.r.values
		vref := page.InsertValue(valuev)
		page.Insert(key, vref)
		return true
	}

	vref := page.InsertValue(valuev)
	ok := page.Insert(key, vref)
	if !ok {
		b.split(key, vref, pageRefs)
	}
	b.size++
	return
}

func (b *Btree) Append(key []byte, value []byte) {
	if len(key) == 0 || len(value) == 0 {
		panic("Illegal nil key or value")
	}

	k, pageRefs, ok := b.search(key)
	if ok {
		pageRef := pageRefs[len(pageRefs)-1]
		page := b.pager.Get(pageRef)
		if !page.IsLeaf() {
			panic("Found a key in a non-leaf node")
		}

		// TODO extend Page's InsertValue to be able to append
		// without having to ditch the old copy.
		newValue := append(page.GetValue(k.Ref()), value...)
		newRef := page.InsertValue(newValue)
		page.Insert(k.Get(), newRef)
	} else {
		if replaced := b.Put(key, value); replaced {
			panic("Did not expect to have to replace the value")
		}
	}
}

func (b *Btree) Size() int64 {
	return b.size
}

// recursively check sorting inside pages and that child pages
// only have keys that are greater than or equal to the keys
// that reference them.
func (b *Btree) checkPage(page Page, checkMinKey bool, minKey []byte, ref int, depth int) error {
	if page.IsLeaf() {
		prev := []byte{}
		for i := 0; i < page.Size(); i++ {
			k, r := page.GetKey(i)
			if !keyLess(prev, k) {
				return fmt.Errorf("expect strict ordering, got violation %v >= %v", prev, k)
			}
			if r < 0 {
				return fmt.Errorf("value reference cannot be < 0")
			}
			prev = k
		}
	} else {
		prevk, prevr := page.GetKey(0)
		if prevr == -1 && page.Size() > 1 {
			return fmt.Errorf("expected internal node to refer to other pages")
		}
		for i := 1; i < page.Size(); i++ {
			k, r := page.GetKey(i)
			if checkMinKey && !keyLess(minKey, k) {
				return fmt.Errorf("expect parent key to be smaller or equal to all in referred to child page: got violation %v >= %v", prevk, minKey)
			}
			if !keyLess(prevk, k) {
				return fmt.Errorf("expect strict ordering, got violation %v >= %v", prevk, k)
			}
			if r < 0 {
				return fmt.Errorf("value reference cannot be < 0")
			}
			if err := b.checkPage(b.pager.Get(r), true, k, r, depth+1); err != nil {
				return err
			}
		}
	}

	return nil
}

func (b *Btree) CheckConsistency() error {

	count := int64(0)

	iter := b.Start([]byte{})
	prev := []byte{}
	for {
		k, _, ok := iter.Next()
		if !ok {
			break
		}
		if len(k) == 0 {
			return fmt.Errorf("got empty key")
		}
		if !keyLess(prev, k) {
			return fmt.Errorf("expect strict ordering, got violation %v >= %v", prev, k)
		}
		count++
	}

	if count != b.Size() {
		return fmt.Errorf("expected %d, got %d", b.Size(), count)
	}

	root := b.pager.Get(b.root)
	return b.checkPage(root, false, []byte{}, 0, 0)
}

func (b *Btree) appendPage(key []byte, ref int, pageRefs []int) {
	pageRef := pageRefs[len(pageRefs)-1]
	page := b.pager.Get(pageRef)

	parentRef := pageRefs[len(pageRefs)-2]
	parent := b.pager.Get(parentRef)

	newPageRef, newPage := b.pager.New(page.IsLeaf())
	page.SetNextPage(newPageRef)

	if page.IsLeaf() {
		newPage.Insert(key, ref)
	} else {
		newPage.SetFirst(ref)
	}

	ok := parent.Insert(key, newPageRef)
	if !ok {
		if parentRef == b.root {
			newRootRef, newRoot := b.pager.New(false)
			newRoot.SetFirst(b.root)
			oldRootRef := b.root
			b.root = newRootRef
			b.appendPage(key, newPageRef, []int{newRootRef, oldRootRef})
		} else {
			b.appendPage(key, newPageRef, pageRefs[:len(pageRefs)-1])
		}
	}
}

// Put a key that is strictly larger than the previous one. Assumes
// you're going to keep doing that and therefore does the bulk put
// operation.
func (b *Btree) PutNext(key, value []byte) {
	if len(key) == 0 || len(value) == 0 {
		panic("Illegal nil key or value")
	}

	pageRefs := make([]int, 0, 8)
	pageRefs = append(pageRefs, b.root)
	page := b.pager.Get(b.root)
	for !page.IsLeaf() {
		k, r := page.GetKey(page.Size() - 1)
		if !keyLess(k, key) {
			panic(fmt.Sprint("out of order put:", key))
		}
		page = b.pager.Get(r)
		pageRefs = append(pageRefs, r)
	}

	vref := page.InsertValue(value)
	ok := page.Insert(key, vref)
	if !ok {
		b.appendPage(key, vref, pageRefs)
	}
	b.size++
}

func spaces(n int) string {
	ret := []byte("")
	for i := 0; i < n; i++ {
		ret = append(ret, '\t')
	}
	return string(ret)
}

func (b *Btree) dumpPage(out io.Writer, ref, depth int) {
	space := spaces(depth)
	page := b.pager.Get(ref)
	fmt.Fprintf(out, "%sPage %d, leaf:%v, %d keys:\n", space, ref, page.IsLeaf(), page.Size())
	for i := 0; i < page.Size(); i++ {
		k, r := page.GetKey(i)
		fmt.Fprintf(out, "%s\t%d: %v -> %d\n", space, i, k, r)
		if !page.IsLeaf() {
			b.dumpPage(out, r, depth+1)
		}
	}
}

func (b *Btree) Dump(out io.Writer) {
	b.dumpPage(out, b.root, 0)
}

type BtreeStats struct {
	Finds            int
	Comparisons      int
	FillRate         float64
	NumInternalPages int
	NumLeafPages     int
}

func (b *Btree) Stats() BtreeStats {
	return b.pager.(*inplacePager).Stats()
}
