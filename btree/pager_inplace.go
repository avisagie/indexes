package btree

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/avisagie/indexes/malloc"
)

const (
	// This is a good inMemoryPageSize for x64 while building an in-memory
	// b+tree with small keys.
	inMemoryPageSize = 1 << 10
)

type inplacePageIter struct {
	pos    int
	prefix []byte
	p      *inplacePage
}

func (i *inplacePageIter) Next() (key []byte, ref int, ok bool) {
	if i.pos >= i.p.numPageEntries {
		return
	}
	key, ref = i.p.readKey(i.pos)
	if !prefixMatches(key, i.prefix) {
		return nil, -1, false
	}

	i.pos++

	return key, ref, true
}

type keyRef struct {
	key []byte
	ref int
}

func (k keyRef) Get() []byte {
	return k.key
}

func (k keyRef) Ref() int {
	return k.ref
}

func newKeyRef(key []byte, ref int) keyRef {
	return keyRef{key, ref}
}

var nilKeyRef = keyRef{nil, -1}

// Implements Page using byte slices on the heap. Keys store length,
// bytes and a reference to the value in the page itself. If it is a
// leaf, the first key has zero bytes, and its reference is the left
// reference for the first key. If it is a leaf, the first key has a
// value reference.
//
// Format:
//   TODO: Update
type inplacePage struct {
	// keys, lengths and refs encoded into bytes for writing to
	// disk.
	data []byte

	// Page entries and ref values.
	pageEntries    []pageEntry
	numPageEntries int

	// The bottom of the actual key bytes.
	bottom int

	// Reference to the next page
	next   int32
	isLeaf bool
	r      *inplacePager

	// where are we in the current buffer?
	nextOffset int

	// the last key in the page
	lastKey []byte

	finds, comparisons int
}

func newInplacePage(isLeaf bool, r *inplacePager) *inplacePage {
	ret := &inplacePage{
		data:           malloc.Malloc(inMemoryPageSize),
		numPageEntries: 0,
		bottom:         inMemoryPageSize,
		next:           -1,
		isLeaf:         isLeaf,
		r:              r,
		nextOffset:     0,
		finds:          0,
		comparisons:    0,
	}
	ret.pageEntries = getPageEntries(ret.data)

	if !isLeaf {
		ret.Insert(nil, -1)
	}
	return ret
}

func (p *inplacePage) find(key []byte) (pos int) {
	pos = sort.Search(p.numPageEntries, func(i int) bool {
		p.comparisons++
		k, _ := p.readKey(i)
		return !keyLess(k, key)
	})
	p.finds++
	return
}

func (p *inplacePage) readKey(pos int) (key []byte, ref int) {
	e := p.pageEntries[pos]
	offset := int(e.offset)
	length := int(e.length)
	return p.data[offset : offset+length], int(e.ref)
}

func (p *inplacePage) writeKey(pos int, key []byte, ref int32) bool {
	if p.bottom-len(key) < pageEntrySize*(p.numPageEntries+1) {
		// PLIF
		return false
	}

	p.bottom -= len(key)
	offset := p.bottom
	copy(p.data[offset:offset+len(key)], key)

	entry := pageEntry{
		offset: uint16(offset),
		length: uint16(len(key)),
		ref:    ref,
	}

	// insert the entry offset into the right place maintain
	// sorted order.
	copy(p.pageEntries[pos+1:p.numPageEntries+1], p.pageEntries[pos:p.numPageEntries])
	p.pageEntries[pos] = entry
	if pos == len(p.pageEntries)-1 {
		// we're adding a new right-most key.
		// save an internal reference to it
		p.lastKey = p.data[offset : offset+len(key)]
	}
	p.numPageEntries++

	return true
}

func (p *inplacePage) Insert(key []byte, ref int) bool {
	// TODO optimize for inserting in order

	pos := p.find(key)

	const refSize = 4
	if pos < p.numPageEntries {
		k, _ := p.readKey(pos)
		// replace
		if bytes.Equal(key, k) {
			// add the reference after the existing one
			p.pageEntries[pos].ref = int32(ref)
			return true
		}

		// TODO remove
		if bytes.Compare(key, k) >= 0 {
			panic(fmt.Sprint("sanity check failed", key, k))
		}
	}

	return p.writeKey(pos, key, int32(ref))
}

func (p *inplacePage) Search(key []byte) (k Key, ok bool) {
	pos := p.find(key)
	if pos == p.numPageEntries {
		// key is greater than the last key in this page.
		if !p.isLeaf {
			return newKeyRef(p.readKey(pos - 1)), false
		}
		return nilKeyRef, false
	}

	k = newKeyRef(p.readKey(pos))
	ok = bytes.Equal(key, k.Get())
	if !ok && !p.isLeaf && keyLess(key, k.Get()) {
		k = newKeyRef(p.readKey(pos - 1))
	}
	return
}

func (p *inplacePage) IsLeaf() bool {
	return p.isLeaf
}

func (p *inplacePage) NextPage() (ref int) {
	return int(p.next)
}

func (p *inplacePage) SetNextPage(ref int) {
	p.next = int32(ref)
}

func (p *inplacePage) Start(prefix []byte) PageIter {
	return &inplacePageIter{p.find(prefix), prefix, p}
}

func (p *inplacePage) GetKey(i int) ([]byte, int) {
	return p.readKey(i)
}

// Used in split. Does not need to do binary search, just keep adding
// to the end.
func (p *inplacePage) appendKey(key []byte, ref int32) {
	ok := p.writeKey(p.numPageEntries, key, ref)
	if !ok {
		panic("appendKey assumes there will be space")
	}
}

func (p *inplacePage) Split(newPageRef int, newPage1 Page) (splitKey []byte) {
	newPage, ok := newPage1.(*inplacePage)
	if !ok {
		panic("Cannot split into a different type of page: expected a inplacePage")
	}

	copy(p.r.scratchData, p.data)
	numPageEntries := p.numPageEntries
	pageEntries := getPageEntries(p.r.scratchData)

	// reset p. If it is not a leaf it will get its first
	// reference back from scratchData shortly.
	p.numPageEntries = 0
	p.bottom = inMemoryPageSize

	pos := 0

	// copy half the keys back into page
	for ; pos < numPageEntries/2; pos++ {
		entry := pageEntries[pos]
		offset, length := int(entry.offset), int(entry.length)
		// fmt.Println("Copying", pos, ":", p.r.scratchData[offset:offset+length], "to left page")
		p.appendKey(p.r.scratchData[offset:offset+length], entry.ref)
	}

	// Do right by the middle key
	{
		entry := pageEntries[pos]
		offset, length := int(entry.offset), int(entry.length)
		splitKey = copyBytes(p.r.scratchData[offset : offset+length])
		// fmt.Println("SplitKey is", pos, ":", p.r.scratchData[offset:offset+length])
		if !p.isLeaf {
			// skip the middle key
			newPage.SetFirst(int(entry.ref))
			pos++
		}
	}

	// copy the remaining keys to newPage
	for ; pos < numPageEntries; pos++ {
		entry := pageEntries[pos]
		offset, length := int(entry.offset), int(entry.length)
		//fmt.Println("Copying", pos, ":", p.r.scratchData[offset:offset+length], "to right page")
		newPage.appendKey(p.r.scratchData[offset:offset+length], entry.ref)
	}

	return

	/*
		copy(p.r.scratchData, p.data)
		p.r.scratchOffsets = append(p.r.scratchOffsets[:0], p.offsets...)

		p.offsets = p.offsets[:0]
		p.nextOffset = 0
		i := 0
		var (
			offset, ref int
			key         []byte
		)
		for ; i < len(p.r.scratchOffsets); i++ {
			offset = p.r.scratchOffsets[i]
			length := int(readInt32(p.r.scratchData, offset))
			offset += 4
			ref = int(readInt32(p.r.scratchData, offset))
			offset += 4
			key = p.r.scratchData[offset : offset+length]
			if length+p.nextOffset > inMemoryPageSize/2 {
				break
			}
			//fmt.Println(i, "Copying", offset, key, ref, "left to", p.nextOffset)
			p.appendKey(key, ref)
		}

		if !p.isLeaf {
			// skip the middle key
			//fmt.Println("Moving middle key up in non-leaf node:", key, ref)
			newPage.SetFirst(int(ref))
			i++
		}
		splitKey = copyBytes(key)
		//fmt.Println(i, "Splitkey =", splitKey)

		for ; i < len(p.r.scratchOffsets); i++ {
			offset = p.r.scratchOffsets[i]
			length := int(readInt32(p.r.scratchData, offset))
			offset += 4
			ref = int(readInt32(p.r.scratchData, offset))
			offset += 4
			key = p.r.scratchData[offset : offset+length]
			//fmt.Println(i, "Copying", offset, key, ref, "right to", p.nextOffset)
			newPage.appendKey(key, ref)
		}

		return
	*/
}

func (p *inplacePage) First() int {
	return int(p.pageEntries[0].ref)
}

func (p *inplacePage) SetFirst(ref int) {
	if p.isLeaf {
		panic("Not setting first on non-leaf node")
	}
	p.pageEntries[0].ref = int32(ref)
}

func (p *inplacePage) Size() int {
	return p.numPageEntries
}

func (p *inplacePage) InsertValue(value []byte) int {
	return p.r.values.Put(value)
}

func (p *inplacePage) GetValue(vref int) []byte {
	return p.r.values.Get(vref)
}

func (p *inplacePage) Dispose() {
	malloc.Free(p.data)
}

// Implements Pager by keeping pages in RAM on the heap.
type inplacePager struct {
	pages          []*inplacePage
	freePages      []int
	scratchData    []byte
	scratchOffsets []int
	values         *everbuf
}

func newInplacePager() *inplacePager {
	return &inplacePager{nil, nil, malloc.Malloc(inMemoryPageSize), make([]int, 32), newEverbuf()}
}

func (r *inplacePager) New(isLeaf bool) (ref int, page Page) {
	// This always allocates a new page, i.e. it does not reuse
	// pages. It forgets them so that GC can get them. It only
	// reuses refs.

	if len(r.freePages) > 0 {
		ref := r.freePages[len(r.freePages)-1]
		r.freePages = r.freePages[:len(r.freePages)-1]
		if r.pages[ref] != nil {
			panic(fmt.Sprint("page", ref, "was in freePages, but the page appears to be in use"))
		}
		page := newInplacePage(isLeaf, r)
		r.pages[ref] = page
		return ref, page
	}

	ref = len(r.pages)
	r.pages = append(r.pages, newInplacePage(isLeaf, r))
	page = r.pages[ref]
	return ref, page
}

func (r *inplacePager) Get(ref int) (page Page) {
	page = r.pages[ref]
	if page == nil {
		panic(fmt.Sprint("Trying to get freed page", ref))
	}
	return page
}

func (r *inplacePager) Release(ref int) {
	r.freePages = append(r.freePages, ref)
	r.pages[ref].Dispose()
	r.pages[ref] = nil
}

func (r *inplacePager) Stats() BtreeStats {
	ret := BtreeStats{}
	sumFill := 0.0
	countFill := 0.0
	for _, p := range r.pages {
		if p != nil {
			ret.Finds += p.finds
			ret.Comparisons += p.comparisons
			sumFill += float64(p.nextOffset) / float64(inMemoryPageSize)
			countFill += 1.0
			if p.IsLeaf() {
				ret.NumLeafPages++
			} else {
				ret.NumInternalPages++
			}

			for ik := 0; ik < p.Size(); ik++ {
				k, ref := p.GetKey(ik)
				ret.KeyBytes += len(k)
				ret.ValueBytes += len(r.values.Get(ref))
			}

			ret.PageBytes += inMemoryPageSize
		}
	}
	ret.FillRate = sumFill / countFill
	ret.ValueStoreBytes = r.values.TotalSize()
	return ret
}

func (r *inplacePager) Dispose() {
	for _, p := range r.pages {
		p.Dispose()
	}
	r.values.Dispose()
	malloc.Free(r.scratchData)
}
