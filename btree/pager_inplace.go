package btree

import (
	"bytes"
	"fmt"
	"sort"
)

// Implements Page using byte slices on the heap. Keys store length,
// bytes and a reference to the value in the page itself. If it is a
// leaf, the first key has zero bytes, and its reference is the left
// reference for the first key. If it is a leaf, the first key has a
// value reference.
//
// Format:
// nextPage: int32
// isLeaf: byte
// page:
//   length: int32, LittleEndian
//   valueRef: int32, LittleEndian
//   bytes
//   repeat
type inplacePage struct {
	// built up while the page is in RAM and when it is read from
	// disk.
	offsets []int

	// keys, lengths and refs encoded into bytes for writing to
	// disk.
	data []byte

	// Reference to the next page
	next   int32
	isLeaf bool
	r      *inplacePager

	// where are we in the current buffer?
	nextOffset int

	finds, comparisons int
}

type inplacePageIter struct {
	pos    int
	prefix []byte
	p      *inplacePage
}

func (i *inplacePageIter) Next() (ok bool, key []byte, ref int) {
	if i.pos >= len(i.p.offsets) {
		return
	}
	key, ref = i.p.readKey(i.pos)
	if !prefixMatches(key, i.prefix) {
		return false, nilBytes, -1
	}

	i.pos += 1

	return true, key, ref
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

var nilKeyRef = keyRef{nilBytes, -1}
var nilBytes []byte

func newInplacePage(isLeaf bool, r *inplacePager) *inplacePage {
	ret := &inplacePage{
		offsets:     make([]int, 0),
		data:        make([]byte, pageSize),
		next:        -1,
		isLeaf:      isLeaf,
		r:           r,
		nextOffset:  0,
		finds:       0,
		comparisons: 0,
	}
	if !isLeaf {
		ret.Insert(nilBytes, -1)
	}
	return ret
}

func (p *inplacePage) find(key []byte) (pos int) {
	pos = sort.Search(len(p.offsets), func(i int) bool {
		p.comparisons++
		k, _ := p.readKey(i)
		return bytes.Compare(k, key) >= 0 // !keyLess(k, key)
	})
	p.finds++
	return
}

func (p *inplacePage) writeKey(offset int, key []byte, ref int) {
	length := len(key)
	writeInt32(p.data, offset, int32(length))
	writeInt32(p.data, offset+4, int32(ref))
	copy(p.data[offset+8:offset+8+length], key)
}

func (p *inplacePage) readKey(pos int) (key []byte, ref int) {
	offset := p.offsets[pos]
	length := int(readInt32(p.data, offset))
	offset+=4
	ref = int(readInt32(p.data, offset))
	offset+=4
	key = p.data[offset : offset+length]
	return
}

func (p *inplacePage) Insert(key []byte, ref int) bool {
	pos := -1

	// short cut for in-order inserts
	if len(p.offsets) > 0 {
		lastKey, _ := p.readKey(len(p.offsets) - 1)
		if bytes.Compare(lastKey, key) < 0 { // keyLess(lastKey, key) {
			pos = len(p.offsets)
		}
	} else {
		pos = 0
	}

	// not in order? Find the right place
	if pos == -1 {
		pos = p.find(key)
	}

	if pos < len(p.offsets) {
		k, _ := p.readKey(pos)
		// replace
		if bytes.Compare(key, k) == 0 {
			writeInt32(p.data, p.offsets[pos]+4, int32(ref))
			return true
		}
	}

	if p.nextOffset+len(key)+4+4 >= pageSize {
		return false
	}

	// append the key to the page
	offset := p.nextOffset
	p.nextOffset += 8 + len(key)
	p.writeKey(offset, key, ref)

	// insert its offset into the right place in p.offsets to
	// maintain sorted order.
	p.offsets = append(p.offsets, -1)
	copy(p.offsets[pos+1:], p.offsets[pos:len(p.offsets)-1])
	p.offsets[pos] = offset

	return true
}

func (p *inplacePage) Search(key []byte) (ok bool, k Key) {
	pos := p.find(key)
	if pos == len(p.offsets) {
		// key is greater than the last key in this page.
		if !p.isLeaf {
			return false, newKeyRef(p.readKey(pos - 1))
		}
		return false, nilKeyRef
	}

	k = newKeyRef(p.readKey(pos))
	ok = bytes.Compare(key, k.Get()) == 0
	if !ok && !p.isLeaf && bytes.Compare(key, k.Get()) < 0 { // keyLess(key, k.Get()) {
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
func (p *inplacePage) appendKey(key []byte, ref int) {
	p.writeKey(p.nextOffset, key, ref)
	p.offsets = append(p.offsets, p.nextOffset)
	p.nextOffset += 8 + len(key)
}

func (p *inplacePage) Split(newPageRef int, newPage1 Page) (splitKey []byte) {
	//fmt.Println("Splitting")

	newPage, ok := newPage1.(*inplacePage)
	if !ok {
		panic("Cannot split into a different type of page: expected a inplacePage")
	}

	p.r.scratchData = append(p.r.scratchData[:0], p.data...)
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
		ref = int(readInt32(p.r.scratchData, offset+4))
		key = p.r.scratchData[offset+8 : offset+8+length]
		if length+p.nextOffset > pageSize/2 {
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
		ref = int(readInt32(p.r.scratchData, offset+4))
		key = p.r.scratchData[offset+8 : offset+8+length]
		//fmt.Println(i, "Copying", offset, key, ref, "right to", p.nextOffset)
		newPage.appendKey(key, ref)
	}

	return
}

func (p *inplacePage) First() int {
	return int(readInt32(p.data, 4))
}

func (p *inplacePage) SetFirst(ref int) {
	if p.isLeaf {
		panic("Not setting first on non-leaf node")
	}

	writeInt32(p.data, 4, int32(ref))
}

func (p *inplacePage) Size() int {
	return len(p.offsets)
}

// Implements Pager by keeping pages in RAM on the heap.
type inplacePager struct {
	pages          []*inplacePage
	freePages      []int
	scratchData    []byte
	scratchOffsets []int
}

func newInplacePager() *inplacePager {
	return &inplacePager{make([]*inplacePage, 0), make([]int, 0), make([]byte, pageSize), make([]int, 32)}
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
			sumFill += float64(p.nextOffset) / float64(pageSize)
			countFill += 1.0
			if p.IsLeaf() {
				ret.NumLeafPages++
			} else {
				ret.NumInternalPages++
			}
		}
	}
	ret.FillRate = sumFill / countFill
	return ret
}
