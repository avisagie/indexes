package btree

const (
	bufSize = 1 << 20
)

type everbuf struct {
	bufs [][]byte
	cur  []byte
	curr int
}

func newEverbuf() *everbuf {
	return &everbuf{make([][]byte, 0), []byte{}, 0}
}

// Copy these bytes, and return a refernce that lets you get it back.
func (e *everbuf) Put(b []byte) (ref int) {
	if len(b)+e.curr >= len(e.cur) {
		e.cur = malloc(bufSize)
		e.bufs = append(e.bufs, e.cur)
		e.curr = 0
	}
	l := len(b)
	o := e.curr
	e.cur[o] = byte(uint16(l) & uint16(0x0F))
	e.cur[o+1] = byte(uint16(l>>8) & uint16(0x0F))
	copy(e.cur[o+2:o+2+l], b)
	ref = e.curr + bufSize*(len(e.bufs)-1)
	e.curr += 2 + l
	if e.curr&0x07 != 0 {
		e.curr = (e.curr | 0x07) + 1
	}
	return
}

// Returns a slice into the underlying storage. Take care to not
// change it or let it escape to someone who might.
func (e *everbuf) Get(ref int) []byte {
	p := ref / bufSize
	o := ref % bufSize
	b := e.bufs[p]
	l := int(uint16(b[o]) | (uint16(b[o+1]) << 8))
	return b[o+2 : o+2+l]
}

func (e *everbuf) TotalSize() int {
	return len(e.bufs) * bufSize
}

func (e *everbuf) Dispose() {
	for _, b := range e.bufs {
		free(b)
	}
}
