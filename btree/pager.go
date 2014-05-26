package btree

type Key interface {
	// key bytes. Immutable please.
	Get() []byte
	// In leaf nodes: reference to values. In internal nodes:
	// reference to the page with keys equal to or greater.
	Ref() int
}

type PageIter interface {
	Next() (key []byte, ref int, ok bool)
}

type Page interface {
	// Insert these bytes and reference as the key. In non-leaf
	// nodes, the references points to child nodes with keys equal
	// to or greater. In leaf nodes it points to a value. Returns
	// false if the key was not inserted. The only reason for not
	// inserting the key is that the page is full. The pager
	// promises to not be dependent on your copy of the byte slice
	// after this operation returns.
	Insert(k []byte, ref int) (ok bool)

	// Like insert, but you promise that you are inserting in
	// order. This is a special case of Insert. You can always
	// just call Insert inside PutNext.
	PutNext(k []byte, ref int) (ok bool)

	// Returns true and the key if it is found. Returns false and
	// one key smaller if not found so that btree can use its
	// reference to figure out in which child page it belongs...
	Search(k []byte) (key Key, ok bool)

	IsLeaf() bool

	// Return the next page at this level
	NextPage() (ref int)
	SetNextPage(ref int)

	// Iterator support. This iterator will stop at the end of the
	// page. It is the responsibility of the btree implementation
	// to find the next page and continue iteration.
	Start(prefix []byte) PageIter

	// Get the key and ref at this index. For leaves keys start at
	// 1. for internal nodes, key number 0 contains the left
	// reference, as set by SetFirst, and no actual key.
	GetKey(i int) ([]byte, int)

	// Split this page into the given one
	Split(newPageRef int, newPage Page) (splitKey []byte)

	First() int
	SetFirst(ref int)

	// Number of keys. See GetKey for an explanation of what to
	// expect around key 0.
	Size() int

	// Tie allocation of space for values to the pages. Only
	// relevant for leaf nodes.
	InsertValue(value []byte) int
	GetValue(ref int) []byte
}

type Pager interface {
	New(isLeaf bool) (ref int, page Page)
	Get(ref int) (page Page)
	Release(ref int)
	Stats() BtreeStats
	Dispose()
}
