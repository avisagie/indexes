// Indexes for key []byte -> value byte[], single key (i.e. not a
// multimap)
package indexes

type Iter interface {
	// return consecutive keys and values. ok is false (key abd
	// value are nil) when done.
	Next() (key []byte, value []byte, ok bool)
}

// Read-only index
type ROIndex interface {
	Get(key []byte) (value []byte, ok bool)
	Start(keyPrefix []byte) Iter
	Size() int64
}

// Index that can put
type Putable interface {
	// put or override a key. returns true if it had to replace
	// one.
	Put(key []byte, value []byte) (replaced bool)

	// Append bytes to the value of a key. Reduces to Put if the
	// key does not exist.
	Append(key []byte, value []byte)
}

// Index that can put in strict increasing order
type PutableInOrder interface {
	// put or override a key. panics if key arrive out of order.
	PutNext(key []byte, value []byte)
}

// General index
type Index interface {
	ROIndex
	Putable
}
