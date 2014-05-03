indexes
=======

This is an experiment with data structures to serve as indexes. At
work have tens or hundreds of terabytes of time-series data on
disk. Once written, a record is immutable. The data is spread over a
few racks of machines with a bunch of disks each. We have to be able
to do very selective queries over some fields of the events.

That is where indexes come in. Obviously.

Currently we use LevelDB to create indexes that span shortish periods
of time, and query them in succession, depending on the query
criteria. LevelDB's write amplification along with the spinning rust
we're stuck with means that it indexes slowly. Some clever folks beat
it into submission with clever tricks, including calling compact twice
to ensure that it does not decide to occupy its single compaction
thread with compaction when you open the db to query.

RocksDB got it right. See their [benchmarks
page](https://github.com/facebook/rocksdb/wiki/Performance-Benchmarks).

So on to the purpose of this experiment... The usecase is to read a
bunch of data, sort it in RAM, dump it to disk once. The structure on
disk must be efficiently queryable, compact and written in a streaming
manner for our 4TB spinning disks.

The data is basically key-value, byte slices each (we use
[Go](http://golang.org)). We serialize such that the sort order of the
keys makes sense for the data type and for doing prefix queries.

Enter B+trees: the layout in RAM or on disk is reasonably compact,
even if the pages are in RAM, it doesn't bog the GC down with 10s or
100s of millions of objects and the in order bulk insert operation
lets us write it out to disk in a streaming way.

So the intention is to first use the b+tree to sort the keys in RAM,
and then iterate over it in order to build another b+tree on disk.

This first experiment is to see how fast it works for both operations.

May it amuse.


