package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/avisagie/indexes"
	"github.com/avisagie/indexes/btree"
)

func printMem() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Println("Memory in use:", memStats.HeapAlloc/1000000, "MB")
}

const (
	N          = 1000000
	spotCheckN = 10000
)

func spotCheck(index indexes.Index) {
	buf := &bytes.Buffer{}
	start := time.Now().UnixNano()
	for i := 0; i < spotCheckN; i++ {
		x := rand.Int63n(N)
		binary.Write(buf, binary.LittleEndian, x)
		k := buf.Bytes()
		v, ok := index.Get(k)
		if !ok || bytes.Compare(v, k) != 0 {
			fmt.Println("bad:", ok, k, v)
		}
		buf.Reset()
	}
	end := time.Now().UnixNano()
	fmt.Println("Spot check of", spotCheckN, "elements took", (end-start)/1000, "us,", (end-start)/spotCheckN, "ns per lookup.")
}

func printStats(stats btree.BtreeStats) {
	s, e := json.MarshalIndent(stats, "", "\t")
	if e != nil {
		panic(e)
	}
	fmt.Println(string(s))
}

func main() {
	out, err := os.Create("prof")
	if err != nil {
		panic(err)
	}
	defer out.Close()

	index := btree.NewInMemoryBtree()
	buf := &bytes.Buffer{}

	start := time.Now().UnixNano()
	for count := int64(0); count < N; count++ {
		binary.Write(buf, binary.LittleEndian, count)
		b := buf.Bytes()
		index.Put(b, b)
		buf.Reset()
	}

	end := time.Now().UnixNano()
	fmt.Println("elapsed: ", (end-start)/1000000, "ms")
	start = end

	runtime.GC()

	printStats(index.(*btree.Btree).Stats())

	//index.(*btree.Btree).Dump(os.Stdout)
	if err := index.(*btree.Btree).CheckConsistency(); err != nil {
		panic(err)
	}

	fmt.Println("GC'd. Elapsed: ", (end-start)/1000000, "ms")
	printMem()

	spotCheck(index)
	runtime.GC()

	pprof.StartCPUProfile(out)
	start = time.Now().UnixNano()
	index2 := btree.NewInMemoryBtree().(*btree.Btree)
	iter := index.Start([]byte{})
	for {
		k, v, ok := iter.Next()
		if !ok {
			break
		}
		index2.PutNext(k, v)
	}

	pprof.StopCPUProfile()

	if index.Size() != index2.Size() {
		panic(fmt.Sprint("Sizes differ, ", index.Size, " vs ", index2.Size()))
	}

	printStats(index2.Stats())

	index = nil

	runtime.GC()
	end = time.Now().UnixNano()
	fmt.Println("elapsed:", (end-start)/1000000, "ms")

	runtime.GC()
	printMem()

	spotCheck(index2)
	runtime.GC()
	printMem()

	for ii := 0; ii < 10; ii++ {
		spotCheck(index2)
	}

}
