package main

import (
	"bytes"
	"encoding/binary"
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
		ok, v := index.Get(k)
		if !ok || bytes.Compare(v, k) != 0 {
			fmt.Println("bad:", ok, k, v)
		}
		buf.Reset()
	}
	end := time.Now().UnixNano()
	fmt.Println("Spot check of", spotCheckN, "elements took", (end-start)/1000, "us,", (end-start)/spotCheckN, "ns per lookup.")
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
		index.Put(buf.Bytes(), buf.Bytes())
		buf.Reset()
	}

	end := time.Now().UnixNano()
	fmt.Println("elapsed: ", (end-start)/1000000, "ms")
	start = end

	runtime.GC()

	fmt.Println(index.(*btree.Btree).Stats())

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
		ok, k, v := iter.Next()
		if !ok {
			break
		}
		index2.PutNext(k, v)
	}

	fmt.Println(index.Size(), index2.Size())

	fmt.Println(index2.Stats())

	index = nil

	runtime.GC()
	end = time.Now().UnixNano()
	fmt.Println("elapsed:", (end-start)/1000000, "ms")

	pprof.StopCPUProfile()

	runtime.GC()
	printMem()

	spotCheck(index2)
	runtime.GC()
	printMem()

	spotCheck(index2)
	spotCheck(index2)
	spotCheck(index2)
}
