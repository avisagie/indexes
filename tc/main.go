package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"runtime"
	"time"

	"github.com/avisagie/indexes"
	"github.com/cloudflare/gokabinet/kc"
)

func printMem() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	fmt.Println("Memory in use:", memStats.HeapAlloc/1000000, "MB")
}

const (
	N          = 10000000
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
	db, err := kc.Open("/dev/shm/cache.kch", kc.WRITE)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	start := time.Now().UnixNano()
	buf := &bytes.Buffer{}
	for count := int64(0); count < N; count++ {
		binary.Write(buf, binary.LittleEndian, count)
		db.Set(string(buf.Bytes()), string(buf.Bytes()))
		buf.Reset()
	}

	end := time.Now().UnixNano()
	fmt.Println("elapsed: ", (end-start)/1000000, "ms")
	start = end
}
