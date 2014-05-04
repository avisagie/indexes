package main

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"runtime"
	"time"

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

func main() {
	db, err := kc.Open("/dev/shm/cache.kch", kc.WRITE)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	start := time.Now().UnixNano()
	buf := &bytes.Buffer{}
	for count := int64(0); count < N; count++ {
		if count%1000 == 0 {
			fmt.Println(count, "/", N)
		}
		binary.Write(buf, binary.LittleEndian, count)
		b := buf.Bytes()
		db.SetBytes(b, b)
		buf.Reset()
	}

	end := time.Now().UnixNano()
	fmt.Println("elapsed: ", (end-start)/1000000, "ms")
	start = end
}
