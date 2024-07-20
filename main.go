package main

import (
	"flag"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"time"

	"github.com/abhishek047/1brc/brc"
)

var cpuProfile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memProfile = flag.String("memprofile", "", "write memory profile to `file`")

const (
	realDeal = "measurements.txt"
	bigTest  = "test_10_000_000.txt"
	small    = "test_100.txt"
)

func main() {
	flag.Parse()
	if *cpuProfile != "" {
		f, err := os.Create(*cpuProfile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}
	// brc.First(realDeal)
	// brc.Second(realDeal)
	// brc.Third(realDeal)
	// brc.Fourth(realDeal)
	// brc.First(bigTest)
	// brc.First(small)
	// brc.Measure(bigTest)
	brc.Measure(realDeal)
	if *memProfile != "" {
		f, err := os.Create(*memProfile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
	time.Sleep(time.Second)
}

// go tool pprof -http=:8080 ./cpu.prof
// go run main.go --cpuprofile cpu.prof
