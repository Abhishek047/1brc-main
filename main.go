package main

import (
	"github.com/abhishek047/1brc/brc"
)

// step 4.
// run the file on go routines for parallel work
// step 3.
// a new hashing function
// measurements.txt

const (
	realDeal = "measurements.txt"
	bigTest  = "test_10_000_000.txt"
	small    = "test_100.txt"
)

func main() {
	// brc.First(realDeal)
	// brc.First(bigTest)
	// brc.Measure(small)
	// brc.Measure(bigTest)
	brc.Measure(realDeal)
}

// go tool pprof -http=:8080 ./cpu.prof
// go run main.go --cpuprofile cpu.prof
