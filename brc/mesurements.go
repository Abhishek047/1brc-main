package brc

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"
)

var cpuProfile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memProfile = flag.String("memprofile", "", "write memory profile to `file`")

type measurementValues struct {
	min   int16
	max   int16
	sum   int64
	count uint16
}

// type hashItem struct {
// 	val   []byte
// 	stats *measurementValues
// }

// const (
// 	offset64 = 14695981039346656037
// 	prime64  = 1099511628211
// )

// Note 1. Need a custom hash function for the above results
// Note 2. Need to change the Scanner in the processFile
func Measure(fileName string) {
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

	readFile(fileName)
	time.Sleep(time.Second)
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
}
func readFile(fileName string) {
	start := time.Now()
	filepath := "./" + fileName
	fileInfo, err := os.Stat(filepath)
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}
	// constants for file splitting
	numGoRoutines := runtime.NumCPU() * 4
	if fileInfo.Size() < 4096*4096 {
		numGoRoutines = 2
	}
	// baseChunkSize := 4096 * 4096
	baseChunkSize := fileInfo.Size() / int64(numGoRoutines)

	var wg sync.WaitGroup

	reader, err := os.Open(filepath)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer reader.Close()

	allChanResults := make(chan map[string]measurementValues)

	var fileStart int64 = 0
	var loopMainI int = 0
	for loopMainI < numGoRoutines {
		// finding from where to run
		fileEnd := fileStart + int64(baseChunkSize)
		if loopMainI == numGoRoutines-1 {
			fileEnd = fileInfo.Size()
		} else {
			tempBufferLen := 100
			tempBuffer := make([]byte, tempBufferLen)
			tempFileEnd := fileEnd - int64(tempBufferLen)
			fmt.Println(fileEnd - int64(tempBufferLen))
			_, err = reader.ReadAt(tempBuffer, fileEnd-int64(tempBufferLen))
			if err != nil && err != io.EOF {
				fmt.Println("Error in chunk read -> ", loopMainI, err)
				return
			}
			lastNewLinePos := findLastNewLine(&tempBuffer)
			fileEnd = int64(lastNewLinePos) + tempFileEnd + 1
		}
		// fmt.Printf("running for %v, from %v to %v  \n", loopMainI, fileStart, fileEnd)
		wg.Add(1)
		go processFile(filepath, fileStart, fileEnd, &wg, allChanResults)
		fileStart = fileEnd
		loopMainI++
	}

	go func() {
		totalStations := make(map[string]measurementValues)
		fmt.Println("Joining all...")
		for result := range allChanResults {
			for res := range result {
				val, ok := totalStations[res]
				if ok {
					val.count = val.count + result[res].count
					val.sum = val.sum + result[res].sum
					val.max = max(val.max, result[res].max)
					val.min = min(val.min, result[res].min)
					totalStations[res] = val
				} else {
					totalStations[res] = result[res]
				}
			}
		}
		fmt.Println("Joining Done!")
		fmt.Println("Time taken before print-> ", time.Since(start))
		// for key, station := range totalStations {
		// 	var avg int64 = 0
		// 	if station.sum != 0 {
		// 		avg = station.sum / int64(station.count)
		// 	} else {
		// 		fmt.Println(key, station)
		// 		fmt.Scanln()
		// 	}
		// 	fmt.Printf("key : %s, min: %.1f, max: %.1f, avg: %.1f \n", key, float64(station.min)*0.1, float64(station.max)*0.1, float64(avg)*0.1)
		// }
		// fmt.Println("Time taken -> ", time.Since(start))
	}()
	wg.Wait()
	close(allChanResults)
}

func processFile(
	filePath string,
	start int64,
	end int64,
	wg *sync.WaitGroup,
	chanResult chan<- map[string]measurementValues,
) {
	file, err := os.Open(filePath)
	defer wg.Done()
	defer file.Close()
	// buckets := make([]hashItem, numBuckets)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	count := 0
	result := make(map[string]measurementValues)
	f := io.LimitedReader{R: file, N: end - start}

	_, err = file.Seek(start, 0)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(&f)
	for scanner.Scan() {
		bytes := scanner.Bytes()
		if len(bytes) == 0 {
			break
		}
		var temp int16 = 0
		var num int16 = 1
		for i := len(bytes) - 2; i >= 0; i-- {
			if bytes[i] == 59 {

				value, ok := result[string(bytes[:i])]
				if ok {
					value.count++
					if value.max < temp {
						value.max = temp
					}
					if value.min > temp {
						value.min = temp
					}
					value.sum = value.sum + int64(temp)
					result[string(bytes[:i])] = value
				} else {
					count++
					result[string(bytes[:i])] = measurementValues{count: 1, max: temp, min: temp, sum: int64(temp)}
				}
				break
			} else {
				if bytes[i] != 46 && bytes[i] != 45 {
					temp = int16(num*int16(bytes[i]-48)) + temp
					num *= 10
				}
				if bytes[i] == 45 {
					temp = 0 - temp
				}
			}
		}
	}
	// Processing
	// replace with end-start
	// for loops*bufferSize < (end - start) {
	// 	_, err = file.Read(buffer)
	// 	if err != nil && err != io.EOF {
	// 		fmt.Println("Error in reading: here", err)
	// 		return
	// 	}
	// 	loops++
	// 	for loopI < len(buffer) {
	// 		if start == 0 {
	// 			fmt.Print(string(buffer[loopI]))
	// 		}
	// 		if startI == -1 {
	// 			startI = loopI
	// 		}
	// 		// is the current index is Semi-colon
	// 		if buffer[loopI] == 59 {
	// 			colonI = loopI
	// 			loopI++
	// 		}
	// 		if colonI == -1 {
	// 			// Do hashing here
	// 		} else {
	// 			if buffer[loopI] >= 48 && buffer[loopI] <= 57 {
	// 				num = (num * 10) + int16(buffer[loopI]-48)
	// 			}
	// 			if buffer[loopI] == 10 {
	// 				count++
	// 				var temp int16 = 0
	// 				if buffer[colonI+1] == 45 {
	// 					temp = 0 - num
	// 				} else {
	// 					temp = num
	// 				}
	// 				value, ok := result[string(buffer[startI:colonI])]
	// 				if ok {
	// 					value.count++
	// 					if value.max < temp {
	// 						value.max = temp
	// 					}
	// 					if value.min > temp {
	// 						value.min = temp
	// 					}
	// 					value.sum = value.sum + int64(temp)
	// 					result[string(buffer[startI:colonI])] = value
	// 				} else {
	// 					result[string(buffer[startI:colonI])] = measurementValues{count: 1, max: temp, min: temp, sum: int64(temp)}
	// 				}
	// 				startI, colonI, num = -1, -1, 0
	// 			}
	// 		}
	// 		loopI++
	// 	}
	// }
	// if colonI > 0 {
	// 	count++
	// 	var temp int16 = 0
	// 	if buffer[colonI+1] == 45 {
	// 		temp = 0 - num
	// 	} else {
	// 		temp = num
	// 	}
	// 	value, ok := result[string(buffer[startI:colonI])]
	// 	if ok {
	// 		value.count++
	// 		if value.max < temp {
	// 			value.max = temp
	// 		}
	// 		if value.min > temp {
	// 			value.min = temp
	// 		}
	// 		value.sum = value.sum + int64(temp)
	// 		result[string(buffer[startI:colonI])] = value
	// 	} else {
	// 		result[string(buffer[startI:colonI])] = measurementValues{count: 1, max: temp, min: temp, sum: int64(temp)}
	// 	}
	// 	startI, colonI, num = -1, -1, 0
	// }
	fmt.Println(count)
	chanResult <- result
}

func findLastNewLine(buffer *[]byte) int {
	for i := len(*buffer) - 1; i >= 0; i-- {
		if (*buffer)[i] == '\n' {
			return i
		}
	}
	return len(*buffer) - 1
}
