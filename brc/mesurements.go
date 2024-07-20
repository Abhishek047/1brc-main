package brc

import (
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

type measurementValues struct {
	min   int16
	max   int16
	sum   int64
	count uint16
}

type hashItem struct {
	val   []byte
	sum   uint64
	stats *measurementValues
}

const (
	offset64 = 14695981039346656037
	prime64  = 1099511628211
)

func Measure(fileName string) {
	readFile(fileName)
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
	numGoRoutines := runtime.NumCPU() * 2
	if fileInfo.Size() < 4096*4096 {
		numGoRoutines = 2
	}
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
		for key, station := range totalStations {
			var avg int64 = 0
			if station.sum != 0 {
				avg = station.sum / int64(station.count)
			} else {
				fmt.Println(key, station)
				fmt.Scanln()
			}
			fmt.Printf("key : %s, min: %.1f, max: %.1f, avg: %.1f \n", key, float64(station.min)*0.1, float64(station.max)*0.1, float64(avg)*0.1)
		}
		fmt.Println("Time taken -> ", time.Since(start))
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
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	numBucket := 1 << 17
	hashTable := make([]hashItem, numBucket)
	tableSize := 0
	count := 0
	_, err = file.Seek(start, 0)
	if err != nil {
		panic(err)
	}
	bufferSize := 1024 * 1024
	if filePath == "./test_100.txt" {
		bufferSize = 250
	}
	buffer := make([]byte, bufferSize)
	var i int64 = 0
	var num int16 = 0
	isNeg, ciFound := false, false
	name := make([]byte, 50)
	nameI, collision := 0, 0
	var hash, strSum uint64 = offset64, 1
	for i < (end - start) {
		_, err = file.Read(buffer)
		if err != nil && err != io.EOF {
			panic(err)
		}
		j := 0
		for j < len(buffer) {
			if i >= (end - start) {
				break
			}
			char := buffer[j]
			if char == 59 {
				ciFound = true
				i++
				j++
				continue
			}
			if char == 45 {
				isNeg = true
				i++
				j++
				continue
			}
			if !ciFound {
				name[nameI] = char
				strSum += uint64(nameI) * uint64(char)
				hash ^= uint64(char)
				hash *= prime64
				nameI++
			} else {
				hashIndex := int(hash & uint64(numBucket-1))
				if char >= 48 && char <= 57 {
					num = (num * 10) + int16(char-48)
				}
				if char == 10 && ciFound {
					count++
					if isNeg {
						num = -num
					}
					for {
						if hashTable[hashIndex].val == nil {
							key := make([]byte, len(name[:nameI]))
							copy(key, name[:nameI])
							hashTable[hashIndex] = hashItem{
								val: key,
								sum: strSum,
								stats: &measurementValues{
									count: 1, max: num, min: num, sum: int64(num),
								},
							}
							tableSize++
							if tableSize > numBucket/2 {
								fmt.Println(tableSize, numBucket, string(key))
								panic("too many items in hash table")
							}
							break
						} else {
							collision++
						}
						if hashTable[hashIndex].sum == strSum {
							s := hashTable[hashIndex].stats
							s.min = min(s.min, num)
							s.max = max(s.max, num)
							s.sum += int64(num)
							s.count++
							break
						}
						hashIndex++
						if hashIndex >= numBucket {
							hashIndex = 0
						}
					}
					nameI, num, ciFound = 0, 0, false
					hash, strSum = offset64, 1
				}
			}
			i++
			j++
		}
	}
	if ciFound {
		count++
		if isNeg {
			num = -num
		}
		hashIndex := int(hash & uint64(numBucket-1))
		for {
			if hashTable[hashIndex].val == nil {
				key := make([]byte, len(name[:nameI]))
				copy(key, name[:nameI])
				hashTable[hashIndex] = hashItem{
					val: key,
					sum: strSum,
					stats: &measurementValues{
						count: 1, max: num, min: num, sum: int64(num),
					},
				}
				tableSize++
				if tableSize > numBucket/2 {
					panic("too many items in hash table")
				}
				break
			} else {
				collision++
			}
			if hashTable[hashIndex].sum == strSum {
				s := hashTable[hashIndex].stats
				s.min = min(s.min, num)
				s.max = max(s.max, num)
				s.sum += int64(num)
				s.count++
				break
			}
			hashIndex++
			if hashIndex >= numBucket {
				hashIndex = 0
			}
		}
		nameI, num, ciFound = 0, 0, false
		hash = offset64
	}
	result := make(map[string]measurementValues, tableSize)
	for _, value := range hashTable {
		if value.val == nil {
			continue
		}
		result[string(value.val)] = measurementValues{
			max:   value.stats.max,
			min:   value.stats.min,
			count: value.stats.count,
			sum:   value.stats.sum,
		}
	}
	fmt.Println(count, "total")
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
