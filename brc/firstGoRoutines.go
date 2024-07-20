package brc

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime"
	"sync"
	"time"
)

func Third(fileName string) {
	readFileT3(fileName)

}
func readFileT3(fileName string) {
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
			lastNewLinePos := findLastNewLineT3(&tempBuffer)
			fileEnd = int64(lastNewLinePos) + tempFileEnd + 1
		}
		// fmt.Printf("running for %v, from %v to %v  \n", loopMainI, fileStart, fileEnd)
		wg.Add(1)
		go processFileT3(filepath, fileStart, fileEnd, &wg, allChanResults)
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
	}()
	wg.Wait()
	close(allChanResults)
}

func processFileT3(
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
	fmt.Println(count)
	chanResult <- result
}

func findLastNewLineT3(buffer *[]byte) int {
	for i := len(*buffer) - 1; i >= 0; i-- {
		if (*buffer)[i] == '\n' {
			return i
		}
	}
	return len(*buffer) - 1
}
