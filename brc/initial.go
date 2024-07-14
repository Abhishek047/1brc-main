package brc

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"runtime/pprof"
	"time"
)

func check(e error, msg string) {
	if e != nil {
		fmt.Println("error in", msg)
		panic(e)
	}
}

func First(fileName string) {
	runFor := 0
	pProfFile, err := os.Create("cpu.prof")
	check(err, "os.create")
	defer pProfFile.Close()

	err = pprof.StartCPUProfile(pProfFile)
	check(err, "pprof.StartCPUProfile")
	defer pprof.StopCPUProfile()
	filepath := "./" + fileName

	file, err := os.Open(filepath)
	check(err, "os.Open")
	start := time.Now()
	defer file.Close()
	reader := bufio.NewReader(file)
	result := make(map[string]measurementValues)
	// reader := bufio.NewReaderSize(file, sizeToRead)
	// // result := make(map[string]measurementValues)
	// // buffer := make([]byte, (sizeToRead / 2))

	// for {
	// 	str, err := reader.ReadString('\n')
	// 	if err != nil && err != io.EOF {
	// 		fmt.Println(err)
	// 		break
	// 	}
	// 	if len(str) == 0 {
	// 		break
	// 	}
	// 	fmt.Println(string(str))

	// }

	for {
		// str is a line split on new line
		str, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			fmt.Println(err)
			break
		}
		if len(str) == 0 {
			break
		}
		// index of ;
		n := 1
		var temp int16 = 0
		for i := len(str) - 2; i >= 0; i-- {
			if str[i] == 59 {
				value, ok := result[string(str[:i])]
				if ok {
					value.count++
					if value.max < temp {
						value.max = temp
					}
					if value.min > temp {
						value.min = temp
					}
					value.sum = value.sum + int64(temp)
					result[string(str[:i])] = value
				} else {
					result[string(str[:i])] = measurementValues{count: 1, max: temp, min: temp, sum: int64(temp)}
				}
				break
			} else {
				if str[i] != 46 && str[i] != 45 {
					temp = int16(n*int(str[i]-48)) + temp
					n *= 10
				}
				if str[i] == 45 {
					temp = 0 - temp
				}
			}
		}
		runFor++
		// if runFor == 100 {
		// 	break
		// }
	}
	fmt.Printf("%s took %v\n", "main", time.Since(start))
	// for key, val := range result {
	// 	fmt.Println("count ", val.count)
	// 	fmt.Printf("key : %s, min: %.1f, max: %.1f, avg: %.1f \n", key, float64(val.min)*0.1, float64(val.max)*0.1, float64(val.sum/int64(val.count))*0.1)
	// }
	fmt.Printf("%s took %v\n for results -> %d ", "main", time.Since(start), runFor)
}
