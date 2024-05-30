package main

import (
	"flag"
	"fmt"
	"os"
	"sync"
	"time"
)

type resultData struct {
	throuput float64
	latency  float64
}

type calculatedData struct {
	id             int
	throuput_avg   float64
	latency_avg    float64
	throuput_best  float64
	latency_best   float64
	throuput_worst float64
	latency_worst  float64
}

// Write data to a file and measure the time.
func writeDataToFile(fileSize uint64, file *os.File, fileName string, blockSize uint64) ([]resultData, error) {
	var totalSize uint64 = 0
	var splitStart time.Time
	var splitEnd time.Duration
	var results []resultData

	data := make([]byte, blockSize)
	for totalSize < fileSize {
		writeSize := blockSize
		if fileSize-totalSize < blockSize {
			writeSize = fileSize - totalSize
		}
		splitStart = time.Now()
		n, err := file.Write(data[:writeSize])
		if err != nil {
			fmt.Println("Error: failed to write data: %s", fileName)
			file.Close()
			err := os.Remove(fileName)
			if err != nil {
				fmt.Println("Error: failed to remove file: %s", fileName)
			}
			return nil, err
		}
		splitEnd = time.Since(splitStart)
		results = append(results, resultData{
			throuput: float64(n) / 1000 / splitEnd.Seconds(),
			latency: float64(splitEnd.Seconds()) * 1000})
		totalSize += uint64(n)
	}
	return results, nil
}

// Calculate the average, best and worst values of throughput and latency. 
func calculateData(results []resultData, id int) calculatedData {
	var throuputSum float64 = 0
	var latencySum float64 = 0
	var throuputBest float64 = 0
	var latencyBest float64 = 0
	var throuputWorst float64 = 0
	var latencyWorst float64 = 0

	for _, result := range results {
		throuputSum += result.throuput
		latencySum += result.latency
		if throuputBest == 0 || result.throuput > throuputBest {
			throuputBest = result.throuput
		}
		if latencyBest == 0 || result.latency < latencyBest {
			latencyBest = result.latency
		}
		if throuputWorst == 0 || result.throuput < throuputWorst {
			throuputWorst = result.throuput
		}
		if latencyWorst == 0 || result.latency > latencyWorst {
			latencyWorst = result.latency
		}
	}
	return calculatedData{
		id:             id,
		throuput_avg:   throuputSum / float64(len(results)),
		latency_avg:    latencySum / float64(len(results)),
		throuput_best:  throuputBest,
		latency_best:   latencyBest,
		throuput_worst: throuputWorst,
		latency_worst:  latencyWorst,
	}
}

// Create a file and test it.
func processBenchmark(id int, blockSize uint64, fileSize uint64, waitGroup *sync.WaitGroup, ch chan calculatedData, sem chan struct{}) {
	defer waitGroup.Done()
	sem <- struct{}{}
	defer func() { <-sem }()

	fileName := fmt.Sprintf("benchmark_file_%d_%d", id, time.Now().Unix())
	file, err := os.OpenFile(fileName, os.O_CREATE | os.O_WRONLY | os.O_SYNC, 0644)
	if err != nil {
		fmt.Println("Error: failed to open file: %s", fileName)
		return
	}

	var results []resultData
	results, err = writeDataToFile(fileSize, file, fileName, blockSize)
	if err != nil {
		return
	}

	var calc calculatedData
	calc = calculateData(results, id)
	ch <- calc
	file.Close()
	err = os.Remove(fileName)
	if err != nil {
		fmt.Println("Error: failed to remove file: %s", fileName)
	}
}

// Processes the arguments, spawns a goroutine, and displays the results.
func main() {
	blockSize := flag.Uint64("blocksize", 512, "Blocksize in byte")
	fileSize := flag.Uint64("filesize", 1024, "Filesize in byte")
	numJobs := flag.Int("numjobs", 4, "The number of jobs to run in parallel")
	numTests := flag.Int("numtests", 4, "The number of times to run the test")
	flag.Parse()

	if *blockSize <= 0 {
		fmt.Println("Error: blocksize must be greater than 0")
		return
	}

	if *fileSize <= 0 {
		fmt.Println("Error: filesize must be greater than 0")
		return
	}

	if *numJobs < 1 {
		fmt.Println("Error: numJobs must be at least 1")
		return
	}

	if *numTests < 1 {
		fmt.Println("Error: numTests must be at least 1")
		return
	}

	fmt.Printf("blocksize: %d, filesize: %d, numjobs: %d numtests: %d\n", *blockSize, *fileSize, *numJobs, *numTests)
	var waitGroup sync.WaitGroup
	ch := make(chan calculatedData, *numTests)
	sem := make(chan struct{}, *numJobs)
	defer close(ch)

	for i := 0; i < *numTests; i++ {
		waitGroup.Add(1)
		go processBenchmark(i, *blockSize, *fileSize, &waitGroup, ch, sem)
	}
	waitGroup.Wait()
	for i := 0; i < *numTests; i++ {
		calc := <-ch
		fmt.Printf("ID: %d\n", calc.id)
		fmt.Printf("Throughput_avg: %.3f kb/s, Throughput_best: %.3f kb/s, Throughput_worst: %.3f kb/s\n", calc.throuput_avg, calc.throuput_best, calc.throuput_worst)
		fmt.Printf("Latency_avg: %.3f ms, Latency_best: %.3f ms, Latency_worst: %.3f ms\n", calc.latency_avg, calc.latency_best, calc.latency_worst)
	}
}
