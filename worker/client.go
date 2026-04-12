package main

import (
	"bufio"
	"driver/common"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type KV struct {
	Key   string
	Value int
}

func main() {

	client, _ := rpc.Dial("tcp", "localhost:7777")
	request := common.Request{}

	for {
		var response = common.Response{}

		if err := client.Call("CoordinatorAPI.RequestTask", request, &response); err != nil {
			// log error
			log.Printf("WARN: RPC RequestTask failed: %v — retrying in 2s", err)
			// sleep for 2 seconsd and retry
			time.Sleep(2 * time.Second)
			continue
		}

		// "Done" means the coordinator has no more work — clean shutdown.
		if response.Task.Done {
			log.Printf("INFO: worker received shutdown signal, exiting")
			return
		}

		// print the response from the server
		task := response.Task
		fmt.Printf("Response from server worker: %d, task: %s, filepath: %s, R: %d, M: %d\n", task.TaskId, task.TaskType, task.Filename, task.R, task.M)
		switch task.TaskType {
		case "M":
			if err := runMapper(&response); err != nil {
				log.Printf("Error mapper task %d has failed %v", response.Task.TaskId, err)
			}

		case "R":
			if err := runReducer(&response); err != nil {
				log.Printf("ERROR: reducer task %d failed: %v", task.TaskId, err)
				time.Sleep(1 * time.Second)
			}
		default:
			log.Fatalf("FATAL: unknown task type %q in task %d", task.TaskType, task.TaskId)
		}
	}
}

// Mapper  -----------------------------------------------------

// runMapper orchestrates the map phase for a single task.
func runMapper(r *common.Response) error {

	// 1. Count word freq from split file
	kv := make(map[string]int)
	if err := mapping(r, kv); err != nil {
		log.Fatal("Failed to map key value")
	}

	// 2. Open intermediate files for writing
	files, err := openFilesForWriting(r)
	if err != nil {
		return fmt.Errorf("open intermediate files: %w", err)
	}
	// 3. Write to intermediate files with hash partitioning
	if err := writeToFile(files, kv, r.Task.R); err != nil {
		log.Fatal("failed to open file %w", err)
	}

	return nil
}

// mapping reads the split file and populates kv with word counts
func mapping(r *common.Response, kv map[string]int) error {

	// open split files
	file, err := os.Open(r.Task.Filename)
	if err != nil {
		return fmt.Errorf("open split %q: %w", r.Task.Filename, err)
	}
	defer file.Close()

	// for each word increase the freq count
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		for word := range strings.FieldsSeq(scanner.Text()) {
			kv[word]++
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan split %q: %w", r.Task.Filename, err)
	}
	return nil
}

// writeToFile from hash-partitions each KV pair into one of R intermediate files
func writeToFile(files []*os.File, kv map[string]int, R int) error {

	// init encoders for each files
	encoders := make([]*json.Encoder, R)
	for i, file := range files {
		encoders[i] = json.NewEncoder(file)
	}

	// for each key valye get a has key for the file and print to that file
	for key, value := range kv {
		fileNo := ihash(key) % R
		err := encoders[fileNo].Encode(KV{Key: key, Value: value})
		if err != nil {
			// Close everything we can before returning
			for _, f := range files {
				f.Close()
			}
			return fmt.Errorf("encode KV %q: %w", key, err)
		}
	}

	// close all files after writing — reducer will re-open by name
	var closeError error
	for _, f := range files {
		if err := f.Close(); err != nil {
			closeError = fmt.Errorf("close intermediate file: %w", err)
		}
	}
	return closeError
}

// initialize intermediate files for wokers, mr-X-Y.json intermediate files
func openFilesForWriting(r *common.Response) ([]*os.File, error) {
	files := make([]*os.File, r.Task.R)
	for i := range r.Task.R {
		filename := fmt.Sprintf("mr-%d-%d.json", r.Task.TaskId, i)
		fmt.Printf("opened %s for writing\n", filename)

		fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {

			// clean up already opened files before returning
			for j := range i {
				files[j].Close()
				os.Remove(fmt.Sprintf("mr-%d-%d.json", r.Task.TaskId, j))
			}
			return nil, fmt.Errorf("create intermediate file %d: %w", i, err)
		}

		files[i] = fd
	}
	return files, nil
}

// Reducer ===========================================================

// orchestrates the reduce phase for a single task
func runReducer(r *common.Response) error {

	// 1. Open intermediate files written by mappers
	files, err := openFilesForReading(r)
	if err != nil {
		return fmt.Errorf("open intermediate files for reading: %w", err)
	}

	// 2. Decode and accumulate all KV pairs
	reduced := make(map[string]int)
	if err := reducer(reduced, files); err != nil {
		return fmt.Errorf("reduce: %w", err)
	}

	// 3. Commit files
	if err := commitFiles(reduced, r.Task.Filename); err != nil {
		return fmt.Errorf("commit output: %w", err)
	}

	return nil

}

// reduced the kv pairs in the intermediate files
func reducer(reduced map[string]int, files []*os.File) error {
	// for each intermediate file decode the kv pairs and accumulate them
	for _, file := range files {
		decoder := json.NewDecoder(file)
		for decoder.More() {
			var kv KV
			if err := decoder.Decode(&kv); err != nil {
				return fmt.Errorf("decode error from %s: %w", file.Name(), err)
			}
			reduced[kv.Key] += kv.Value
		}
	}

	// close all files after reading
	var closeErr error
	for _, f := range files {
		fmt.Println(f)
		if err := f.Close(); err != nil {
			closeErr = fmt.Errorf("close intermediate file: %w", err)
		}
	}

	return closeErr
}

// Commit files writes reduced values to output file
func commitFiles(reduced map[string]int, outputPath string) error {
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("open output file error: %w", err)
	}
	defer file.Close()

	outFile := json.NewEncoder(file)
	for key, value := range reduced {
		if err := outFile.Encode(KV{Key: key, Value: value}); err != nil {
			fmt.Println("error encoding key value pair")
		}
	}

	fmt.Println("End commit")

	return nil
}

// initialize intermediate files for wokers
// // R = # of map
func openFilesForReading(r *common.Response) ([]*os.File, error) {
	files := make([]*os.File, r.Task.M)
	for i := range r.Task.M {
		filename := fmt.Sprintf("mr-%d-%d.json", i, r.Task.TaskId)
		fmt.Printf("opened %s for reading\n", filename)

		fd, err := os.Open(filename)
		if err != nil {

			// clean up already opened files before returning
			for j := range i {
				files[j].Close()
			}
			return nil, fmt.Errorf("open %q: %w", filename, err)
		}

		files[i] = fd
	}
	return files, nil
}

// hash func for intermediate files
// return hash with 0x7fffffff to clear sign bit
func ihash(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32() & 0x7fffffff)
}
