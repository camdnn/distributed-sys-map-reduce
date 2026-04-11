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
)

func main() {

	client, _ := rpc.Dial("tcp", "localhost:7777")
	request := common.Request{}
	var response = common.Response{}

	for {
		if err := client.Call("CoordinatorAPI.RequestTask", request, &response); err != nil {
			log.Fatal("unable to connect to server:", err)
		}
		fmt.Printf("Response from server: %v\n", response)

		if response.Task.TaskType == "M" {
			// init intermediate files into a list
			files := openFilesForWriting(&response)
			if files == nil {
				log.Fatal("failed to init any intermediate files")
			}

			// create kv pairs
			kv := make(map[string]int)
			if err := mapping(&response, kv); err != nil {
				log.Fatal("Failed to map key value")
			}

			// write to the files
			err := writeToFile(files, kv, response.Task.R)
			if err != nil {
				log.Fatal("failed to open file %w", err)
			}

			clear(kv) //empties map, same allocation

		} else if response.Task.TaskType == "R" {

			// init intermediate files into a list
			filesWrite := openFilesForReading(&response)
			if filesWrite == nil {
				log.Fatal("failed to init any intermediate files")
			}

			reduced := make(map[string]int)
			if err := reducer(reduced, filesWrite); err != nil {
				log.Fatal("Error in reducer")
			}

			if err := commitFiles(reduced, response.Task.Filename); err != nil {
				log.Fatal("Error in commiting Files")
			}
		} else {
			log.Println("Error not valid task type")
		}
	}
}

// MAPPING
// Mapping function
// recieve a response, and a kv pair
func mapping(r *common.Response, kv map[string]int) error {
	// open the map function
	file, err := os.Open(r.Task.Filename)
	if err != nil {
		log.Println("OpenFile: ", err)
	}
	defer file.Close()

	// create key value pairs for each word
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		for word := range strings.FieldsSeq(line) {
			kv[word]++
		}
	}
	return nil
}

// reduced the kv pairs in the intermediate files
func reducer(reduced map[string]int, files []*os.File) error {
	// for each intermediate file decode the kv pairs and accumulate them
	for i := range files {
		decoder := json.NewDecoder(files[i])
		for decoder.More() {
			var kv KV
			if err := decoder.Decode(&kv); err != nil {
				return fmt.Errorf("decode error: %w", err)
			}
			reduced[kv.Key] += kv.Value
		}
	}

	// close all files after reading
	for _, f := range files {
		if err := f.Close(); err != nil {
			return fmt.Errorf("close intermediate file: %w", err)
		}
	}

	return nil
}

// Commit files to final output file
func commitFiles(reduced map[string]int, outputPath string) error {
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
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

	return nil
}

// initialize intermediate files for wokers
func openFilesForWriting(r *common.Response) []*os.File {
	files := make([]*os.File, r.Task.R)
	for i := range r.Task.R {
		filename := fmt.Sprintf("mr-%d-%d.json", r.Task.TaskID, i)

		fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {

			// clean up already opened files before returning
			for j := range i {
				files[j].Close()
			}
			log.Println("Failed to open intermediate file: ", i, err)
			return nil
		}

		files[i] = fd
	}
	return files
}

// initialize intermediate files for wokers
// // R = # of map
func openFilesForReading(r *common.Response) []*os.File {
	files := make([]*os.File, r.Task.R)
	for i := range r.Task.R {
		filename := fmt.Sprintf("mr-%d-%d.json", i, r.Task.TaskID)

		fd, err := os.Open(filename)
		if err != nil {

			// clean up already opened files before returning
			for j := range i {
				files[j].Close()
			}
			log.Println("Failed to open intermediate file: ", i, err)
			return nil
		}

		files[i] = fd
	}
	return files
}

type KV struct {
	Key   string
	Value int
}

// writiing the kv pairs to a intermediate file
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
			return fmt.Errorf("failed to encode key value pair: %w", err)
		}
	}

	// close all files after writing — reducer will re-open by name
	for _, f := range files {
		if err := f.Close(); err != nil {
			return fmt.Errorf("close intermediate file: %w", err)
		}
	}
	return nil
}

// hash func for intermediate files
// return hash with 0x7fffffff to clear sign bit
func ihash(key string) int {
	hash := fnv.New32a()
	hash.Write([]byte(key))
	return int(hash.Sum32() & 0x7fffffff)
}
