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

	// add R value for response struct
	var response = common.Response{}

	client.Call("CoordinatorAPI.requestTask", request, &response)

	if response.Task.TaskType == "M" {
		kv := make(map[string]int)
		mapping(response, kv)
	} else {

	}

	fmt.Printf("Response from server: %v\n", response)
}

// Mapping function
// recieve a response, and a kv pair
func mapping(r common.Response, kv map[string]int) error {
	// open the map function
	file, err := os.Open(r.Task.Filename)
	if err != nil {
		log.Println("OpenFile: ", err)
		return err
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

	for key, value := range kv {
		fmt.Println("Key: ", key)
		fmt.Println("Value: ", value)
	}

	// init intermediate files into a list
	files := initFiles(r.Task.R)
	if files == nil {
		log.Fatal("failed to init any intermediate files")
	}

	// clean all files when done
	defer func() {
		for _, f := range files {
			if f != nil {
				f.Close()
			}
		}
	}()

	err = writeToFile(files, kv, r.Task.R)
	if err != nil {
		return fmt.Errorf("failed to open file %w", err)
	}

	return nil

}

// initialize intermediate files for wokers
func initFiles(R int) []*os.File {
	files := make([]*os.File, R)
	for i := range R {
		filename := fmt.Sprintf("intermediate_%d.json", i)

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

	for key, value := range kv {
		fileNo := ihash(key) % R
		err := encoders[fileNo].Encode(KV{Key: key, Value: value})
		if err != nil {
			return fmt.Errorf("failed to encode key value pair: %w", err)
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
