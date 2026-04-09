package main

import (
	"bufio"
	"driver/common"
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

	if response.FunType == "M" {
		kv := make(map[string]int)
		mapping(response, kv)
	} else {

	}

	fmt.Printf("Response from server: %v\n", response)
}

// Mapping function
// recieve a response, and a kv pair
func mapping(r common.Response, kv map[string]int) error {
	file, err := os.Open(r.Filename)
	if err != nil {
		log.Println("OpenFile: ", err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		for word := range strings.FieldsSeq(line) {
			kv[word]++
		}
	}

	for key, value := range kv {
		fileNo := ihash(key) % r.R

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
