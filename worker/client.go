package main

import (
	"driver/common"
	"fmt"
	"net/rpc"
)

func main() {

	client, _ := rpc.Dial("tcp", "localhost:7777")

	request := common.Request{A: 4, B: 5}
	var response common.Response

	client.Call("CalculatorAPI.AddTwo", request, &response)

	fmt.Printf("Response from server: %v\n", response)
}
