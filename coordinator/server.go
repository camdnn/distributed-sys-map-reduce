package main

import (
	"driver/common"
	"fmt"
	"net"
	"net/rpc"
	"os"
)

type CalculatorAPI struct{}

func (calculator *CalculatorAPI) AddTwo(request common.Request, response *common.Response) error {
	response.R = request.A + request.B
	fmt.Printf("request: %v, response:%v\n", request, response)
	return nil
}

func Coordinator(M int, R int, file *os.File) {
	calculator := new(CalculatorAPI)
	rpc.Register(calculator)

	listener, _ := net.Listen("tcp", "localhost:7777")

	fmt.Println("Server is ready and waiting for connections on port 7777.")

	for {
		conn, _ := listener.Accept()
		fmt.Println("Connection accepted..")
		go rpc.ServeConn(conn)
	}
}
