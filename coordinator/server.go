package main

import (
	"driver/common"
	"fmt"
	"net"
	"net/rpc"
	"os"
)

// type CalculatorAPI struct{}
type CoordinatorAPI struct{}

func (coordinator *CoordinatorAPI) setupWorker(request common.Request, response *common.Response) error {
	responseStruct := common.Response{}

}

//func (calculator *CalculatorAPI) AddTwo(request common.Request, response *common.Response) error {
//	response.R = request.A + request.B
//	fmt.Printf("request: %v, response:%v\n", request, response)
//	return nil
//}

func Coordinator(M int, R int, file *os.File) {
	// establish the RPC API
	coordinatorApi := new(CoordinatorAPI)
	rpc.Register(coordinatorApi)

	queue := make([]common.Request, 0, M+R)

	// spawn a thread looking for new connections
	go listenForWorkers()

	// while the queue isn't empty, loop
	for len(queue) > 0 {

	}

}

func listenForWorkers() {
	fmt.Println("listening")

	listener, _ := net.Listen("tcp", "localhost:7777")

	fmt.Println("Server is ready and waiting for connections on port 7777.")

	for {
		conn, _ := listener.Accept()
		fmt.Println("Connection accepted..")
		go rpc.ServeConn(conn)
	}

}
