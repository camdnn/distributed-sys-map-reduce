package main

import (
	"bufio"
	"driver/common"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

// the coordinatorAPI along with all relevant information
type CoordinatorAPI struct {
	mu    sync.Mutex    // mutex lock
	tasks []common.Task // the task queue
}

// IN PROG: used by idle workers
func (coordinator *CoordinatorAPI) requestTask(request common.Request, response *common.Response) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return nil
}

func Coordinator(M int, R int, file *os.File) {

	// establish all task information before initializing the RPC
	lines, _ := getNonEmptyLines(file)

	fmt.Println("File lines = %d\n", len(lines))

	num_splits := len(lines) / M

	fmt.Println("Number of splits = %d\n", num_splits)

	taskQueue := make([]common.Task, 0, M+R)

	for i := 0; i < M; i++ {
		start := i * num_splits
		end := start + num_splits
		if i == M-1 {
			// the last file might not align perfectly, so it'll be a little smaller
			end = len(lines)
		}
		makeMFile(i, lines[start:end])
	}

	// establish the RPC API
	coordinatorApi := new(CoordinatorAPI)
	rpc.Register(coordinatorApi)

	// spawn a thread looking for new connections
	go listenForWorkers()

	// while the queue isn't empty, loop
	for len(taskQueue) > 0 {

	}

}

// make a M file and append its lines
func makeMFile(id int, lines []string) {
	filename := "../intermediate/file_" + strconv.Itoa(id)
	fd, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer fd.Close()

	for i, line := range lines {
		if i+1 == len(lines) {
			fd.WriteString(line)
		} else {
			fd.WriteString(line + "\n")
		}
	}
}

func getNonEmptyLines(f *os.File) ([]string, error) {
	scanner := bufio.NewScanner(f)
	var lines []string

	for scanner.Scan() {
		line := scanner.Text()

		if len(line) > 0 {
			lines = append(lines, line)
		}
	}

	return lines, scanner.Err()
}

// // get the non-empty line count of the file
// func getLineCount(f *os.File) (int, error) {
// 	scanner := bufio.NewScanner(f)
// 	count := 0

// 	for scanner.Scan() {
// 		if len(scanner.Text()) != 0 {
// 			count++
// 		}
// 	}

// 	// going through the file would bring the file ptr to the end
// 	// so seeking would put it back at the beginning
// 	f.Seek(0, io.SeekStart)

// 	return count, scanner.Err()
// }

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
