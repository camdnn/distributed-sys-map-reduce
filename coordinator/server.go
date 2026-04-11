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
	"strings"
	"sync"
)

// the coordinatorAPI along with all relevant information
type CoordinatorAPI struct {
	mu         *sync.Mutex   // mutex lock
	tasks      []common.Task // the task queue
	inProgress []common.Task // list of tasks that are in prog
	R          int
}

// get the R value from the coordinator to the worker
func (c *CoordinatorAPI) getR(request common.Request, response *int) error {
	*response = c.R
	return nil
}

// Used by idle workers
func (coordinator *CoordinatorAPI) RequestTask(request common.Request, response *common.Response) error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	// get the first task from the queue
	task, tasks, isValid := getTask(coordinator.tasks)

	if !isValid {
		return fmt.Errorf("no tasks availiable")
	}

	// update the queue in the coordinator
	coordinator.tasks = tasks

	// set the task to be in progress
	task.InProgress = true

	// add the task to inProgress
	coordinator.inProgress = append(coordinator.inProgress, task)

	// send the task in the response
	response.Task = task
	return nil
}

// returns task, updated queue, and if it's a valid task
func getTask(queue []common.Task) (common.Task, []common.Task, bool) {
	// pop off the front of the queue
	if len(queue) > 0 {
		t := queue[0]

		// reassign queue to equal everything after the first element
		// this acts like popping off the front
		queue = queue[1:]

		return t, queue, true
	}

	// if there's nothing in the queue, ret an empty task
	return common.Task{}, queue, false
}

func Coordinator(M int, R int, file *os.File) {

	// establish all task information before initializing the RPC
	lines, _ := getNonEmptyLines(file)

	fmt.Println("File lines = %d\n", len(lines))

	num_splits := len(lines) / M

	fmt.Println("Number of splits = %d\n", num_splits)

	total_tasks := M + R

	// make the queue and populate it
	taskQueue := make([]common.Task, 0, total_tasks)

	for i := 0; i < total_tasks; i++ {
		var t common.Task
		if i < M {
			t = common.Task{
				TaskID:     i,
				TaskType:   "M",
				InProgress: false,
				Filename:   fmt.Sprintf("../splits/split_p%d", i),
				R:          R,
			}

		} else {
			t = common.Task{
				TaskID:     i,
				TaskType:   "R",
				InProgress: false,
				Filename:   "../output.txt",
				R:          R,
			}
		}

		// append the task to the queue
		taskQueue = append(taskQueue, t)

	}

	// make the split files
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
	coordinatorApi.inProgress = make([]common.Task, 0)
	coordinatorApi.mu = new(sync.Mutex)
	coordinatorApi.R = R
	coordinatorApi.tasks = taskQueue

	// register it
	rpc.Register(coordinatorApi)

	// spawn a thread looking for new connections
	go listenForWorkers()

	// while the queue isn't empty, loop
	for len(taskQueue) > 0 {

	}

	// queue must be empty, so return

	return

}

// make a M file and append its lines
func makeMFile(id int, lines []string) {
	filename := "../splits/split_p" + strconv.Itoa(id)
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

		words := strings.Split(line, " ")

		// lower all the words
		for i := range len(words) {
			words[i] = strings.ToLower(words[i])
		}

		line = strings.Join(words, " ")

		if len(line) > 0 {
			lines = append(lines, line)
		}
	}

	return lines, scanner.Err()
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
