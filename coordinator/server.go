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
	"time"
)

var done = false

// the coordinatorAPI along with all relevant information
type CoordinatorAPI struct {
	mu             *sync.Mutex       // mutex lock
	mTasks         *[]common.Task    // the task queue
	rTasks         *[]common.Task    // the task queue
	inProgress     map[int]time.Time // maps worker id to its job to its start time
	workersToTasks map[int][]common.Task
	R              int
	M              int
}

// Used by idle workers
func (c *CoordinatorAPI) RequestTask(request common.Request, response *common.Response) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// remove the recent job from the in progress list,
	// since the worker is asking for another task
	// 	it must be done with the last one
	delete(c.inProgress, request.WorkerID)

	// get the first task from the queue
	task, mtasks, rtasks, isValid := getTask(c.mTasks, c.rTasks)

	if !isValid {
		if len(c.inProgress) == 0 {
			response.Task = common.Task{
				TaskId:   -1,
				TaskType: "",
				Done:     true,
				Filename: "",
				R:        -1,
				M:        -1,
			}
		}
		return fmt.Errorf("no tasks availiable")
	}

	// update the queue in the coordinator
	c.mTasks = mtasks
	c.rTasks = rtasks

	// add the task to inProgress
	c.inProgress[request.WorkerID] = time.Now()
	c.workersToTasks[request.WorkerID] = append(c.workersToTasks[request.WorkerID], task)

	// send the task in the response
	response.Task = task

	printTask(response.Task)

	return nil
}

// returns task, updated queue, and if it's a valid task
func getTask(mq *[]common.Task, rq *[]common.Task) (common.Task, *[]common.Task, *[]common.Task, bool) {
	// pop off the front of the queue
	mqueue := *mq
	rqueue := *rq

	// for m
	if len(mqueue) > 0 {
		t := mqueue[0]

		// reassign queue to equal everything after the first element
		// this acts like popping off the front
		mqueue = mqueue[1:]

		return t, &mqueue, &rqueue, true
	}

	if len(rqueue) > 0 {
		t := rqueue[0]

		// reassign queue to equal everything after the first element
		// this acts like popping off the front
		rqueue = rqueue[1:]

		return t, &mqueue, &rqueue, true
	}

	// if there's nothing in the queue, ret an empty task
	return common.Task{}, &mqueue, &rqueue, false
}

func Coordinator(M int, R int, file *os.File) {
	fmt.Println("COORDINATOR:")

	// establish all task information before initializing the RPC
	lines, _ := getNonEmptyLines(file)

	fmt.Printf("File lines = %d\n", len(lines))

	num_splits := len(lines) / M

	fmt.Printf("Number of splits = %d\n", num_splits)

	// make the queue and populate it
	mTaskQueue := make([]common.Task, M)
	rTaskQueue := make([]common.Task, R)

	for i := range mTaskQueue {
		t := common.Task{
			TaskId:   i,
			TaskType: "M",
			Done:     false,
			Filename: fmt.Sprintf("../splits/split_p%d", i),
			R:        R,
			M:        M,
		}

		//printTask(t)

		mTaskQueue[i] = t
	}

	for i := range rTaskQueue {
		t := common.Task{
			TaskId:   i,
			TaskType: "R",
			Done:     false,
			Filename: fmt.Sprintf("../output%d.json", i),
			R:        R,
			M:        M,
		}

		// printTask(t)

		rTaskQueue[i] = t
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
	coordinatorApi.inProgress = make(map[int]time.Time, 0)
	coordinatorApi.workersToTasks = make(map[int][]common.Task, 0)
	coordinatorApi.mu = new(sync.Mutex)
	coordinatorApi.R = R
	coordinatorApi.mTasks = &mTaskQueue
	coordinatorApi.rTasks = &rTaskQueue
	coordinatorApi.M = M

	// register it
	rpc.Register(coordinatorApi)

	// spawn a thread looking for new connections
	go listenForWorkers()

	for {
		coordinatorApi.mu.Lock()

		total := len(mTaskQueue) + len(rTaskQueue) + len(coordinatorApi.inProgress)

		if total == 0 {
			coordinatorApi.mu.Unlock()
			break
		}

		for wId, startTime := range coordinatorApi.inProgress {
			if time.Since(startTime) > (time.Second * 10) {
				requeueTasks(coordinatorApi, wId)
			}
		}

		coordinatorApi.mu.Unlock()

		// sleep so mu can be unlocked giving other's access
		time.Sleep(500 * time.Millisecond)
	}

	// queue must be empty, end
	done = true
}

func Done() bool {
	return done
}

func requeueTasks(c *CoordinatorAPI, wId int) {
	tasksToQueue := c.workersToTasks[wId]
	mTasks := *c.mTasks
	rTasks := *c.rTasks

	for _, task := range tasksToQueue {
		if task.TaskType == "M" {
			mTasks = append(mTasks, task)
		} else {
			rTasks = append(rTasks, task)
		}
	}
}

func printTask(t common.Task) {
	fmt.Printf("id: %d, type: %s, done: %t, fname: %s, R: %d, M: %d\n", t.TaskId, t.TaskType, t.Done, t.Filename, t.R, t.M)
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
