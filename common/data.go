package common

// all information about a specific task
type Task struct {
	TaskID     int
	TaskType   string // M or R
	InProgress bool
	Filename   string
	R          int //reduce
}

// from worker
type Request struct {
	WorkerID int
}

// from controller
type Response struct {
	Task Task
}
