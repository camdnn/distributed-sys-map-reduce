package common

// all information about a specific task
type Task struct {
	TaskId     int
	TaskType   string // M or R
	InProgress bool
	Filename   string
	R          int
	M          int
}

// from worker
type Request struct {
	WorkerID int
}

// from controller
type Response struct {
	Task Task
}
