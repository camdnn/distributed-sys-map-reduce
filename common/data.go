package common

// all information about a specific task
type Task struct {
	taskId     int
	taskType   string // M or R
	inProgress bool
	filename   string
}

// from worker
type Request struct {
	WorkerID int
}

// from controller
type Response struct {
	Task Task
}
