package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskRequest struct {
	WorkerId string
}
type BaseTask struct {
	RemainNum int
	TaskType  string
}

const (
	Init = iota
	DISPATCHED
	FINISH
)

type MapTask struct {
	TaskId   string
	FileName string
	Contents string
	Status   int
}

type MapTaskResult struct {
	MapTask
	KeyValues []KeyValue
}
type ReduceTask struct {
	TaskId       string
	KeyValuesMap map[string][]string
	Status       int
}

type ReduceTaskResult struct {
	ReduceTask
}

type Task struct {
	BaseTask
	MapTask
	ReduceTask
}

type CommonResult struct {
	Message string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
