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

type AssignWorkerIdArgs struct {
}

type AssignWorkerIdReply struct {
	WorkerId int
	NReduce  int
}

type AssignTaskArgs struct {
	WorkerId int
}

type AssignTaskReply struct {
	Task *TaskStruct
}

type CompleteMapArgs struct {
	TaskId    int
	InterLocs []string
}

type CompleteMapReply struct {
}

type CompleteReduceArgs struct {
	TaskId int
}

type CompleteReduceReply struct {
}

type KeepAliveArgs struct {
	WorkerId int
}

type KeepAliveReply struct {
}

type ExitWorkerArgs struct {
	WorkerId int
}

type ExitWorkerReply struct {
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