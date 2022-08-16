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
type WorkerRequest struct{
	WorkerId string
	WorkDone int
}

type JobCondition int

const(
	JobWorking = 0
	JobWaiting = 1
	JobDone = 2
)

type JobType int

const(
	MapJob=0
	ReduceJob=1
	WaittingJob=2
	KillJob=3
)

type Condition int

const(
	MapPhase=0
	ReducePhase=1
	AllDone=2
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
