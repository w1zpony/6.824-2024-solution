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

type TaskResult int

const (
	Success TaskResult = iota
	Failure
)

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	Wait
	AllDone
)

type MessageArgs struct {
	TaskID     int
	TaskStatus TaskResult //Worker回复的任务状态
}

type MessageReply struct {
	TaskID    int
	TaskPhase Phase
	TaskFile  string
	NReduce   int
	NMap      int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
