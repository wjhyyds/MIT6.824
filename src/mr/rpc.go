package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskState int

const (
	Waiting TaskState = iota
	Mapping
	Reducing
	Done
	Exit
)

type TaskArgs struct{}

// Add your RPC definitions here.
type Task struct {
	Id        int
	Files     []string
	ReduceNum int
	State     TaskState
	Begin     time.Time
}

type Phase int 
const (
	MapPhase Phase = iota
	ReducePhase 
	AllDone
)

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
