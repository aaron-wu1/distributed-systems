package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MapTask struct {
	Id       int
	NReduce  int
	Filename string
}

type ReduceTask struct {
	Id                int
	IntermediateFiles []string
}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type FetchFileArgs struct {
}
type FetchFileReply struct {
	TaskType   int
	MapTask    MapTask
	ReduceTask ReduceTask
}

type AddIntermediateFilesArgs struct {
	MapId             int
	Filename          string
	IntermediateFiles []string
	TaskType          int
}

type AddIntermediateFilesReply struct {
}

type SetReduceTaskDoneArgs struct {
	ReduceId int
}

type SetReduceTaskDoneReply struct {
}

type SetMapTaskDoneArgs struct {
	MapId    int
	Filename string
}

type SetMapTaskDoneReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
