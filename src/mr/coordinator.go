package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	nReduce int
	ch      chan File
}

const (
	MAP    = iota
	REDUCE = iota
)

type File struct {
	id       int
	filename string
	taskType int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Assign work
func (c *Coordinator) FetchFile(args *FetchFileArgs, reply *FetchFileReply) error {
	file := <-c.ch
	reply.Filename = file.filename
	reply.Id = file.id
	reply.NReduce = c.nReduce
	reply.TaskType = file.taskType
	return nil
}

func (c *Coordinator) AddFile(args *AddFileArgs, reply *AddFileReply) error {
	fmt.Println("AddFile to channel", args.Id, args.Filename, args.TaskType)
	c.ch <- File{id: args.Id, filename: args.Filename, taskType: args.TaskType}
	reply.Y = 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,
		ch:      make(chan File, 100),
	}
	for i, file := range files {
		go func(i int, file string) {
			c.ch <- File{id: i, filename: file, taskType: MAP}
		}(i, file)
	}

	// Your code here.
	fmt.Println(len(files), nReduce)

	c.server()
	return &c
}
