package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	nReduce           int
	mapCh             chan File
	mapTaskStatus     map[string]int
	reduceTaskStatus  map[int]int
	intermediateFiles map[int][]string
	mu                sync.Mutex
}

const (
	NONE   = iota
	MAP    = iota
	REDUCE = iota
)

const (
	NOT_STARTED = iota
	PROCESSING  = iota
	DONE        = iota
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.checkMapTaskStatus() {
		// All map tasks done before reduce taasks
		reduceTask := ReduceTask{}
		if !c.findReduceTask(&reduceTask) {
			reply.TaskType = NONE
		} else {
			c.reduceTaskStatus[reduceTask.Id] = PROCESSING
			reply.ReduceTask = reduceTask
			reply.TaskType = REDUCE
			go c.checkReduceWorkerHealth(reduceTask.Id)
			return nil
		}
		return nil
	}
	if c.checkReduceTaskStatus() {
		// All reduce task done, prevents channel from blocking
		reply.TaskType = NONE
		return nil
	}
	if c.checkEmptyMapTask() {
		// All map tasks assigned
		reply.TaskType = NONE
		return nil
	}
	file := <-c.mapCh
	c.mapTaskStatus[file.filename] = PROCESSING
	mapTask := MapTask{}
	mapTask.Filename = file.filename
	mapTask.Id = file.id
	mapTask.NReduce = c.nReduce
	reply.MapTask = mapTask
	reply.TaskType = file.taskType
	go c.checkMapWorkerHealth(file.id, file.filename)
	return nil
}

func (c *Coordinator) checkReduceWorkerHealth(taskid int) {
	timer := time.NewTimer(10 * time.Second)
	<-timer.C
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.reduceTaskStatus[taskid] == PROCESSING {
		c.reduceTaskStatus[taskid] = NOT_STARTED
	}
}

func (c *Coordinator) checkMapWorkerHealth(fileid int, filename string) {
	timer := time.NewTimer(10 * time.Second)
	<-timer.C
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mapTaskStatus[filename] == PROCESSING {
		c.mapCh <- File{id: fileid, filename: filename, taskType: MAP}
		c.mapTaskStatus[filename] = NOT_STARTED
	}
}

func (c *Coordinator) checkEmptyMapTask() bool {
	for _, status := range c.mapTaskStatus {
		if status == NOT_STARTED {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkMapTaskStatus() bool {
	for _, status := range c.mapTaskStatus {
		if status != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) checkReduceTaskStatus() bool {
	for _, status := range c.reduceTaskStatus {
		if status != DONE {
			return false
		}
	}
	return true
}

func (c *Coordinator) AddIntermediateFiles(args *AddIntermediateFilesArgs, reply *AddIntermediateFilesReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for i, file := range args.IntermediateFiles {
		c.intermediateFiles[i] = append(c.intermediateFiles[i], file)
	}
	for i := range args.IntermediateFiles {
		c.reduceTaskStatus[i] = NOT_STARTED
	}
	c.mapTaskStatus[args.Filename] = DONE
	return nil
}

func (c *Coordinator) SetMapTaskDone(args *SetMapTaskDoneArgs, reply *SetMapTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mapTaskStatus[args.Filename] = DONE
	for i := 0; i < c.nReduce; i++ {
		c.reduceTaskStatus[i] = NOT_STARTED
	}
	return nil
}

func (c *Coordinator) findReduceTask(reduceTask *ReduceTask) bool {
	for key, val := range c.reduceTaskStatus {
		if val == NOT_STARTED {
			reduceTask.Id = key
			reduceTask.IntermediateFiles = c.intermediateFiles[key]
			return true
		}
	}
	return false
}

func (c *Coordinator) SetReduceTaskDone(args *SetReduceTaskDoneArgs, reply *SetReduceTaskDoneReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reduceTaskStatus[args.ReduceId] = DONE
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
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.checkReduceTaskStatus() {
		return false
	}

	if !c.checkMapTaskStatus() {
		return false
	}
	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce:           nReduce,
		mapCh:             make(chan File, 100),
		mapTaskStatus:     make(map[string]int),
		reduceTaskStatus:  make(map[int]int),
		intermediateFiles: make(map[int][]string),
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < nReduce; i++ {
		c.reduceTaskStatus[i] = NOT_STARTED
	}
	for i, file := range files {
		// go func(i int, file string) {
		c.mapTaskStatus[file] = 0
		c.mapCh <- File{id: i, filename: file, taskType: MAP}
		// }(i, file)
	}
	c.server()
	return &c
}
