package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"plugin"
	"sort"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply := CallFetchFile()
		if reply.TaskType == MAP {
			mapTask := reply.MapTask
			GenerateIntermediateFiles(mapTask.Id, mapTask.Filename, mapTask.NReduce)
		} else if reply.TaskType == REDUCE {
			reduceTask := reply.ReduceTask
			reduceIntermediateFile(reduceTask.Id, reduceTask.IntermediateFiles)
		} else if reply.TaskType == NONE {
			// WAITITITI
			time.Sleep(1 * time.Second)
		}
	}
}

func CallFetchFile() FetchFileReply {

	// // declare an argument structure.
	args := FetchFileArgs{}

	// // declare a reply structure.
	reply := FetchFileReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	ok := call("Coordinator.FetchFile", &args, &reply)
	if ok {
		// reply.Y should be 100.
	} else {
		fmt.Printf("call fetch file failed!\n")
	}
	return reply
}

func GenerateIntermediateFiles(id int, filename string, nReduce int) {
	mapf, _ := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	intermediateFiles := make([]string, nReduce)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	// partition keys into buckets
	partitionedKva := make([][]KeyValue, nReduce)
	for _, v := range kva {
		partitionKey := ihash(v.Key) % nReduce
		partitionedKva[partitionKey] = append(partitionedKva[partitionKey], v)
	}
	// print(partitionedKva)
	for i := 0; i < nReduce; i++ {
		newInterfile, err := os.Create(filepath.Base(fmt.Sprintf("mr-%d-%d", id, i)))
		if err != nil {
			log.Fatalf("cannot create %v", fmt.Sprintf("mr-%d-%d", id, i))
		}
		defer newInterfile.Close()
		marshalledKva, err := json.Marshal(partitionedKva[i])
		if err != nil {
			log.Fatalf("cannot encode %v", partitionedKva[i])
		}
		newInterfile.Write(marshalledKva)
		intermediateFiles[i] = fmt.Sprintf("mr-%d-%d", id, i)
	}
	CallIntermediateFilesReady(id, filename, intermediateFiles)
}

// Tell coordinator that the map task is done
func CallMapDone(mapId int, filename string) {
	// declare an argument structure.
	args := SetMapTaskDoneArgs{}

	args.MapId = mapId
	args.Filename = filename

	reply := SetMapTaskDoneReply{}

	ok := call("Coordinator.SetMapTaskDone", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call map done failed!\n")
	}
}

// Tell coordinator that the reduce file is ready
func CallIntermediateFilesReady(mapid int, filename string, intermediateFiles []string) {

	// declare an argument structure.
	args := AddIntermediateFilesArgs{}

	// fill in the argument(s).
	// args.ReduceId = reduceId
	args.MapId = mapid
	args.Filename = filename
	args.IntermediateFiles = intermediateFiles
	args.TaskType = REDUCE

	// declare a reply structure.
	reply := AddIntermediateFilesReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AddIntermediateFiles", &args, &reply)
	if ok {
	} else {
		fmt.Printf("add intermediate files call failed!\n")
	}
}

func reduceIntermediateFile(reduceId int, intermediateFiles []string) {
	_, reducef := loadPlugin(os.Args[1])
	kva := []KeyValue{}
	for _, filename := range intermediateFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		defer file.Close()
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		intermediate := []KeyValue{}
		json.Unmarshal(content, &intermediate)
		kva = append(kva, intermediate...)
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", reduceId)
	tempFile, err := os.CreateTemp(".", oname)
	if err != nil {
		fmt.Println("Error creating temp file")
	}

	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)

		i = j
	}
	os.Rename(tempFile.Name(), oname)
	CallReduceDone(reduceId)
}

func CallReduceDone(reduceId int) {

	// declare an argument structure.
	args := SetReduceTaskDoneArgs{}

	// fill in the argument(s).
	args.ReduceId = reduceId

	// declare a reply structure.
	reply := SetReduceTaskDoneReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.SetReduceTaskDone", &args, &reply)
	if ok {
	} else {
		fmt.Printf("call reduce done failed!\n")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}
	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []KeyValue)
	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
