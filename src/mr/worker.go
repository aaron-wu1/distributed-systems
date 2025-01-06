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
)

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
	CallExample()
	reply := CallFetchFile()
	fmt.Println(reply)
	if reply.TaskType == MAP {
		GenerateIntermediateFiles(reply.Id, reply.Filename, reply.NReduce)
	}
	if reply.TaskType == REDUCE {
		// reducef()
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
	fmt.Println(ok)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("file name %v\n", reply.Filename)
	} else {
		fmt.Printf("call failed!\n")
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
	intermediates := [][]KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// divide into buckets
	partSize := len(content) / nReduce
	for i := 0; i < nReduce; i++ {
		start := i * partSize
		end := start + partSize
		if i == nReduce-1 {
			end = len(content)
		}
		part := content[start:end]
		kva := mapf(fmt.Sprintf("mr-%d-%d", id, i), string(part))
		file, err := os.Create(filepath.Base(fmt.Sprintf("mr-%d-%d", id, i)))
		if err != nil {
			log.Fatalf("cannot create %v", fmt.Sprintf("mr-%d-%d", id, i))
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range kva {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		AddReduceFile(id, fmt.Sprintf("mr-%d-%d", id, i))
		intermediates = append(intermediates, kva)
	}

	fmt.Println(nReduce, len(intermediates))
	// fmt.Println(intermediates)
}

// Tell coordinator that the reduce file is ready
func AddReduceFile(reduceId int, reduceFilename string) {

	// declare an argument structure.
	args := AddFileArgs{}

	// fill in the argument(s).
	args.Id = reduceId
	args.Filename = reduceFilename
	args.TaskType = REDUCE

	// declare a reply structure.
	reply := AddFileReply{}
	fmt.Println("calling AddReduceFile")

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AddFile", &args, &reply)
	if ok {
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
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
