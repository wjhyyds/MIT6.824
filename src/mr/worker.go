package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

type (
	// 第一个参数是文件名，第二个参数是文件内容，文件名应当被忽略
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type SortedKey []KeyValue

func (k SortedKey) Len() int           { return len(k) }
func (k SortedKey) Less(i, j int) bool { return k[i].Key < k[j].Key }
func (k SortedKey) Swap(i, j int)      { k[i], k[j] = k[j], k[i] }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func GetTask() Task {
	args := TaskArgs{}
	ret := Task{}
	ok := call("Coordinator.AssignTask", &args, &ret)
	if !ok {
		log.Fatalln("[ERROR] call failed")
	}
	// log.Printf("[INFO] get task %v", ret)
	return ret
}

// main/mrworker.go calls this function.
func Worker(mf mapf, rf reducef) {
	// Your worker implementation here.
	var task Task
loop:
	for {
		task = GetTask()
		// fmt.Println(task)
		switch task.State {
		case Waiting:
			{
				log.Println("[INFO] no idle task,waiting")
				time.Sleep(time.Second)
			}
		case Mapping:
			{
				log.Printf("[INFO] begin map task %v", task.Id)
				Map(mf, &task)
				// log.Printf("[INFO] finish map task %v", task.Id)

				callDone(task.Id)
			}
		case Reducing:
			{
				log.Printf("[INFO] begin reduce task %v", task.Id)
				Reduce(rf, &task)
				// log.Printf("[INFO] finish reduce task %v", task.Id)

				callDone(task.Id)
			}
		case Exit:
			{
				log.Println("[INFO] exit")
				break loop
			}
		default:
			{
				log.Fatalln("[ERROR] wrong state,exit")
				break loop
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func Map(mf mapf, task *Task) {
	file, err := os.Open(task.Files[0])
	if err != nil {
		log.Fatalf("[ERROR] cannot open %v during map", task.Files[0])
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("[ERROR] cannot read %v during map", task.Files[0])
	}
	// Files[0] 应该被忽略
	intermediate := mf(task.Files[0], string(content))

	n := task.ReduceNum
	dir, _ := os.Getwd()
	hsh := make([][]KeyValue, n)
	for _, kv := range intermediate {
		hsh[ihash(kv.Key)%n] = append(hsh[ihash(kv.Key)%n], kv)
	}
	for i := 0; i < n; i++ {
		filename := fmt.Sprintf("mr-%v-%v", task.Id, i)
		tfile, _ := os.CreateTemp(dir, "map-tmp-*")
		defer tfile.Close()

		enc := json.NewEncoder(tfile)
		for _, kv := range hsh[i] {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("[ERROR] encode fail in %v during map", tfile)
			}
		}
		os.Rename(tfile.Name(), filename)
	}
}

func Reduce(rf reducef, task *Task) {
	intermediate := shuffle(task.Files)
	dir, _ := os.Getwd()
	tfile, _ := os.CreateTemp(dir, "reduce-tmp-*")
	defer tfile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i] = intermediate[k].Value
		}
		output := rf(intermediate[i].Key, values)
		fmt.Fprintf(tfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	os.Rename(tfile.Name(), fmt.Sprintf("mr-out-%d", task.Id))
}

func shuffle(files []string) []KeyValue {
	kva := []KeyValue{}
	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			log.Fatalf("[ERROR] cannot open %v during shuffle", file)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		defer f.Close()
	}
	sort.Sort(SortedKey(kva))
	return kva
}

// 告知第i个任务结束
func callDone(id int) {
	if ok := call("Coordinator.SetTaskDone", id, &Task{}); !ok {
		log.Fatalln("[ERROR] call failed")
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {
//
// 	// declare an argument structure.
// 	args := ExampleArgs{}
//
// 	// fill in the argument(s).
// 	args.X = 99
//
// 	// declare a reply structure.
// 	reply := ExampleReply{}
//
// 	// send the RPC request, wait for the reply.
// 	// the "Coordinator.Example" tells the
// 	// receiving server that we'd like to call
// 	// the Example() method of struct Coordinator.
// 	ok := call("Coordinator.Example", &args, &reply)
// 	if ok {
// 		// reply.Y should be 100.
// 		fmt.Printf("reply.Y %v\n", reply.Y)
// 	} else {
// 		fmt.Printf("call failed!\n")
// 	}
// }

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
