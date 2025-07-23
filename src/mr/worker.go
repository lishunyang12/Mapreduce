package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
//  Map functions return a slice of KeyValue
//
type KeyValue struct {
	Key string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// 
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// start worker
	for {
		task := getTask()
		// map task to mapper, reduce task to reducer
		// wait for 5s in wait, or exit
		switch task.TaskState {
		case Map:
			mapper(&task, mapf)
		case Reduce:
			reducer(&task, reducef)
		case Wait:
			time.Sleep(5 * time.Second)
		case Exit:
			return	
		}
	}
}

func reducer(task *Task, reducef func(string, []string) string) {
	// read intermediate in local file
	intermediate := *readFromLocalFile(task.Intermediates)
	// sort on key 
	sort.Sort(ByKey(intermediate))
	
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-temp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskNumber)
	os.Rename(tempFile.Name(), oname)
	TaskCompleted(task)
}

func mapper(task *Task, mapf func(string, string) []KeyValue) {
	// read content from filename	
	content, err := ioutil.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}
	// send content to mapf and get intermediate results
	kva := mapf(task.Input, string(content))

	// write results to local disk divided into R parts
	mapOutput := []string{}
	tmpFiles := []*os.File{}
	encoders := []*json.Encoder{}
	dir, _ := os.Getwd()
	for i := 0; i < task.NReducer; i++ {
		tmpFile, err := ioutil.TempFile(dir, "mr-tmp-*")
		if err != nil { 
			log.Fatalf("cannot open tmpfile")
		}		
		tmpFiles = append(tmpFiles, tmpFile)
		enc := json.NewEncoder(tmpFile)
		encoders = append(encoders, enc)
	}
	// send locations of R files to master
	for _, kv := range kva {
		r := ihash(kv.Key) % task.NReducer
		encoders[r].Encode(&kv)
	}
	for _, f := range tmpFiles {
		f.Close()
	}
	// atomically rename temp files to final intermediate files
	for r := 0; r < task.NReducer; r++ {
		outputName := fmt.Sprintf("mr-%d-%d", task.TaskNumber, r)
		os.Rename(tmpFiles[r].Name(), outputName)
		mapOutput = append(mapOutput,filepath.Join(dir, outputName))	      }
	task.Intermediates = mapOutput
	TaskCompleted(task)
}	

func getTask() Task {
	// get task from master
	args := ExampleArgs{}
	reply := Task{}
	call("Master.AssignTask", &args, &reply)
	return reply
}

func TaskCompleted(task *Task) {
	// send task to master
	reply := ExampleReply{}
	call("Master.TaskCompleted", task, &reply)
}

func readFromLocalFile(files []string) *[]KeyValue{
	kva := []KeyValue{}
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("failed to open file "+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := dec.Decode(&kv)
			if err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}	
	return &kva
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		os.Exit(0)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
