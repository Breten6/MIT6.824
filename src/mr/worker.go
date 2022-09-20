package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}
func tempmapfilegen(workerid int, taskid int, key int) string {
	return fmt.Sprintf("tmp_map_%d_%d_%d",workerid, taskid, key)
}
func mapfilegen(taskid int, key int) string {
	return fmt.Sprintf("map_%d_%d", taskid, key)
}
func tempreducefilegen(workererId int, nreduce int) string {
	return fmt.Sprintf("tem_reduce_%d_%d",workererId, nreduce)
}
func fianlfilegen(nreduce int) string {
	return fmt.Sprintf("mr-out-%d", nreduce)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	workerId := os.Getpid()
	var curtaskId int
	var curtaskType string
	for {// for loop, keep requesting new tasks for worker
		args := AssignArgs{
			WorkerId:    workerId,
			CurtaskType: curtaskType,
			CurtaskId:   curtaskId,
		}
		reply := AssignReply{}
		call("Coordinator.Assigntasks", &args, &reply)
		
		if reply.TaskType == "MAP" {//handle map task
			log.Printf("worker starts mapping\n");
			//read a task map file
			file, _ := os.Open(reply.MapFile)
			content, _ := ioutil.ReadAll(file)
			file.Close()
			//mapf
			kva := mapf(reply.MapFile, string(content))
			hashtable := make(map[int][]KeyValue)

			for _, kv := range kva {//hash function, generate a hash table
				hashed := ihash(kv.Key) % reply.NReduce
				hashtable[hashed] = append(hashtable[hashed], kv)
			}

			for i := 0; i < reply.NReduce; i++ {//create temp map file by the hashtable
				outFile, _ := os.Create(tempmapfilegen(workerId, reply.TaskId, i))
				for _, kv := range hashtable[i] {
					fmt.Fprintf(outFile, "%v\t%v\n", kv.Key, kv.Value)
				}
				outFile.Close()
			}
		} else if reply.TaskType == "REDUCE" {//handle reduce task
			log.Printf("worker starts reducing\n");
			var reducetask []string
			for i := 0; i < reply.NMap; i++ {//read the map files, add the lines to a string[]
				file, _ := os.Open(mapfilegen(i, reply.TaskId))
				content, _ := ioutil.ReadAll(file)
				reducetask = append(reducetask, strings.Split(string(content),"\n")...)
			}
			var kva []KeyValue
			for _, line := range reducetask {//generate kva by lines
				if strings.TrimSpace(line) == "" {
					continue
				}
				split := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key:   split[0],
					Value: split[1],
				})
			}
			sort.Sort(ByKey(kva))

			outFile, _ := os.Create(tempreducefilegen(workerId, reply.TaskId))
			//add to temp reduce files
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)

				fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			time.Sleep(time.Second*10)
			outFile.Close()
		} else {
			break
		}
		curtaskId = reply.TaskId
		curtaskType = reply.TaskType
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func CallExample() {

// 	// declare an argument structure.
// 	args := ExampleArgs{}

// 	// fill in the argument(s).
// 	args.X = 99

// 	// declare a reply structure.
// 	reply := ExampleReply{}

// 	// send the RPC request, wait for the reply.
// 	call("Coordinator.Example", &args, &reply)

// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Printf("%v",err)
	return false
}
