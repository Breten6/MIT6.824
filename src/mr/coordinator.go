package mr

import (
	"log"
	"math"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)
//define task struct, using "Timeout" for helping recycling
type Task struct{
	Type string;// MAP or REDUCE
	Id int;
	WorkerId int;// which worker is doing this task
	Timeout time.Time;
	MapFile string;//task splits
}
// 
type Coordinator struct {
	// Your definitions here.
	stage string; // current stage, MAP or REDUCE or DONE
	nMap int; // count of input files
	nReduce int; // count of how many reduce tasks
	tasks map[int]Task // store tasks
	readytasks chan Task // by using channel, no need to delete tasks manually
	lock sync.Mutex // mutex lock
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
// func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
// 	reply.Y = args.X + 1
// 	return nil
// }
func (c* Coordinator) Assigntasks(args* AssignArgs,reply* AssignReply) error{
	//check if there is finished tmp files
	if args.CurtaskType != ""{
		log.Printf("%v %v %v %v", args.WorkerId,args.CurtaskId,args.CurtaskType, c.tasks[args.CurtaskId].WorkerId)
		c.lock.Lock()
		// check if worker ID match, make sure time out tasks are ignored 
		if task, exists := c.tasks[args.CurtaskId]; exists && task.WorkerId == args.WorkerId{
			//deal with finished map tasks
			if args.CurtaskType == "MAP"{
				for i:=0;i<c.nReduce;i++{
					os.Rename(tempmapfilegen(args.WorkerId,args.CurtaskId,i),mapfilegen(args.CurtaskId,i))
					}
			//deal with finished reduce tasks				
			}else if args.CurtaskType == "REDUCE"{
				os.Rename(tempreducefilegen(args.WorkerId,args.CurtaskId),fianlfilegen(args.CurtaskId))
			}else{
				c.lock.Unlock()
				return nil
			}
		}
		log.Printf("%v %v %v %v", args.WorkerId,args.CurtaskId,args.CurtaskType, c.tasks[args.CurtaskId].WorkerId)
		//delete finished task
		delete(c.tasks,args.CurtaskId);
		// check if the stage end
		if len(c.tasks)==0{
			c.nextprocess()
		}
		c.lock.Unlock()
	}
	c.lock.Lock()
	//get a new task from channel
	newtask,ok:= <- c.readytasks
	if !ok {
        return nil
    }
	newtask.WorkerId = args.WorkerId
	newtask.Timeout = time.Now().Add(10* time.Second)
	//add worker id and Timeout to assigned task
	c.tasks[newtask.Id] = newtask
	//reply the task detail to worker
	reply.MapFile = newtask.MapFile
	reply.NMap = c.nMap
	reply.NReduce = c.nReduce
	reply.TaskType = newtask.Type
	reply.TaskId = newtask.Id
	c.lock.Unlock()
	//worker starts process the task
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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
func (c* Coordinator) nextprocess(){
	
	//MAP to REDUCE stage
	if c.stage == "MAP" {
		log.Printf("Mapping finished");
		c.stage = "REDUCE"
		for i := 0; i < c.nReduce; i++ {

			task := Task{Id: i, Type: "REDUCE", WorkerId: -1}
			c.tasks[i] = task
			c.readytasks <- task
		}
	//REDUCE finished, go to DONE stage, wait mrcoodinator.go to end
	} else if c.stage == "REDUCE" {
		log.Printf("Reducing finished")
		close(c.readytasks)
		c.stage = "DONE"
	}

}
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//lock it to avoid c.stage data race
	c.lock.Lock()
	ret := c.stage == "DONE"

	// Your code here.
	c.lock.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	log.Printf("lets start!")
	//generate a coordinator
	c := Coordinator{
		nMap: len(files),
		nReduce: nReduce,
		stage: "MAP",
		tasks: make(map[int]Task),
		readytasks: make(chan Task,int(math.Max(float64(len(files)), float64(nReduce)))),
	}

	// Your code here.
	// add input tasks
	for i,file :=range files{
		task:= Task{
			Id : i,
			MapFile: file,
			Type: "MAP",
			WorkerId: -1,
		}
		c.tasks[i]=task;
		//add tasks to channel
		c.readytasks <- task
	}

	c.server()
	//loop for checking timeout
	go func ()  {
		for{
			time.Sleep(500*time.Millisecond)
			
			c.lock.Lock() //lock to avoid c.tasks data race
			for _,task:= range c.tasks{
				if time.Now().After(task.Timeout) && task.WorkerId!=-1{
					task.WorkerId = -1;
					c.readytasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	return &c
}
