package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type AssignArgs struct {
	WorkerId int;//indicates whose calling the func
	CurtaskType string;//indicates the last task type of caller(or worker)
	CurtaskId int;//indicates the last task id of caller(or worker)
}
//MUST UPPERCASE THE FIRST LETTER!!!!!!!!
//reply new task info
type AssignReply struct {
	TaskType string;
	TaskId int;
	NMap int;
	NReduce int;
	MapFile string;
	Timeout time.Time;
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	log.Printf("%v",s)
	return s
}
