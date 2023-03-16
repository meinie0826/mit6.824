package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 请求
type ExampleArgs struct {
	//Request/Success
	Types int
	Filename string
}
// 回复
type ExampleReply struct {
	//0 Map 1 Reduce 2 Wait 3 Return
	Types int 
	//filename
	Filename string
	//nReduce
	NReduce int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func CoordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
