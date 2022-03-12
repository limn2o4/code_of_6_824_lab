package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerReq struct {
	WorkerId string
}

type WorkerResp struct {
	JobId     string   `json:"jobId"`
	JobType   string   `json:"jobType"`
	Partition int      `json:"partition"`
	NumReduce int      `json:"numReduce"`
	Input     []string `json:"input"`
	Output    []string `json:"output"`
}

type WorkerReportReq struct {
	WorkerId  string
	JobId     string
	JobStatus int
}

type WorkerReportResp struct {
	WorkerId string
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
