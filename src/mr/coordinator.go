package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Job struct {
	jobId     string
	jobType   string
	partition int
	numReduce int
	input     []string
	output    string
	Status    int //init 0,run 1,success 2,fail 3
}

const (
	INIT    int = 0
	RUNNING     = 1
	SUCCESS     = 2
	FAIL        = 3
)

type jobSyncQueue struct {
	lock  sync.Mutex
	queue []*Job
}
type JobStatus struct {
	lock   sync.Mutex
	Status map[string]int //jobId:int
	Cnt    map[string]int //jobId:failed cnt
}

type Coordinator struct {
	// Your definitions here.
	workers   map[string]int
	jobInfo   map[string]*Job
	jobStatus JobStatus
	jobQueue  jobSyncQueue
}

func (q *jobSyncQueue) push(job *Job) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.queue = append(q.queue, job)
}
func (q *jobSyncQueue) pushFront(job *Job) {
	q.lock.Lock()
	defer q.lock.Unlock()
	q.queue = append(q.queue, &Job{})
	copy(q.queue[1:], q.queue)
	q.queue[0] = job
}
func (q *jobSyncQueue) pop() *Job {
	q.lock.Lock()
	defer q.lock.Unlock()
	if q.queue == nil || len(q.queue) == 0 {
		return nil
	} else {
		top := q.queue[0]
		q.queue = q.queue[1:]
		return top
	}
}
func (jobStatus *JobStatus) getStatus(jobId string) int {
	jobStatus.lock.Lock()
	defer jobStatus.lock.Unlock()
	return jobStatus.Status[jobId]
}
func (jobStatus *JobStatus) setStatus(jobId string, status int) int {
	jobStatus.lock.Lock()
	defer jobStatus.lock.Unlock()
	jobStatus.Status[jobId] = status
	return jobStatus.Status[jobId]
}
func (jobStatus *JobStatus) getCnt(jobId string) int {
	jobStatus.lock.Lock()
	defer jobStatus.lock.Unlock()
	return jobStatus.Cnt[jobId]
}
func (jobStatus *JobStatus) resetCnt(jobId string) int {
	jobStatus.lock.Lock()
	defer jobStatus.lock.Unlock()
	jobStatus.Cnt[jobId] = 0
	return jobStatus.Cnt[jobId]
}
func (jobStatus *JobStatus) increaseCnt(jobId string) int {
	jobStatus.lock.Lock()
	defer jobStatus.lock.Unlock()
	jobStatus.Cnt[jobId] += 1
	return jobStatus.Cnt[jobId]
}

// Your code here -- RPC handlers for the worker to call.
//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
func (c *Coordinator) GetJob(req *WorkerReq, resp *WorkerResp) error {

	fmt.Println("call GetJob")
	job := c.jobQueue.pop()
	if job == nil {
		return fmt.Errorf("no job avaliable")
	}
	resp.JobId = job.jobId
	resp.JobType = job.jobType
	resp.Partition = job.partition
	resp.NumReduce = job.numReduce
	resp.Input = job.input
	resp.Output = job.output
	c.jobStatus.setStatus(job.jobId, RUNNING)
	fmt.Printf("assigned job %s to worker %s\n", job.jobId, req.WorkerId)

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
	fmt.Println("http init done")
	go http.Serve(l, nil)
}

func (c *Coordinator) makeJobs(files []string, nReduce int) int {
	input_list := make([]string, 0)
	c.jobStatus.Status = make(map[string]int)
	c.jobStatus.Cnt = make(map[string]int)
	c.jobInfo = make(map[string]*Job)

	for idx, file := range files {
		jobId := fmt.Sprintf("map_job_%d", idx)
		input_file := file
		output_file := fmt.Sprintf("mr-map-res-%s.txt", jobId)
		input_list = append(input_list, output_file)
		jobInst := Job{jobId: jobId, jobType: "map", input: []string{input_file}, output: output_file}
		c.jobQueue.push(&jobInst)
		c.jobInfo[jobId] = &jobInst
		c.jobStatus.Status[jobId] = INIT
		c.jobStatus.Cnt[jobId] = 0
	}
	for i := 0; i < nReduce; i += 1 {
		jobId := fmt.Sprintf("reduce_job_%d", i)
		output_file := fmt.Sprintf("mr-out-%d.txt", i)
		jobInst := Job{jobId: jobId, jobType: "reduce", input: input_list, output: output_file, partition: i, numReduce: nReduce}
		c.jobQueue.push(&jobInst)
		c.jobInfo[jobId] = &jobInst
		c.jobStatus.Status[jobId] = INIT
		c.jobStatus.Cnt[jobId] = 0
	}

	return len(c.jobQueue.queue)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) ReTryJob(jobInst *Job) {

	c.jobQueue.push(jobInst)
	c.jobStatus.lock.Lock()
	defer c.jobStatus.lock.Unlock()
	c.jobInfo[jobInst.jobId] = jobInst
	c.jobStatus.Status[jobInst.jobId] = INIT
	c.jobStatus.Cnt[jobInst.jobId] = 0
}

func (c *Coordinator) Done() bool {
	ret := true

	// Your code here.
	complete_job := 0
	for jobId, item := range c.jobInfo {

		_, err := os.Stat(item.output)
		is_job_finish := false
		if err == nil {
			if !os.IsExist(err) {
				is_job_finish = true
			} else {
				is_job_finish = false
			}
		}
		// fmt.Printf("jobid %s status : %d is_finish %v \n", item.jobId, c.jobStatus.Status[item.jobId], is_job_finish)
		curStatus := c.jobStatus.getStatus(jobId)
		if is_job_finish {
			complete_job += 1
			c.jobStatus.lock.Lock()
			c.jobStatus.Status[jobId] = SUCCESS
			c.jobStatus.lock.Unlock()
		} else if curStatus == RUNNING {
			c.jobStatus.lock.Lock()
			if c.jobStatus.Cnt[jobId] < 10 {
				c.jobStatus.Cnt[jobId] += 1
				c.jobStatus.lock.Unlock()
			} else {
				c.jobStatus.Status[item.jobId] = INIT
				c.jobStatus.Cnt[item.jobId] = 0
				c.jobStatus.lock.Unlock()
				if item.jobType == "map" {
					c.jobQueue.pushFront(item)
				} else {
					c.jobQueue.push(item)
				}

				log.Printf("Job failed after waiting for 10 s %s", jobId)
			}
		}

	}
	ret = complete_job == len(c.jobInfo)
	log.Printf("Job status: %d/%d", complete_job, len(c.jobInfo))
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	job_size := c.makeJobs(files, nReduce)
	// Your code here.
	fmt.Printf("master init complete job size = %d\n", job_size)

	c.server()
	return &c
}
