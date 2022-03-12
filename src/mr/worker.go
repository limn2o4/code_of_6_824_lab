package mr

import (
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/big"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var WorkerId = 0

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func doMapJob(mapf func(string, string) []KeyValue, resp *WorkerResp) error {

	intermediate := make([]KeyValue, 0)
	for _, fileName := range resp.Input {

		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", fileName)
			return err
		}
		file.Close()
		kva := mapf(fileName, string(content))
		intermediate = append(intermediate, kva...)
	}
	outputArr := make([][]KeyValue, resp.NumReduce)

	for _, kv := range intermediate {
		partIndex := ihash(kv.Key) % (resp.NumReduce)
		outputArr[partIndex] = append(outputArr[partIndex], kv)
	}
	for output_idx, partArr := range outputArr {
		outputFile, err := os.Create(resp.Output[output_idx])
		if err != nil {
			log.Fatalf("cannot open %v", resp.Output)
			return err
		}
		enc := json.NewEncoder(outputFile)
		for _, kv := range partArr {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatalf("encode error %v", kv)
			}
		}
		outputFile.Close()
	}
	return nil
}

func doReduceJob(reducef func(string, []string) string, resp *WorkerResp) error {
	kva := make([]KeyValue, 0)
	for _, filename := range resp.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if ihash(kv.Key)%(resp.NumReduce) == resp.Partition {
				kva = append(kva, kv)
			}
		}
	}
	fmt.Printf("reduce partition %d get %d keys\n", resp.Partition, len(kva))
	// combine
	sort.Sort(ByKey(kva))
	rr, _ := crand.Int(crand.Reader, big.NewInt(1000))
	oname := fmt.Sprintf("mr-tmp-%s-%d.txt", resp.JobType, rr)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
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
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		i = j
	}

	ofile.Close()

	realname := resp.Output[0]
	os.Rename(oname, realname)

	return nil

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	WorkerId = int(time.Now().UnixNano())
	// uncomment to send the Example RPC to the coordinator.
	for {
		time.Sleep(time.Second)
		job := getJob()
		var jobStatus = SUCCESS
		if job.JobType == "map" {
			err := doMapJob(mapf, job)
			if err != nil {
				jobStatus = FAIL
			}
		} else if job.JobType == "reduce" {
			err := doReduceJob(reducef, job)
			if err != nil {
				jobStatus = FAIL
			}
		} else if job.JobType == "empty" {
			continue
		} else {
			fmt.Printf("worker %d exit \n", WorkerId)
			os.Exit(0)
		}

		reportDone(job.JobId, jobStatus)

	}

}
func getJob() *WorkerResp {
	args := WorkerReq{WorkerId: fmt.Sprintf("%d", WorkerId)}

	resp := WorkerResp{}

	call("Coordinator.GetJob", &args, &resp)

	fmt.Printf("worker get %+v\n", resp)

	return &resp
}

func reportDone(jobId string, jobStatus int) *WorkerReportResp {
	args := WorkerReportReq{WorkerId: fmt.Sprintf("%d", WorkerId), JobId: jobId, JobStatus: jobStatus}

	resp := WorkerReportResp{}

	call("Coordinator.reportJob", &args, &resp)

	return &resp
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

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

	fmt.Println(err)
	return false
}
