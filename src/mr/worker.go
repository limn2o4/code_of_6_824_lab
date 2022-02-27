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
		}
		file.Close()
		kva := mapf(fileName, string(content))
		intermediate = append(intermediate, kva...)
	}
	outputFile, err := os.Create(resp.Output)
	if err != nil {
		log.Fatalf("cannot open %v", resp.Output)
	}
	enc := json.NewEncoder(outputFile)
	for _, kv := range intermediate {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("encode error %v", kv)
		}
	}
	defer outputFile.Close()
	return nil
}

func doReduceJob(reducef func(string, []string) string, resp *WorkerResp) {
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
	oname := fmt.Sprintf("tmp-%s-%d.txt", resp.JobType, rr)
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

	realname := resp.Output
	os.Rename(oname, realname)

}
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	WorkerId = int(time.Now().UnixNano())
	// uncomment to send the Example RPC to the coordinator.
	for i := 0; i < 20; i += 1 {
		time.Sleep(time.Second)
		job := getJob()

		if job.JobType == "map" {
			doMapJob(mapf, job)
		} else if job.JobType == "reduce" {
			doReduceJob(reducef, job)
		}
	}

}
func getJob() *WorkerResp {
	args := WorkerReq{WorkerId: fmt.Sprintf("%d", WorkerId)}

	resp := WorkerResp{}

	call("Coordinator.GetJob", &args, &resp)

	fmt.Printf("worker get %+v\n", resp)

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
