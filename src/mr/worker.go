package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"time"
)

import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	for {
		request := WorkerRequest{}
		response := WorkerResponse{}
		ok := call("Coordinator.AllocateTask", &request, &response)
		if !ok || response.TaskType == 3 {
			// the master may died, which means the job is finished
			break
		}
		if response.TaskType == 0 {
			intermediate := []KeyValue{}
			// open && read the file
			file, err := os.Open(response.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", response.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", response.Filename)
			}
			file.Close()
			kva := mapf(response.Filename, string(content))
			intermediate = append(intermediate, kva...)

			// hash into buckets
			buckets := make([][]KeyValue, response.NReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kva := range intermediate {
				buckets[ihash(kva.Key)%response.NReduce] = append(buckets[ihash(kva.Key)%response.NReduce], kva)
			}
			for i := range buckets {
				oName := "mr-" + strconv.Itoa(response.MapTaskNumber) + "-" + strconv.Itoa(i)
				oFile, _ := os.CreateTemp("", oName+"*")
				encoder := json.NewEncoder(oFile)
				for _, kva := range buckets[i] {
					err := encoder.Encode(kva)
					if err != nil {
						log.Fatalf("cannot write into %v", oName)
					}
				}
				os.Rename(oFile.Name(), oName)
				oFile.Close()
			}
			finishedRequest := WorkerRequest{response.MapTaskNumber, -1}
			finishedResponse := WorkerResponse{}
			call("Coordinator.ReceiveFinishedMap", &finishedRequest, &finishedResponse)
		} else if response.TaskType == 1 {
			// reduce task
			// collect key-value from mr-X-Y
			intermediate := []KeyValue{}
			for i := 0; i < response.NMap; i++ {
				iName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(response.ReduceTaskNumber)
				// open && read the file
				file, err := os.Open(iName)
				if err != nil {
					log.Fatalf("cannot open %v", file)
				}
				decoder := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := decoder.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			// sort by key
			sort.Sort(ByKey(intermediate))

			// output file
			oname := "mr-out-" + strconv.Itoa(response.ReduceTaskNumber)
			oFile, _ := os.CreateTemp("", oname+"*")

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(oFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			os.Rename(oFile.Name(), oname)
			oFile.Close()

			for i = 0; i < response.NMap; i++ {
				iName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(response.ReduceTaskNumber)
				err := os.Remove(iName)
				if err != nil {
					log.Fatalf("cannot open delete" + iName)
				}
			}

			// send the finish message to master
			finishedRequest := WorkerRequest{-1, response.ReduceTaskNumber}
			finishedResponse := WorkerResponse{}
			call("Coordinator.ReceiveFinishedReduce", &finishedRequest, &finishedResponse)
		}
		time.Sleep(time.Second)
	}
	return
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

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
