package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	nReduce        int // number of reduce task
	nMap           int // number of map task
	files          []string
	mapFinished    int        // number of finished map task
	mapTaskLog     []int      // log for map task, 0: not allocated, 1: waiting, 2:finished
	reduceFinished int        // number of finished map task
	reduceTaskLog  []int      // log for reduce task
	mu             sync.Mutex // lock
}

func (c *Coordinator) ReceiveFinishedMap(request *WorkerRequest, response *WorkerResponse) error {
	c.mu.Lock()
	c.mapFinished++
	c.mapTaskLog[request.MapTaskNumber] = 2
	c.mu.Unlock()
	return nil
}
func (c *Coordinator) ReceiveFinishedReduce(request *WorkerRequest, response *WorkerResponse) error {
	c.mu.Lock()
	c.reduceFinished++
	c.reduceTaskLog[request.ResuceTaskNumber] = 2
	c.mu.Unlock()
	return nil
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(request *WorkerRequest, response *WorkerResponse) error {
	c.mu.Lock()
	if c.mapFinished < c.nMap {
		allocate := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			response.TaskType = 2
			c.mu.Unlock()
		} else {
			response.NReduce = c.nReduce
			response.TaskType = 0
			response.MapTaskNumber = allocate
			response.Filename = c.files[allocate]
			c.mapTaskLog[allocate] = 1 // waiting
			c.mu.Unlock()              // avoid deadlock

			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				c.mu.Lock()
				if c.mapTaskLog[allocate] == 1 {
					// still waiting, assume the map worker is died
					c.mapTaskLog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else if c.mapFinished == c.nMap && c.reduceFinished < c.nReduce {
		allocate := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceTaskLog[i] == 0 {
				allocate = i
				break
			}
		}
		if allocate == -1 {
			// waiting for unfinished reduce jobs
			response.TaskType = 2
			c.mu.Unlock()
		} else {
			// allocate reduce jobs
			response.NMap = c.nMap
			response.TaskType = 1
			response.ReduceTaskNumber = allocate
			c.reduceTaskLog[allocate] = 1 // waiting
			c.mu.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second) // wait 10 seconds
				c.mu.Lock()
				if c.reduceTaskLog[allocate] == 1 {
					// still waiting, assume the reduce worker is died
					c.reduceTaskLog[allocate] = 0
				}
				c.mu.Unlock()
			}()
		}
	} else {
		response.TaskType = 3
		c.mu.Unlock()
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.

	ret := c.reduceFinished == c.nReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nMap = len(files)
	c.nReduce = nReduce
	c.mapTaskLog = make([]int, c.nMap)
	c.reduceTaskLog = make([]int, c.nReduce)
	c.server()
	return &c
}
