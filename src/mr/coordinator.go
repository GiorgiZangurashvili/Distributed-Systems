package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "time"
import "sync"
// import "fmt"

type Coordinator struct {
	// Your definitions here.
	files []string
	immutableFiles []string
	initialFilesSize int
	nReduce int
	mapTaskNumbers map[string]int
	intermediateFiles [][]string
	setOfInitialFiles map[string]bool
	mapDone bool
	lock sync.Mutex
	reduceFileIndexes []int
	done bool
	// reduceTaskNumbers map[string]int
	completedReduceIndexes []int
}

var coordinator Coordinator

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GiveTask(requestForTask *RequestForTask, responseWithTask *ResponseWithTask) error{
	c.lock.Lock()
	if c.done {
		responseWithTask.TaskName = "Done"
		c.lock.Unlock()
		return nil
	}
	if c.mapDone {
		// fmt.Printf("Hi mapDone\n")
		if len(c.reduceFileIndexes) == 0 {
			responseWithTask.TaskName = "Sleep"
			c.lock.Unlock()
			return nil
		}
		responseWithTask.NReduce = c.nReduce
		responseWithTask.ReduceTaskNum = c.reduceFileIndexes[len(c.reduceFileIndexes) - 1]
		c.reduceFileIndexes = c.reduceFileIndexes[:len(c.reduceFileIndexes) - 1]
		responseWithTask.ReduceFiles = c.intermediateFiles[responseWithTask.ReduceTaskNum]
		responseWithTask.TaskName = "Reduce"
		c.lock.Unlock()
		go c.CheckIfReduceTaskIsComplete(responseWithTask.ReduceTaskNum)
	}else{
		// fmt.Printf("Hi else\n")
		if len(c.files) == 0 {
			responseWithTask.TaskName = "Sleep"
			c.lock.Unlock()
			return nil
		}
		responseWithTask.Filename = c.files[len(c.files) - 1]
		c.files = c.files[:len(c.files) - 1]
		responseWithTask.MapTaskNum = c.mapTaskNumbers[responseWithTask.Filename]
		// if len(c.intermediateFiles) > 0 {
		// 	if len(c.intermediateFiles[responseWithTask.MapTaskNum]) > 0 {
		// 		// fmt.Printf(c.intermediateFiles[0][0] + "\n")
		// 		for i := 0; i < len(c.intermediateFiles[responseWithTask.MapTaskNum]); i++ {
		// 			fmt.Printf(c.intermediateFiles[responseWithTask.MapTaskNum][i] + "\n")
		// 		}
		// 	}
		// }
		responseWithTask.NReduce = c.nReduce
		responseWithTask.TaskName = "Map"
		c.lock.Unlock()
		go c.CheckIfMapTaskIsComplete(responseWithTask.Filename)
	}
	return nil
}

func (c *Coordinator) IntermediateFilesHandler(intermediateFile *IntermediateFile, responseWithTask *ResponseWithTask) error{
	c.lock.Lock()
	c.intermediateFiles[intermediateFile.ReduceTaskNum] = append(c.intermediateFiles[intermediateFile.ReduceTaskNum], intermediateFile.Filename)
	c.setOfInitialFiles[intermediateFile.OriginalFilename] = true
	// fmt.Printf(intermediateFile.OriginalFilename + " done!")
	c.lock.Unlock()
	return nil
}

func (c *Coordinator) MarkReduceTaskAsComplete(reduceIndex *ReduceIndexStruct, responseWithTask *ResponseWithTask) error{
	c.lock.Lock()
	if !c.ContainsIndex(reduceIndex.ReduceIndex) {
		c.completedReduceIndexes = append(c.completedReduceIndexes, reduceIndex.ReduceIndex)
	}
	c.lock.Unlock()
	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.


	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	// for i := 0; i < len(files); i++ {
	// 	fmt.Printf(files[i] + ", ")
	// }
	c.files = files
	c.immutableFiles = files
	c.initialFilesSize = len(files)
	// fmt.Printf("%d\n", c.initialFilesSize)
	c.nReduce = nReduce
	c.mapTaskNumbers = make(map[string]int)
	c.intermediateFiles = make([][]string, nReduce)
	c.setOfInitialFiles = make(map[string]bool)
	c.mapDone = false
	c.reduceFileIndexes = make([]int, nReduce)
	c.done = false
	// c.reduceTaskNumbers = make(map[string]int)
	c.completedReduceIndexes = make([]int, nReduce)

	c.MapFilenamesToIndexes()
	c.FillReduceFileIndexes()

	c.server()
	return &c
}

func (c *Coordinator) MapFilenamesToIndexes(){
	for index, value := range c.files {
		c.mapTaskNumbers[value] = index 
	}
}

// func MapFirstFilenamesToIndexes(){
// 	for i := 0; i < len(c.intermediateFiles); i++ {
// 		c.reduceTaskNumbers[c.intermediateFiles[i][0]] = i
// 	}
// }

func (c *Coordinator) CheckIfMapTaskIsComplete(filename string){
	time.Sleep(time.Second * time.Duration(10))
	c.lock.Lock()
	if c.setOfInitialFiles[filename] {
		// fmt.Printf("Worker done! " + filename + "\n")
		if c.ContainsAllFiles() {
			//this means all map tasks are complete, so we mark mapDone as true
			c.mapDone = true
			//we map intermediate files to indexes after map phase is complete
			// c.MapFirstFilenamesToIndexes()
		}
	}else {
		c.files = append(c.files, filename)
	}
	c.lock.Unlock()
}

func (c *Coordinator) CheckIfReduceTaskIsComplete(reduceIndex int){
	time.Sleep(time.Second * time.Duration(15))
	c.lock.Lock()
	if c.ContainsIndex(reduceIndex) {
		// fmt.Printf("Reduce Task Done: %d\n", reduceIndex)
		if c.ContainsAllIndexes() {
			//this means all reduce tasks are complete so we mark done as true
			// for i := 0; i < len(c.completedReduceIndexes); i++ {
			// 	fmt.Printf("c.completedReduceIndexes[%d]=%d\n", i, c.completedReduceIndexes[i])
			// }
			c.done = true
		}
	}else {
		c.reduceFileIndexes = append(c.reduceFileIndexes, reduceIndex)
	}
	c.lock.Unlock()
}

func (c *Coordinator) FillReduceFileIndexes(){
	for i := 0; i < c.nReduce; i++ {
		c.reduceFileIndexes = append(c.reduceFileIndexes, i)
	}
}

func (c *Coordinator) ContainsIndex(reduceIndex int) bool{
	for i := 0; i < len(c.completedReduceIndexes); i++ {
		if c.completedReduceIndexes[i] == reduceIndex {
			return true
		}
	}
	return false
}


func (c *Coordinator) ContainsAllFiles() bool {
	for i := 0; i < len(c.immutableFiles); i++ {
		if !c.setOfInitialFiles[c.immutableFiles[i]] {
			return false
		}
	}
	return true
}

func (c *Coordinator) ContainsAllIndexes() bool {
	for i := 0; i < c.nReduce; i++ {
		if !c.ContainsIndex(i) {
			return false
		}
	}
	return true
}