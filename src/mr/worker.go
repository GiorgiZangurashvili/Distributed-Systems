package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "os"
import "sort"
import "encoding/json"
import "strconv"
import "time"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallMyExample()
	for (true) {
		responseWithTask := ResponseWithTask{}
		AskForATask(&responseWithTask)
		if responseWithTask.TaskName == "Map" {
			Map(&responseWithTask, mapf)
		}else if responseWithTask.TaskName == "Reduce" {
			Reduce(&responseWithTask, reducef)
		}else if responseWithTask.TaskName == "Sleep" {
			time.Sleep(time.Second * time.Duration(5))
		}else {
			break
		}
	}
}

func AskForATask(responseWithTask *ResponseWithTask){
	requestForTask := RequestForTask{}
	requestForTask.Message = "Give me task!"

	call("Coordinator.GiveTask", &requestForTask, responseWithTask)
	// if ok {
	// 	fmt.Printf("filename = %v\n", responseWithTask.Filename)
	// }else{
	// 	fmt.Printf("call failed!\n")
	// }
}

func Map(responseWithTask *ResponseWithTask, mapf func(string, string) []KeyValue){
	filename := responseWithTask.Filename
	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)
	// for i := 0; i < len(intermediate); i++ {
	// 	fmt.Printf("Key: " + intermediate[i].Key + " Value: " + intermediate[i].Value)
	// }
	
	//sort intermediate pairs
	sort.Sort(ByKey(intermediate))

	partitioned_kv_pairs := make([][]KeyValue, responseWithTask.NReduce)
	for i := 0; i < len(intermediate); i++ {
		reduceIndex := ihash(intermediate[i].Key) % responseWithTask.NReduce
		partitioned_kv_pairs[reduceIndex] = append(partitioned_kv_pairs[reduceIndex], intermediate[i])
	}

	EncodeIntermediateFiles(responseWithTask, partitioned_kv_pairs)
}

func EncodeIntermediateFiles(responseWithTask *ResponseWithTask, partitioned_kv_pairs [][]KeyValue){
	for i := 0; i < responseWithTask.NReduce; i++ {
		filename := "mr-" + strconv.Itoa(responseWithTask.MapTaskNum) + "-" + strconv.Itoa(i)
		file, _ := os.Create(filename)

		enc := json.NewEncoder(file)
		for j := 0; j < len(partitioned_kv_pairs[i]); j++ {
			enc.Encode(&partitioned_kv_pairs[i][j])
		}
		CallIntermediateFilesHandler(filename, i, responseWithTask.Filename)
	}
}

func CallIntermediateFilesHandler(filename string, reduceTaskNum int, originalFilename string){
	intermediateFile := IntermediateFile{}
	intermediateFile.Filename = filename
	intermediateFile.ReduceTaskNum = reduceTaskNum
	intermediateFile.OriginalFilename = originalFilename
	responseWTask := ResponseWithTask{}
	call("Coordinator.IntermediateFilesHandler", &intermediateFile, &responseWTask)
}

func Reduce(responseWithTask *ResponseWithTask, reducef func(string, []string) string){
	intermediateFiles := GetIntermediateFiles(responseWithTask)
	oname := "mr-out-" + strconv.Itoa(responseWithTask.ReduceTaskNum)
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	sort.Sort(ByKey(intermediateFiles))
	i := 0
	for i < len(intermediateFiles) {
		j := i + 1
		for j < len(intermediateFiles) && intermediateFiles[j].Key == intermediateFiles[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateFiles[k].Value)
		}
		output := reducef(intermediateFiles[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediateFiles[i].Key, output)

		i = j
	}

	ofile.Close()

	CallMarkReduceTaskAsComplete(responseWithTask)
}

func CallMarkReduceTaskAsComplete(responseWithTask *ResponseWithTask){
	reduceIndexStruct := ReduceIndexStruct{}
	reduceIndexStruct.ReduceIndex = responseWithTask.ReduceTaskNum
	responseWTask := ResponseWithTask{}
	call("Coordinator.MarkReduceTaskAsComplete", &reduceIndexStruct, &responseWTask)
}

func GetIntermediateFiles(responseWithTask *ResponseWithTask) []KeyValue {
	kva := []KeyValue{}
	for i := 0; i < len(responseWithTask.ReduceFiles); i++ {
		filename := responseWithTask.ReduceFiles[i]
		file, _ := os.Open(filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
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
