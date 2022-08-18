package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// 读取远端worker结点的文件
func openRemoteFile(filename string) (*os.File, error) {
	return os.Open(filename)
}

func workerPrintf(format string, a ...interface{}) (n int, err error) {
	//return fmt.Printf("Worker %v:"+format, a...)
	return 0, nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// 请求分配worker结点ID
	workerId, nReduce := callAssignWorkerId()
	// 启动保活goroutine
	keepAlive(workerId)

	for {
		task := callAssignTask(workerId) // 请求分配任务
		//time.Sleep(2 * time.Second)
		switch task.TaskType {
		case MapTask:
			workerPrintf("got a MAP task:%v, files:%v\n", workerId, task.TaskId, task.FileLocs)
			// 处理MAP任务
			processMapTask(mapf, task, nReduce)
		case ReduceTask:
			workerPrintf("got a REDUCE task:%v, files:%v\n", workerId, task.TaskId, task.FileLocs)
			// 处理REDUCE任务
			processReduceTask(reducef, task)
		case WaitingTask:
			workerPrintf("got a WAITING task\n", workerId)
			// 收到WAITING任务休眠一段时间
			time.Sleep(3 * time.Second)
		case ExitTask:
			workerPrintf("got EXIT task\n", workerId)
			// 通知Coordinator当前worker节点将退出
			callExitWorker(workerId)
			workerPrintf("worker %v will exit\n", workerId, workerId)
			return
		}
	}
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// 经过一段时间通知Coordinator保活
func keepAlive(workerId int) {
	go func() {
		for {
			callKeepAlive(workerId)
			workerPrintf("keep alive\n", workerId)
			time.Sleep(AliveTime >> 1)
		}
	}()
}

// 处理MAP任务
func processMapTask(mapf func(string, string) []KeyValue,
	task *TaskStruct, nReduce int) {
	var intermediate []KeyValue
	for _, filename := range task.FileLocs {
		// 读取输入数据文件
		file, err := os.Open(filename)
		if err != nil {
			log.Print(err)
			return
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Print(err)
			return
		}
		file.Close()
		// 执行MAP函数得到中间键值对
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// 将中间键值对置于对应的桶中
	interBuckets := make([][]KeyValue, nReduce)
	for _, kv := range intermediate {
		i := ihash(kv.Key) % nReduce
		interBuckets[i] = append(interBuckets[i], kv)
	}
	// 将每个桶中的键值对写入中间值文件
	interLocs := make([]string, nReduce)
	for i := 0; i < nReduce; i++ {
		tempFile, err := ioutil.TempFile(".", "mr-t-")
		if err != nil {
			log.Print(err)
			return
		}
		enc := json.NewEncoder(tempFile)
		if len(interBuckets[i]) == 0 {
			continue
		}

		for _, kv := range interBuckets[i] {
			if err := enc.Encode(&kv); err != nil {
				log.Print(err)
				return
			}
		}
		tempFile.Close()
		// 重命名临时文件
		interFilename := "mr-" + strconv.Itoa(task.TaskId) + "-" + strconv.Itoa(i)
		if err := os.Rename(tempFile.Name(), interFilename); err != nil {
			log.Print(err)
			return
		}
		// 记录当前桶的中间键值对生成的中间值文件位置
		interLocs[i] = interFilename
	}
	workerPrintf("create inter files: %v\n", "", interLocs)

	callCompleteMap(task.TaskId, interLocs)
}

// 处理REDUCE任务
func processReduceTask(reducef func(string, []string) string, task *TaskStruct) {
	// 读取中间键值对
	var intermediate []KeyValue
	for _, filename := range task.FileLocs {
		file, err := openRemoteFile(filename)
		if err != nil {
			log.Print(err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	// 分别对同一键名的键值对执行REDUCE函数
	oname := "mr-out-" + strconv.Itoa(task.TaskId)
	ofile, _ := os.Create(oname)
	defer ofile.Close()
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		// 将同一键名的值放入列表values[]
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		// 执行reduce函数
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	callCompleteReduce(task.TaskId)
}

func callAssignWorkerId() (int, int) {
	args := AssignWorkerIdArgs{}
	reply := AssignWorkerIdReply{}
	_ = call("Coordinator.AssignWorkerId", &args, &reply)
	workerPrintf("got a workerId: %v, nReduce: %v\n", "", reply.WorkerId, reply.NReduce)
	return reply.WorkerId, reply.NReduce
}

func callAssignTask(workerId int) *TaskStruct {
	args := AssignTaskArgs{
		WorkerId: workerId,
	}
	reply := AssignTaskReply{}

	if ok := call("Coordinator.AssignTask", &args, &reply); !ok {
		log.Print("callAssignTask failed")
		os.Exit(1)
	}
	workerPrintf("ask for a task\n", workerId)
	return reply.Task
}

func callCompleteMap(taskId int, interLocs []string) {
	args := CompleteMapArgs{
		TaskId:    taskId,
		InterLocs: interLocs,
	}
	reply := CompleteMapReply{}
	_ = call("Coordinator.CompleteMap", &args, &reply)
	workerPrintf("completed MAP task %v\n", "", taskId)
}

func callCompleteReduce(taskId int) {
	args := CompleteReduceArgs{
		TaskId: taskId,
	}
	reply := CompleteReduceReply{}
	_ = call("Coordinator.CompleteReduce", &args, &reply)
	workerPrintf("completed REDUCE task %v\n", "", taskId)
}

func callKeepAlive(workerId int) {
	args := KeepAliveArgs{
		WorkerId: workerId,
	}
	reply := KeepAliveReply{}
	_ = call("Coordinator.KeepAlive", &args, &reply)
}

func callExitWorker(workerId int) {
	args := ExitWorkerArgs{
		WorkerId: workerId,
	}
	reply := ExitWorkerReply{}
	_ = call("Coordinator.ExitWorker", &args, &reply)
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