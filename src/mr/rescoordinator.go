package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const AliveTime = 10 * time.Second
const BackupTime = 10 * time.Second

type TaskStateType uint8

const (
	TaskIdle TaskStateType = iota
	TaskInProgress
	TaskCompleted
)

type TaskType uint8

const (
	MapTask TaskType = iota
	ReduceTask
	WaitingTask
	ExitTask
)

// 任务数据结构
type TaskStruct struct {
	TaskType TaskType // 任务类型
	TaskId   int      // 任务ID号
	FileLocs []string // 任务对应处理的文件位置切片
}

type WorkerType uint8

const (
	MapWorker WorkerType = iota
	ReduceWorker
)

type Coordinator struct {
	// Your definitions here.
	nMap        int      // MAP任务数
	nReduce     int      // REDUCE任务数
	inputSplits []string // MapReduce输入文件

	taskMu           sync.Mutex      // 用于任务相关读写的互斥锁
	mapTaskStates    []TaskStateType // MAP任务状态
	reduceTaskStates []TaskStateType // REDUCE任务状态
	mapTaskStart     []time.Time     // MAP任务启动时间
	reduceTaskStart  []time.Time     // REDUCE任务启动时间
	interLocs        [][]string      // 每个MAP任务产生的中间值文件的位置切片
	reduceLeft       int             // 剩余未完成的REDUCE任务数量

	workerMu    sync.Mutex         // 用于worker相关信息的互斥锁
	workerId    int                // 分配给worker节点的ID,单增
	workerTypes map[int]WorkerType // 非空闲worker节点类型
	workerAlive []chan struct{}    // 用于传递保活信息的信道切片
	mapTasks    [][]int            // 每个worker节点处理的MAP任务切片,用于失败重新执行
	reduceTasks []int              // 每个worker节点处理中的REDUCE任务,用于失败重新执行
}

// Your code here -- RPC handlers for the worker to call.

// 由输入文件生成MAP任务的输入数据切片
func makeInputSplits(files []string) []string {
	return files
}

// 启动执行备份任务的阈值设置
func backupThreshold(taskNum int) int {
	return taskNum / 5
}

func coordinatorPrintf(format string, a ...interface{}) (n int, err error) {
	//return fmt.Printf("Coordintor:"+format, a...)
	return 0, nil
}

// RPC:初始分配worker ID和获取REDUCE任务数
func (c *Coordinator) AssignWorkerId(args *AssignWorkerIdArgs, reply *AssignWorkerIdReply) error {
	reply.NReduce = c.nReduce

	c.workerMu.Lock()
	reply.WorkerId = c.workerId // 分配的worker ID
	c.workerId++
	// 为worker分配节点保活信道,处理的MAP任务切片,和处理中的REDUCE任务
	c.workerAlive = append(c.workerAlive, nil)
	c.mapTasks = append(c.mapTasks, make([]int, 0))
	c.reduceTasks = append(c.reduceTasks, -1)
	c.workerMu.Unlock()

	coordinatorPrintf("send a workerId %v and nReduce %v\n", reply.WorkerId, reply.NReduce)
	return nil
}

// 分配MAP任务:true表示分配了任务,false表示没有分配任务
func (c *Coordinator) assignMapTask(args *AssignTaskArgs, reply *AssignTaskReply) bool {
	inProgress := 0 // 剩余进行中的MAP任务
	c.taskMu.Lock()
	for i := range c.mapTaskStates {
		// 遍历找到空闲MAP任务分配
		if c.mapTaskStates[i] == TaskIdle {
			c.mapTaskStates[i] = TaskInProgress
			c.mapTaskStart[i] = time.Now()
			c.taskMu.Unlock()

			c.workerMu.Lock()
			// 设置当前worker节点类型
			c.workerTypes[args.WorkerId] = MapWorker
			// 将当前任务ID加入到当前worker节点处理的任务切片中
			c.mapTasks[args.WorkerId] = append(c.mapTasks[args.WorkerId], i)
			c.workerMu.Unlock()

			reply.Task = &TaskStruct{
				TaskType: MapTask,
				TaskId:   i,
				FileLocs: c.inputSplits[i : i+1], // 当前MAP任务的输入数据位置
			}
			coordinatorPrintf("send a MAP task %v/%v to worker %v\n", i, c.nMap, args.WorkerId)
			return true
		} else if c.mapTaskStates[i] == TaskInProgress {
			inProgress++
		}
	}

	// 若剩余有任务正在进行(运行到此处所有MAP任务一定已经分配)
	if inProgress > 0 {
		// 剩余进行中的任务数达到备份执行的条件
		if inProgress <= backupThreshold(c.nMap) {
			for i := range c.mapTaskStates {
				// 遍历找到进行中的MAP任务,且任务已经执行了超过备份时间,将该任务进行分配
				if c.mapTaskStates[i] == TaskInProgress && time.Now().Sub(c.mapTaskStart[i]) >= BackupTime {
					c.taskMu.Unlock()

					c.workerMu.Lock()
					c.workerTypes[args.WorkerId] = MapWorker
					c.mapTasks[args.WorkerId] = append(c.mapTasks[args.WorkerId], i)
					c.workerMu.Unlock()

					reply.Task = &TaskStruct{
						TaskType: MapTask,
						TaskId:   i,
						FileLocs: c.inputSplits[i : i+1],
					}
					coordinatorPrintf("send a Backup MAP task %v/%v to worker %v\n", i, c.nMap, args.WorkerId)
					return true
				}
			}
		}
		c.taskMu.Unlock()

		// 若所有MAP任务已分配但未全部完成返回WAITING任务
		coordinatorPrintf("all MAP tasks are assigned but not completed\n")
		reply.Task = &TaskStruct{
			TaskType: WaitingTask,
		}
		coordinatorPrintf("send a WAITING task to worker %v\n", args.WorkerId)
		return true
	}

	// 若所有MAP任务已经完成无任务分配则返回false
	c.taskMu.Unlock()
	return false
}

// 分配REDUCE任务
func (c *Coordinator) assignReduceTask(args *AssignTaskArgs, reply *AssignTaskReply) bool {
	inProgress := 0 // 剩余进行中的REDUCE任务
	c.taskMu.Lock()
	// 若所有REDUCE任务已完成无任务分配直接返回false
	if c.reduceLeft == 0 {
		c.taskMu.Unlock()
		return false
	}

	for i := range c.reduceTaskStates {
		// 遍历找到空闲REDUCE任务
		if c.reduceTaskStates[i] == TaskIdle {
			c.reduceTaskStates[i] = TaskInProgress
			c.reduceTaskStart[i] = time.Now()
			// 遍历每个MAP任务的中间文件位置切片,若有该REDUCE任务的文件则添加到切片中
			interLocs := make([]string, 0)
			for _, locs := range c.interLocs {
				if locs[i] != "" {
					interLocs = append(interLocs, locs[i])
				}
			}
			c.taskMu.Unlock()

			c.workerMu.Lock()
			c.workerTypes[args.WorkerId] = ReduceWorker
			c.reduceTasks[args.WorkerId] = i
			c.workerMu.Unlock()

			reply.Task = &TaskStruct{
				TaskType: ReduceTask,
				TaskId:   i,
				FileLocs: interLocs, // 该REDUCE任务
			}
			coordinatorPrintf("send a REDUCE task %v/%v to worker %v\n", i, c.nReduce, args.WorkerId)
			return true
		} else if c.reduceTaskStates[i] == TaskInProgress {
			inProgress++
		}
	}

	// 若剩余有任务正在进行(运行到此处所有REDUCE任务一定已经分配)
	if inProgress > 0 {
		// 剩余进行中的任务数达到备份执行的条件
		if inProgress <= backupThreshold(c.nReduce) {
			for i := range c.reduceTaskStates {
				// 遍历找到进行中的REDUCE任务,且任务已经执行了超过备份时间,将该任务进行分配
				if c.reduceTaskStates[i] == TaskInProgress && time.Now().Sub(c.reduceTaskStart[i]) >= BackupTime {
					interLocs := make([]string, 0)
					for _, locs := range c.interLocs {
						if locs[i] != "" {
							interLocs = append(interLocs, locs[i])
						}
					}
					c.taskMu.Unlock()

					c.workerMu.Lock()
					c.workerTypes[args.WorkerId] = ReduceWorker
					c.reduceTasks[args.WorkerId] = i
					c.workerMu.Unlock()

					reply.Task = &TaskStruct{
						TaskType: ReduceTask,
						TaskId:   i,
						FileLocs: interLocs,
					}
					coordinatorPrintf("send a Backup REDUCE task %v/%v to worker %v\n", i, c.nReduce, args.WorkerId)
					return true
				}
			}
		}
		c.taskMu.Unlock()

		// 若所有REDUCE任务已分配但未全部完成返回WAITING任务
		coordinatorPrintf("all REDUCE tasks are assigned but not completed\n")
		reply.Task = &TaskStruct{
			TaskType: WaitingTask,
		}
		coordinatorPrintf("send a WAITING task to worker %v\n", args.WorkerId)
		return true
	}

	// 若所有REDUCE任务已经完成无任务分配则返回false
	c.taskMu.Unlock()
	return false
}

// RPC:分配任务
func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	if args.WorkerId < 0 || args.WorkerId >= c.workerId {
		return errors.New("an error worker machine")
	}

	if c.assignMapTask(args, reply) {
		return nil
	}

	if c.assignReduceTask(args, reply) {
		return nil
	}

	// 所有任务都已完成,分配EXIT任务请求worker退出
	reply.Task = &TaskStruct{
		TaskType: ExitTask,
	}
	coordinatorPrintf("send a EXIT task to worker %v\n", args.WorkerId)
	return nil
}

// RPC:完成MAP任务
func (c *Coordinator) CompleteMap(args *CompleteMapArgs, reply *CompleteMapReply) error {
	c.taskMu.Lock()
	// 若当前MAP在进行中(若已完成则舍弃本次任务的信息,不处理)
	if c.mapTaskStates[args.TaskId] == TaskInProgress {
		// 记录该任务的中间值文件位置
		c.interLocs[args.TaskId] = args.InterLocs
		c.mapTaskStates[args.TaskId] = TaskCompleted
		coordinatorPrintf("MAP task %v is completed\n", args.TaskId)
	}
	c.taskMu.Unlock()
	return nil
}

// RPC:完成REDUCE任务
func (c *Coordinator) CompleteReduce(args *CompleteReduceArgs, reply *CompleteReduceReply) error {
	c.taskMu.Lock()
	// 若当前REDUCE在进行中(若已完成则舍弃本次任务的信息,不处理)
	if c.reduceTaskStates[args.TaskId] == TaskInProgress {
		c.reduceTaskStates[args.TaskId] = TaskCompleted
		// 剩余未完成REDUCE任务减一
		c.reduceLeft--
		coordinatorPrintf("REDUCE task %v is completed\n", args.TaskId)
	}
	c.taskMu.Unlock()
	return nil
}

// RPC:worker结点心跳机制保活
func (c *Coordinator) KeepAlive(args *KeepAliveArgs, reply *KeepAliveReply) error {
	workerId := args.WorkerId
	c.workerMu.Lock()
	defer c.workerMu.Unlock()

	if c.workerAlive[workerId] != nil { // 若当前worker已经分配了保活信道
		// 发送保活信号
		c.workerAlive[workerId] <- struct{}{}
	} else { // 若当前worker未分配保活信道
		// 分配保活信道
		aliveChan := make(chan struct{})
		c.workerAlive[workerId] = aliveChan
		// 启动保活goroutine
		go func() {
			for {
				select {
				case <-aliveChan: // 收到保活信号
					coordinatorPrintf("worker %v is alive\n", workerId)
				case <-time.After(AliveTime): // 超时则视worker节点已故障
					coordinatorPrintf("[X] worker %v is crashed\n", workerId)
					c.workerMu.Lock()
					// 将故障的worker结点标记为关闭
					workerType := c.workerTypes[workerId]
					delete(c.workerTypes, workerId)
					// 删除保活信道
					c.workerAlive[workerId] = nil

					switch workerType {
					case ReduceWorker:
						c.taskMu.Lock()
						// 将当前worker结点的REDUCE任务标记为空闲
						c.reduceTaskStates[c.reduceTasks[workerId]] = TaskIdle
						// 将当前worker结点的进行中的REDUCE任务置空
						c.reduceTasks[workerId] = -1
						c.taskMu.Unlock()
						//fallthrough
					case MapWorker:
						c.taskMu.Lock()
						// 将该worker结点处理过的MAP任务的状态标记为空闲并清空中间文件
						for _, taskId := range c.mapTasks[workerId] {
							c.mapTaskStates[taskId] = TaskIdle
							c.interLocs[taskId] = nil
						}
						// 将当前worker结点的进行的REDUCE任务置空
						c.mapTasks[workerId] = c.mapTasks[workerId][:0]
						c.taskMu.Unlock()
					}

					c.workerMu.Unlock()
					return
				}
			}
		}()
	}
	return nil
}

// RPC:退出worker节点
func (c *Coordinator) ExitWorker(args *ExitWorkerArgs, reply *ExitWorkerReply) error {
	c.workerMu.Lock()
	// 将当前worker节点标记为关闭
	delete(c.workerTypes, args.WorkerId)
	c.workerMu.Unlock()
	coordinatorPrintf("worker %v try to exit\n", args.WorkerId)
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
	ret := false

	// Your code here.
	c.taskMu.Lock()
	// 所有REDUCE任务已完成
	if c.reduceLeft != 0 {
		c.taskMu.Unlock()
		return false
	}
	c.taskMu.Unlock()

	c.workerMu.Lock()
	// 且所有worker结点都已关闭
	ret = len(c.workerTypes) == 0
	c.workerMu.Unlock()
	if ret {
		coordinatorPrintf("coordinator will exit\n")
	}
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.inputSplits = makeInputSplits(files)
	c.nMap = len(c.inputSplits)
	coordinatorPrintf("MAP task total: %v\n", c.nMap)
	c.nReduce = nReduce

	c.mapTaskStates = make([]TaskStateType, len(files))
	for i := 0; i < len(c.mapTaskStates); i++ {
		c.mapTaskStates[i] = TaskIdle
	}
	c.reduceTaskStates = make([]TaskStateType, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTaskStates[i] = TaskIdle
	}
	c.mapTaskStart = make([]time.Time, c.nMap)
	c.reduceTaskStart = make([]time.Time, nReduce)
	c.interLocs = make([][]string, c.nMap)
	// 用于Coordinator退出
	c.reduceLeft = nReduce

	c.workerId = 0
	c.workerTypes = make(map[int]WorkerType)
	c.workerAlive = make([]chan struct{}, 0)
	// 初始化每个worker节点的任务分配信息,用于失败时重新执行
	c.mapTasks = make([][]int, 0)
	c.reduceTasks = make([]int, 0)

	coordinatorPrintf("coordinator init\n")
	c.server()
	return &c
}
