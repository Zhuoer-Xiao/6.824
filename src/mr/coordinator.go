package mr
import (
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
//定义任务的数据结构
var mu sync.Mutex

type Job struct {
	JobType    JobType//map or reduce?
	InputFile  []string
	//用于生成中间文件的标志
	JobId      int
	ReducerNum int
}


type Coordinator struct {
	// Your definitions here.
	//使用管道存放任务
	JobChannelMap        chan *Job
	JobChannelReduce     chan *Job
	ReducerNum           int
	MapNum               int
	CoordinatorCondition Condition
	jobMetaHolder        JobMetaHolder
	//维护任务序号
	uniqueJobId          int
}

type JobMetaHolder struct {
	MetaMap map[int]*JobMetaInfo
}

type JobMetaInfo struct {
	condition JobCondition
	StartTime time.Time
	JobPtr    *Job
}

func (j *JobMetaHolder) getJobMetaInfo(jobId int) (bool, *JobMetaInfo) {
	res, ok := j.MetaMap[jobId]
	return ok, res
}

func (j *JobMetaHolder) fireTheJob(jobId int) bool {
	ok, jobInfo := j.getJobMetaInfo(jobId)
	if !ok || jobInfo.condition != JobWaiting {
		return false
	}
	jobInfo.condition = JobWorking
	jobInfo.StartTime = time.Now()
	return true
}

func (j *JobMetaHolder) checkJobDone() bool {
	reduceDoneNum := 0
	reduceUndoneNum := 0
	mapDoneNum := 0
	mapUndoneNum := 0
	for _, v := range j.MetaMap {
		if v.JobPtr.JobType == MapJob {
			if v.condition == JobDone {
				mapDoneNum += 1
			} else {
				mapUndoneNum++
			}
		} else {
			if v.condition == JobDone {
				reduceDoneNum++
			} else {
				reduceUndoneNum++
			}
		}
	}
	//fmt.Printf("%d/%d map jobs are done, %d/%d reduce job are done\n",
		//mapDoneNum, mapDoneNum+mapUndoneNum, reduceDoneNum, reduceDoneNum+reduceUndoneNum)

	if (reduceDoneNum > 0 && reduceUndoneNum == 0) || (mapDoneNum > 0 && mapUndoneNum == 0) {
		return true
	}

	return false
}

func (j *JobMetaHolder) putJob(JobInfo *JobMetaInfo) bool {
	jobId := JobInfo.JobPtr.JobId
	meta, _ := j.MetaMap[jobId]
	if meta != nil {
		//fmt.Println("meta contains job which id = ", jobId)
		return false
	} else {
		j.MetaMap[jobId] = JobInfo
	}
	return true
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


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
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
	mu.Lock()
	defer mu.Unlock()
	//fmt.Println("+++++++++++++++++++++++++++++++++++++++++++")
	return c.CoordinatorCondition == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		JobChannelMap:    make(chan *Job, len(files)),
		JobChannelReduce: make(chan *Job, nReduce),
		CoordinatorCondition: MapPhase,
		ReducerNum:           nReduce,
		MapNum:               len(files),
		uniqueJobId:          0,
		jobMetaHolder: JobMetaHolder{
			MetaMap: make(map[int]*JobMetaInfo, len(files)+nReduce),
		},
	}

	c.makeMapJobs(files)
	go c.CrashDealer()
	c.server()
	return &c
}
//进入下一状态
func (c *Coordinator) nextPhase() {
	if c.CoordinatorCondition == MapPhase {
		//close(c.JobChannelMap)
		c.makeReduceJobs()
		c.CoordinatorCondition = ReducePhase
	} else if c.CoordinatorCondition == ReducePhase {
		//close(c.JobChannelReduce)
		c.CoordinatorCondition = AllDone
	}
}

//制作map任务
func (c *Coordinator) makeMapJobs(files []string) {
	for _, v := range files {
		id := c.generateJobId()
		//fmt.Println("making map job :", id)
		job := Job{
			JobType:    MapJob,
			InputFile:  []string{v},
			JobId:      id,
			ReducerNum: c.ReducerNum,
		}

		jobMetaINfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &job,
		}
		c.jobMetaHolder.putJob(&jobMetaINfo)
		//fmt.Println("making map job :", &job)
		c.JobChannelMap <- &job
	}
	//defer close(c.JobChannelMap)
	//fmt.Println("done making map jobs")
	c.jobMetaHolder.checkJobDone()
}
  
func (c *Coordinator)generateJobId() int {
	jobId:=c.uniqueJobId
	c.uniqueJobId++
	return jobId
}
//任务分配
func (c *Coordinator) DistributeJob(args *ExampleArgs, reply *Job) error {
	mu.Lock()
	defer mu.Unlock()
	//fmt.Println("coordinator get a request from worker :")
	if c.CoordinatorCondition == MapPhase {
		if len(c.JobChannelMap) > 0 {//map任务非空
			*reply = *<-c.JobChannelMap
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				//fmt.Printf("[duplicated job id]job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else if c.CoordinatorCondition == ReducePhase {
		if len(c.JobChannelReduce) > 0 {
			*reply = *<-c.JobChannelReduce
			if !c.jobMetaHolder.fireTheJob(reply.JobId) {
				//fmt.Printf("job %d is running\n", reply.JobId)
			}
		} else {
			reply.JobType = WaittingJob
			if c.jobMetaHolder.checkJobDone() {
				c.nextPhase()
			}
			return nil
		}
	} else {
		reply.JobType = KillJob
	}
	return nil
}

func (c *Coordinator) JobIsDone(args *Job, reply *ExampleReply) error {
	mu.Lock()
	defer mu.Unlock()
	switch args.JobType {
	case MapJob:
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
			//fmt.Printf("Map task on %d complete\n", args.JobId)
		} else {
			//fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	case ReduceJob:
		//fmt.Printf("Reduce task on %d complete\n", args.JobId)
		ok, meta := c.jobMetaHolder.getJobMetaInfo(args.JobId)
		//prevent a duplicated work which returned from another worker
		if ok && meta.condition == JobWorking {
			meta.condition = JobDone
		} else {
			//fmt.Println("[duplicated] job done", args.JobId)
		}
		break
	default:
		panic("wrong job done")
	}
	return nil
}

func (c *Coordinator) makeReduceJobs() {
	for i := 0; i < c.ReducerNum; i++ {
		id := c.generateJobId()
		//fmt.Println("making reduce job :", id)
		JobToDo := Job{
			JobType:   ReduceJob,
			JobId:     id,
			InputFile: TmpFileAssignHelper(i, "main/mr-tmp"),
		}
		jobMetaInfo := JobMetaInfo{
			condition: JobWaiting,
			JobPtr:    &JobToDo,
		}
		c.jobMetaHolder.putJob(&jobMetaInfo)
		c.JobChannelReduce <- &JobToDo

	}
	//defer close(c.JobChannelReduce)
	//fmt.Println("done making reduce jobs")
	c.jobMetaHolder.checkJobDone()
}



func TmpFileAssignHelper(whichReduce int, tmpFileDirectoryName string) []string {
	var res []string
	path, _ := os.Getwd()
	rd, _ := ioutil.ReadDir(path)
	for _, fi := range rd {
		if strings.HasPrefix(fi.Name(), "mr-tmp") && strings.HasSuffix(fi.Name(), strconv.Itoa(whichReduce)) {
			res = append(res, fi.Name())
		}
	}
	return res
}

func (c *Coordinator) CrashDealer() {
	for {
		time.Sleep(time.Second * 2)
		mu.Lock()
		if c.CoordinatorCondition == AllDone {
			mu.Unlock()
		}

		timenow := time.Now()
		for _, v := range c.jobMetaHolder.MetaMap {
			fmt.Println(v)
			if v.condition == JobWorking {
				fmt.Println("job", v.JobPtr.JobId, " working for ", timenow.Sub(v.StartTime))
			}
			if v.condition == JobWorking && time.Now().Sub(v.StartTime) > 5*time.Second {
				fmt.Println("detect a crash on job ", v.JobPtr.JobId)
				switch v.JobPtr.JobType {
				case MapJob:
					c.JobChannelMap <- v.JobPtr
					v.condition = JobWaiting
				case ReduceJob:
					c.JobChannelReduce <- v.JobPtr
					v.condition = JobWorking
				}
			}
		}
		mu.Unlock()
	}
}
