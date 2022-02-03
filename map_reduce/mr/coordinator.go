package mr

import (
	"fmt"
	"log"
	"sort"
	"strconv"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files          []string
	nReduce        int
	mapTasks       []*MapTask
	mapTaskMap     map[string]*MapTask
	reduceTaskMap  map[string]*ReduceTask
	mapTaskResult  map[string]*MapTaskResult
	reduceTaskLock sync.Mutex
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

func (c *Coordinator) OnMapFinish(mapResult *MapTaskResult, commonResult *CommonResult) error {

	fmt.Printf("OnMapFinish taskId:%v taskName:%v \n", mapResult.TaskId, mapResult.FileName)
	//标记为该任务已完成
	commonResult.Message = "success"
	task := c.mapTaskMap[mapResult.TaskId]
	task.Status = FINISH
	c.mapTaskResult[mapResult.TaskId] = mapResult

	go func() {
		if c.isAllMapTaskFinished() {
			//所有map任务都完成了 开始生成reduce任务
			c.initReduceTask()
		}
	}()

	return nil
}

func (c *Coordinator) OnReduceFinish(mapResult *ReduceTaskResult, commonResult *CommonResult) error {

	fmt.Printf("OnReduceFinish %v \n", mapResult.ReduceTask.TaskId)
	//标记为该任务已完成
	commonResult.Message = "success"
	task := c.reduceTaskMap[mapResult.TaskId]
	task.Status = FINISH

	return nil
}
func (c *Coordinator) GetTask(request *TaskRequest, task *Task) error {

	fmt.Printf("GetTask %v\n", request)

	initTask := c.getOneInitMapTask()
	if initTask != nil {
		task.TaskType = "map"
		task.RemainNum = 1
		task.MapTask = MapTask{
			FileName: initTask.FileName,
			Status:   initTask.Status,
			TaskId:   initTask.TaskId,
		}
		initTask.Status = DISPATCHED
		return nil
	}
	finished := c.isAllMapTaskFinished()
	if finished {
		initReduceTask := c.getOneInitReduceTask()
		if initReduceTask != nil {
			task.TaskType = "reduce"
			task.RemainNum = 1
			task.ReduceTask = ReduceTask{
				initReduceTask.TaskId,
				initReduceTask.KeyValuesMap,
				initReduceTask.Status,
			}
			initReduceTask.Status = DISPATCHED
			return nil
		}
		finished = c.isAllReduceTaskFinished()
		if finished {
			task.TaskType = "allFinish"
			return nil
		}
	}
	//maptask还未完成 等
	task.TaskType = "wait"
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
	if c.isAllReduceTaskFinished() {
		ret = true
	}
	return ret
}

func (c *Coordinator) getOneInitMapTask() *MapTask {

	for _, task := range c.mapTasks {
		if task.Status == Init {
			return task
		}
	}
	return nil
}

func (c *Coordinator) isAllMapTaskFinished() bool {

	if c.mapTaskMap == nil {
		return false
	}
	for _, task := range c.mapTaskMap {
		if task.Status != FINISH {
			return false
		}
	}
	return true
}

func (c *Coordinator) getOneInitReduceTask() *ReduceTask {

	c.reduceTaskLock.Lock()
	defer c.reduceTaskLock.Unlock()
	for _, task := range c.reduceTaskMap {
		if task.Status == Init {
			return task
		}
	}
	return nil
}

func (c *Coordinator) isAllReduceTaskFinished() bool {

	if c.reduceTaskMap == nil || len(c.reduceTaskMap) == 0 {
		return false
	}
	for _, task := range c.reduceTaskMap {
		if task.Status != FINISH {
			return false
		}
	}
	return true
}

func (c *Coordinator) initReduceTask() {

	c.reduceTaskLock.Lock()
	defer c.reduceTaskLock.Unlock()
	allKeyValues := make([]KeyValue, 0)
	for _, mapResult := range c.mapTaskResult {
		if mapResult.KeyValues != nil {
			allKeyValues = append(allKeyValues, mapResult.KeyValues...)
		}
	}
	sort.Sort(ByKey(allKeyValues))

	i := 0
	keyValuesMap := map[string][]string{}
	for i < len(allKeyValues) {
		j := i + 1
		for j < len(allKeyValues) && allKeyValues[j].Key == allKeyValues[i].Key {
			j++
		}
		//把相同的key合并成一个values
		var values []string
		for k := i; k < j; k++ {
			values = append(values, allKeyValues[k].Value)
		}
		keyValuesMap[allKeyValues[i].Key] = values
		i = j
	}

	reductTaskMap := map[string]*ReduceTask{}

	for s, keyValues := range keyValuesMap {

		hashId := ihash(s) % c.nReduce
		taskId := "reduce_task_" + strconv.Itoa(hashId)
		if task, ok := reductTaskMap[taskId]; !ok {
			task = &ReduceTask{
				TaskId:       taskId,
				KeyValuesMap: map[string][]string{},
				Status:       Init,
			}
			reductTaskMap[taskId] = task
			task.KeyValuesMap[s] = keyValues
		} else {
			task.KeyValuesMap[s] = keyValues
		}
	}
	c.reduceTaskMap = reductTaskMap
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nReduce: nReduce,
	}
	// Your code here.
	c.mapTasks = make([]*MapTask, len(files))
	c.mapTaskMap = make(map[string]*MapTask)
	c.mapTaskResult = map[string]*MapTaskResult{}
	for i, file := range files {
		c.mapTasks[i] = &MapTask{
			FileName: file,
			TaskId:   "map_task_" + strconv.Itoa(i),
			Contents: "",
			Status:   Init,
		}
		c.mapTaskMap[c.mapTasks[i].TaskId] = c.mapTasks[i]
	}

	c.server()
	return &c
}
