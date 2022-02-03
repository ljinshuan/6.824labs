package mr

import (
	"fmt"
	"io/ioutil"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// use ihash(Key) % NReduce to choose the reduce
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
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for true {
		// Your worker implementation here.
		task := getTaskFromMaster()
		if task != nil && task.RemainNum > 0 {
			handleTask(task, mapf, reducef)
		} else {
			//退出
			return
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func handleTask(task *Task, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	switch task.TaskType {
	case "map":
		fmt.Printf("map task start  %v %v \n", task.MapTask.TaskId, task.FileName)
		values := make([]KeyValue, 0)
		defer func() {
			call("Coordinator.OnMapFinish", &MapTaskResult{
				MapTask:   task.MapTask,
				KeyValues: values,
			}, &CommonResult{})
		}()

		//读文件
		file, err := ioutil.ReadFile(task.FileName)
		if err != nil {
			fmt.Printf("read file error fileName:%v %v\n", task.FileName, err)
			return
		}
		task.Contents = string(file)
		values = append(mapf(task.FileName, task.Contents))

		fmt.Printf("map task finish  %v %v \n", task.MapTask.TaskId, task.FileName)

	case "reduce":
		fmt.Printf("reduce task start %v\n", task.ReduceTask.TaskId)
		sb := strings.Builder{}
		valuesMap := task.ReduceTask.KeyValuesMap
		for s, strings := range valuesMap {
			reduceAns := reducef(s, strings)
			sb.WriteString(fmt.Sprintf("%v %v\n", s, reduceAns))
		}
		ioutil.WriteFile(task.ReduceTask.TaskId, []byte(sb.String()), 0664)
		fmt.Printf("reduce task finish %v\n", task.ReduceTask.TaskId)
		call("Coordinator.OnReduceFinish", &ReduceTaskResult{
			task.ReduceTask,
		}, &CommonResult{})
	case "wait":
		fmt.Println("worker start wait task")
		time.Sleep(500 * time.Millisecond)
	case "allFinish":
		fmt.Println("all task finished  ready to exit ")
	}
}

func getTaskFromMaster() *Task {

	task := &Task{}
	ok := call("Coordinator.GetTask", &TaskRequest{WorkerId: "workder01"}, task)
	if ok {
		return task
	} else {
		fmt.Println("getTaskFromMaster error ")
	}
	return nil
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
