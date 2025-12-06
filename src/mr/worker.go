package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

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

	// uncomment to send the Example RPC to the coordinator.

	StartWorker()

}

// TODO: 完成Map任务的处理
func ProcessMapTask(task *MapTaskType) {

}

// TODO: 完成Reduce任务的处理
func ProcessReduceTask(task *ReduceTaskType) {

}

func ProcessResponse(args *WrokerRequest, resp *CoorResponse) bool {
	switch resp.Command {
	case CoorRspMapTask: // 处理Map任务
		args.TaskId = resp.TaskId
		ProcessMapTask(&resp.MapTask)             // 复用resp中的MapTask
		args.ResultFile = resp.MapTask.ResultFile // 设置Map任务输出的中间文件
		args.Command = WorkerSubmitTask           // 设置返回命令为提交任务
	case CoorRspReduceTask: // 处理Reduce任务
		args.TaskId = resp.TaskId           // 设置返回的任务ID
		ProcessReduceTask(&resp.ReduceTask) // 复用resp中的ReduceTask
		args.Command = WorkerSubmitTask     // 设置返回命令为提交任务
	case CoorNoTaskToAlloc: // 等待一段时间再请求有没有任务
		/* do nothing */
	case CoorExitWorker: // 所有任务执行结束，退出
		return true
	}
	return false
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func StartWorker() {

	// declare an argument structure.
	args := WrokerRequest{}
	args.Command = WorkerReqTask // 第一次需要先请求一个任务
	// declare a reply structure.
	reply := CoorResponse{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.

	retryTimes := 0

	for {
		ok := call("Coordinator.CoordinatorHandler", &args, &reply)
		if ok {
			retryTimes = 0
			isOver := ProcessResponse(&args, &reply) // 对任务进行处理
			if isOver == true {
				break
			}
		} else {
			if retryTimes >= 5 {
				log.Println("call failed! more than 5 times!")
				break
			}
			retryTimes += 1
			log.Println("call failed!")
		}
	}
	log.Println("Worker execute over...")
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
