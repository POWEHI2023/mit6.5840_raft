package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type CommandType int

const (
	WorkerReqTask     CommandType = iota // Worker请求任务
	WorkerSubmitTask                     // Worker执行结束后提交任务
	CoorRspMapTask                       // 给Wroker发送Map任务
	CoorRspReduceTask                    // 给Worker发送Reduce任务
	CoorExitWorker                       // 全部执行结束后，给所有Worker发送Exit命令
)

func (c *CommandType) ToString() string {
	switch *c {
	case WorkerReqTask:
		return "WorkerReqTask"
	case WorkerSubmitTask:
		return "WorkerSubmitTask"
	case CoorRspMapTask:
		return "CoorRspMapTask"
	case CoorRspReduceTask:
		return "CoorRspReduceTask"
	case CoorExitWorker:
		return "CoorExitWorker"
	default:
		return "Unknown"
	}
}

type MapTaskType struct {
	FileName string
}

type ReduceTaskType struct {
}

type WrokerRequest struct {
	TaskId     int         // 如果提交任务，所提交任务的ID
	Command    CommandType // 任务类型
	ResultFile string      // 任务结果文件
}

type CoorResponse struct {
	TaskId     int            // 分配任务ID
	Command    CommandType    // 分配的任务类型
	ReduceTask ReduceTaskType // 分配的Reduce任务
	MapTask    MapTaskType    // 分配的Map任务
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
