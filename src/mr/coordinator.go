package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

type Coordinator struct {
	sync.RWMutex // 阻止并发修改导致的错误
	// Your definitions here.
	nReduce int              // 剩余nReduce任务
	sm      *StateMachine    // 状态机调度
	tasks   chan MapTaskType // 任务队列
	isOver  bool             // 是否执行结束

	// task_id:task_file
	running    map[uint64]MapTaskType // 正在执行的任务
	complished map[uint64]MapTaskType // 已经完成的任务
	// 即不正在执行，也没有完成的任务，就是超时之后重新入任务队列的任务
}

// Your code here -- RPC handlers for the worker to call.

type CoorEventType uint32

const (
	RequestTask CoorEventType = iota
	SubmitTask
)

type StateMachine struct {
	cid  uint64
	coor *Coordinator
}

// 根据event出发后续的几种操作
func (s *StateMachine) TriggerEvent(
	event CoorEventType,
	submit *WrokerRequest,
	resp *CoorResponse,
) {
	switch event {
	case RequestTask:
		go s.AllocTaskForWorker(resp)
	case SubmitTask:
		go s.ProcessSubmit(submit, resp)
	default:
		/* do nothing */
	}
}

// 当动作是RequestTask时，设置CoorResponse内部的值，分配任务
func (s *StateMachine) AllocTaskForWorker(resp *CoorResponse) {
	if s.coor.nReduce == 0 && len(s.coor.tasks) == 0 && len(s.coor.running) == 0 {
		s.coor.isOver = true
		return
	}

	// todo: 分配任务
}

// 当动作是SubmitTask时，先处理提交的任务，如果超时就丢弃返回的文件
// 然后触发RequestTask的动作
func (s *StateMachine) ProcessSubmit(submit *WrokerRequest, resp *CoorResponse) {
	// todo: 处理提交

	go s.TriggerEvent(RequestTask, submit, resp)
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *WrokerRequest, reply *CoorResponse) error {
	switch args.Command {
	case WorkerReqTask:
		c.sm.TriggerEvent(RequestTask, args, reply)
	case WorkerSubmitTask:
		c.sm.TriggerEvent(SubmitTask, args, reply)
	default:
		return fmt.Errorf("Unknown request command: %s", args.Command.ToString())
	}

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
	return c.isOver
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.nReduce = nReduce
	sm := StateMachine{cid: 1, coor: &c}
	c.sm = &sm

	// Init tasks
	c.tasks = make(chan MapTaskType)
	for _, file := range files {
		c.tasks <- MapTaskType{FileName: file}
	}

	c.server()
	return &c
}
