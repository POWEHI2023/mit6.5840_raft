package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
)

type Coordinator struct {
	sync.RWMutex // 阻止并发修改导致的错误
	// Your definitions here.
	nReduce   int64            // 剩余nReduce任务
	idCounter uint64           // 注意要8字节对齐，否则无法通过atomic执行原子操作
	sm        *StateMachine    // 状态机调度
	tasks     chan MapTaskType // 任务队列
	isOver    bool             // 是否执行结束

	// task_id:task_file
	running    map[uint64]MapTaskType // 正在执行的任务
	complished map[uint64]MapTaskType // 已经完成的任务
	// 即不正在执行，也没有完成的任务，就是超时之后重新入任务队列的任务

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	log.Println("Make coordinator server...")

	// Your code here.
	c.nReduce = int64(nReduce)
	c.idCounter = 1

	sm := StateMachine{cid: 1, coor: &c}
	c.sm = &sm

	log.Println("Init tasks in task queue...")
	// Init tasks
	c.tasks = make(chan MapTaskType, len(files))
	for _, file := range files {
		log.Println("->> Preparing task: ", file)
		c.tasks <- MapTaskType{FileName: file}
	}

	c.isOver = false
	c.running = make(map[uint64]MapTaskType)
	c.complished = make(map[uint64]MapTaskType)

	log.Println("Listening...")

	c.server()
	return &c
}

func (c *Coordinator) NextTaskId() uint64 {
	log.Println("Start allocate new task")
	oldVal := atomic.LoadUint64(&c.idCounter)
	log.Println("Start allocate new task 111")
	for atomic.CompareAndSwapUint64(&c.idCounter, oldVal, oldVal+1) != true {
		oldVal = atomic.LoadUint64(&c.idCounter)
	}
	log.Println("Allocate new task which id is ", oldVal)
	return oldVal
}

func (c *Coordinator) FetchReduceNum() int64 {
	oldVal := atomic.LoadInt64(&c.nReduce)
	if oldVal < 0 {
		return -1
	}
	for atomic.CompareAndSwapInt64(&c.nReduce, oldVal, oldVal-1) != true {
		oldVal = atomic.LoadInt64(&c.nReduce)
		if oldVal < 0 {
			return -1
		}
	}
	return oldVal
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
		log.Println("Request task from worker.")
		go s.AllocTaskForWorker(resp)
	case SubmitTask:
		log.Println("Submit task from worker.")
		go s.ProcessSubmit(submit, resp)
	default:
		/* do nothing */
	}
}

func (s *StateMachine) AllocTaskForWorker(resp *CoorResponse) {
	s.AllocTaskForWorkerHelper(resp)
	// TODO: 启动定时器，定时器绑定任务ID，超时任务重新加入任务队列

}

// 当动作是RequestTask时，设置CoorResponse内部的值，分配任务
func (s *StateMachine) AllocTaskForWorkerHelper(resp *CoorResponse) {
	if s.coor.nReduce == 0 && len(s.coor.tasks) == 0 && len(s.coor.running) == 0 {
		s.coor.isOver = true
		return
	}

	select {
	case task := <-s.coor.tasks: // 获得一个MapTask
		resp.TaskId = s.coor.NextTaskId()
		resp.Command = CoorRspMapTask
		resp.MapTask = task
	default: // 检查ReduceTask或者返回结束
		reduceNum := s.coor.FetchReduceNum()
		if reduceNum < 0 {
			resp.Command = CoorExitWorker
		} else {
			// 分配一个Reduce任务
			filenames := make([]string, 0)
			for _, v := range s.coor.complished {
				filenames = append(filenames, v.ResultFile)
			}
			task := ReduceTaskType{
				ReduceIndex: reduceNum,
				FromFiles:   filenames,
				ResultFile:  "",
			}
			resp.TaskId = s.coor.NextTaskId()
			resp.Command = CoorRspReduceTask
			resp.ReduceTask = task
		}
	}

}

func (s *StateMachine) ProcessSubmit(submit *WrokerRequest, resp *CoorResponse) {
	// TODO: 如果存在定时器，先停止定时器，根据任务ID寻找启动的定时器
	s.ProcessSubmitHelper(submit, resp)
}

// 当动作是SubmitTask时，先处理提交的任务，如果超时就丢弃返回的文件
// 然后触发RequestTask的动作
func (s *StateMachine) ProcessSubmitHelper(submit *WrokerRequest, resp *CoorResponse) {
	tid := uint64(submit.TaskId)
	{
		s.coor.RLock() // 如果已经完成，则删除中间文件后进行后续的任务
		_, exist := s.coor.complished[tid]
		if exist == true {
			os.Remove(submit.ResultFile)
			go s.TriggerEvent(RequestTask, submit, resp)
			return
		}
	}
	s.coor.Lock() // 如果未完成，检查是否在执行队列中，处理完成的任务
	val, exist := s.coor.running[tid]
	if exist == false {
		os.Remove(submit.ResultFile)
		go s.TriggerEvent(RequestTask, submit, resp)
		return
	}
	val.ResultFile = submit.ResultFile
	s.coor.complished[tid] = val

	go s.TriggerEvent(RequestTask, submit, resp)
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) CoordinatorHandler(args *WrokerRequest, reply *CoorResponse) error {
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
