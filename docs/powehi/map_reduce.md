# Introduce

Your job is to implement a distributed MapReduce, consisting of two programs, the coordinator and the worker. There will be just one coordinator process, and one or more worker processes executing in parallel. In a real system the workers would run on a bunch of different machines, but for this lab you'll run them all on a single machine. The workers will talk to the coordinator via RPC. Each worker process will, in a loop, ask the coordinator for a task, read the task's input from one or more files, execute the task, write the task's output to one or more files, and again ask the coordinator for a new task. The coordinator should notice if a worker hasn't completed its task in a reasonable amount of time (for this lab, use ten seconds), and give the same task to a different worker.

We have given you a little code to start you off. The "main" routines for the coordinator and worker are in `main/mrcoordinator.go` and `main/mrworker.go`; don't change these files. You should put your implementation in `mr/coordinator.go`, `mr/worker.go`, and `mr/rpc.go`.

# Design

`coordinator.go`提供了`Example`接口用来接收`Worker`的输入`ExampleArgs`，并将输出作为`ExampleReply`返回给`worker.go`的调用。

`worker.go`通过调用`Example`接口主动获取需要执行的任务，`coordinator.go`每分配一个任务，就会为这个任务启动一个定时器，当任务超时没有被完成之后，任务重新加入任务队列，等待下一次调度。

- 分配任务时，会为任务指定一个输出文件，并且在`Map`中记录在执行任务的信息。

- 任务完成后，停止定时器，下一次请求任务时会附带完成任务的情况，完成后的任务从`Map`中取出，放入待`Reduce`的队列中。忽视超时后提交的任务，并移除冗余的文件。

- 任务失败时，移除创建的文件，恢复任务的执行环境，请求下一次任务。

- `Map`任务全部执行结束之后，开始分配`Reduce`任务，最多分配`NReduce`个`Reduce`任务。

- `Reduce`任务会获取输出文件的锁，用于汇聚结果。

## Question(s)

- `ihash(key) % NReduce`有什么作用？
