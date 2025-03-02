package mr

import (
	"container/list"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

// 任务超时时间设置为10秒
const TaskTimeout = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	filenames []string
	nReduce   int
	nMap      int

	mapTasks    map[int]*Task
	reduceTasks map[int]*Task

	tasks          map[int]*Task
	pendingTasks   chan int
	inProgress     *list.List
	inProgressMap  map[int]*list.Element
	completedTasks map[int]struct{}

	phase Phase
	mu    sync.Mutex
}

type Task struct {
	id        int
	filename  string
	startTime time.Time
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AllocateTask(args *MessageArgs, reply *MessageReply) error {
	if c.phase == AllDone {
		reply.TaskPhase = AllDone
		return nil
	}
	select {
	case taskId := <-c.pendingTasks:
		c.mu.Lock()
		defer c.mu.Unlock()
		task := c.tasks[taskId]
		task.startTime = time.Now()
		elem := c.inProgress.PushBack(task)
		c.inProgressMap[taskId] = elem

		reply.TaskID = taskId
		reply.TaskPhase = c.phase
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap
		reply.TaskFile = task.filename
		return nil
	default:
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.inProgress.Len() > 0 {
			first := c.inProgress.Front()
			task, _ := first.Value.(*Task)
			if time.Since(task.startTime) > TaskTimeout {
				task.startTime = time.Now()
				c.inProgress.MoveToBack(first)

				reply.TaskID = task.id
				reply.TaskPhase = c.phase
				reply.NReduce = c.nReduce
				reply.NMap = c.nMap
				reply.TaskFile = task.filename
				return nil
			}
		}
		reply.TaskPhase = Wait
	}
	return nil
}

func (c *Coordinator) ReportTask(args *MessageArgs, reply *MessageReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskStatus {
	case Success:
		c.inProgress.Remove(c.inProgressMap[args.TaskID])
		delete(c.inProgressMap, args.TaskID)
		c.completedTasks[args.TaskID] = struct{}{}
		c.checkConvertPhase()
	case Failure:
		c.inProgress.Remove(c.inProgressMap[args.TaskID])
		delete(c.inProgressMap, args.TaskID)
		c.pendingTasks <- args.TaskID
		return nil
	}
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
	return c.phase == AllDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
//Cordinate 任务
//1. 初始化Coordinator， 生成所有待执行的Map任务，等待Worker调用Rpc获得任务
//2. 等待所有Map任务执行完成，生成所有待执行的Reduce任务，等待Worker调用Rpc获得任务
//3. 维护所有的任务状态，Worker请求任务，
//   如果超时的话重新分配任务，如果Worker报告任务失败的话重新分配任务
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := &Coordinator{
		filenames: files,
		nReduce:   nReduce,
		nMap:      len(files),

		tasks:          make(map[int]*Task, len(files)),
		pendingTasks:   make(chan int, len(files)),
		inProgress:     list.New(),
		inProgressMap:  make(map[int]*list.Element),
		completedTasks: make(map[int]struct{}),

		phase: MapPhase,
		mu:    sync.Mutex{},
	}

	c.initMap()
	c.server()
	return c
}

func (c *Coordinator) initMap() {
	for i, filename := range c.filenames {
		c.tasks[i] = &Task{
			id:       i,
			filename: filename,
		}
		c.pendingTasks <- i
	}
}

func (c *Coordinator) initReduce() {
	c.tasks = make(map[int]*Task, c.nReduce)
	c.pendingTasks = make(chan int, c.nReduce)
	c.inProgress = list.New()
	c.inProgressMap = make(map[int]*list.Element)
	c.completedTasks = make(map[int]struct{})

	for i := 0; i < c.nReduce; i++ {
		c.tasks[i] = &Task{
			id: i,
		}
		c.pendingTasks <- i
	}
}

func (c *Coordinator) checkConvertPhase() {
	if len(c.completedTasks) == len(c.tasks) {
		if c.phase == MapPhase {
			c.phase = ReducePhase
			c.initReduce()
		} else {
			c.phase = AllDone
		}
	}
}
