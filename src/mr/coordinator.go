package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	taskId     int
	reduceNum  int
	files      []string
	mapTask    chan *Task
	reduceTask chan *Task
	tasks      map[int]*Task
	phase      Phase
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.phase {
	case MapPhase:
		{
			if len(c.mapTask) > 0 {
				*task = *<-c.mapTask
				task.State = Mapping
				task.Begin = time.Now()
				fmt.Println(task)
				log.Fatalf("[INFO] assign map task %v", task.Id)
			} else {
				task.State = Waiting
				c.toNextPhase()
			}
		}
	case ReducePhase:
		{
			if len(c.reduceTask) > 0 {
				task = <-c.reduceTask
				task.State = Reducing
				task.Begin = time.Now()
				log.Fatalf("[INFO] assign reduce task %v", task.Id)
			} else {
				task.State = Waiting
				c.toNextPhase()
			}
		}
	case AllDone:
		{
			task.State = Exit
		}
	default:
		{
			panic("undefined phase")
		}
	}
	return nil
}

func (c *Coordinator) generateId() int {
	id := c.taskId
	c.taskId++
	return id
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateId()
		task := &Task{
			Id:        id,
			Files:     []string{v},
			ReduceNum: c.reduceNum,
			State:     Waiting,
		}
		c.tasks[id] = task
		c.mapTask <- task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.reduceNum; i++ {
		id := c.generateId()
		task := &Task{
			Id:        id,
			Files:     collectReduceFiles(i),
			ReduceNum: c.reduceNum,
			State:     Waiting,
		}
		c.tasks[id] = task
		c.reduceTask <- task
	}
}

// 获取第k个reduce task需要处理的文件
func collectReduceFiles(k int) (s []string) {
	p, _ := os.Getwd()
	files, _ := ioutil.ReadDir(p)
	for _, fs := range files {
		if matched, _ := regexp.MatchString(`^mr-\d+-`+strconv.Itoa(k), fs.Name()); matched {
			s = append(s, fs.Name())
		}
	}
	return
}

func (c *Coordinator) toNextPhase() {
	alldone := func() bool {
		for _, v := range c.tasks {
			if v.State != Done {
				return false
			}
		}
		return true
	}

	if c.phase == MapPhase {
		if alldone() {
			c.makeReduceTasks()
			c.phase = ReducePhase
		}
	} else if c.phase == ReducePhase {
		// 进入reducephase保证所有map任务均完成
		if alldone() {
			c.phase = AllDone
		}
	}
}

func (c *Coordinator) SetTaskDone(args int, reply *Task) error {
	reply = c.tasks[args]
	reply.State = Done
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	// l, e := net.Listen("tcp", ":1234")
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
	ret := false

	// Your code here.
	mu.Lock()
	defer mu.Unlock()
	if c.phase == AllDone {
		fmt.Println("[INFO] all tasks done,coordinator exit")
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	c := Coordinator{
		files:      files,
		reduceNum:  nReduce,
		mapTask:    make(chan *Task, len(files)),
		reduceTask: make(chan *Task, nReduce),
		tasks:      make(map[int]*Task, len(files)+nReduce),
		phase:      MapPhase,
	}
	c.makeMapTasks(files)

	c.server()
	return &c
}
