package mr

import (
	"errors"
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
	TaskId     int
	ReduceNum  int
	Files      []string
	MapTask    chan *Task
	ReduceTask chan *Task
	Tasks      map[int]*Task
	Phase      Phase
}

var mu sync.Mutex

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *TaskArgs, task *Task) error {
	mu.Lock()
	defer mu.Unlock()
	switch c.Phase {
	case MapPhase:
		{
			if len(c.MapTask) > 0 {
				*task = *<-c.MapTask
				task.State = Mapping
				task.Begin = time.Now()
				// 需要手动更新c.tasks
				c.Tasks[task.Id] = task
				log.Printf("[INFO] assign map task %v\n", task.Id)
			} else {
				task.State = Waiting
				c.toNextPhase()
			}
		}
	case ReducePhase:
		{
			if len(c.ReduceTask) > 0 {
				*task = *<-c.ReduceTask
				task.State = Reducing
				task.Begin = time.Now()
				c.Tasks[task.Id] = task
				log.Printf("[INFO] assign reduce task %v", task.Id)
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
	id := c.TaskId
	c.TaskId++
	return id
}

func (c *Coordinator) makeMapTasks(files []string) {
	for _, v := range files {
		id := c.generateId()
		task := &Task{
			Id:        id,
			Files:     []string{v},
			ReduceNum: c.ReduceNum,
			State:     Waiting,
		}
		c.Tasks[id] = task
		c.MapTask <- task
	}
}

func (c *Coordinator) makeReduceTasks() {
	for i := 0; i < c.ReduceNum; i++ {
		id := c.generateId()
		task := &Task{
			Id:        id,
			Files:     collectReduceFiles(i),
			ReduceNum: c.ReduceNum,
			State:     Waiting,
		}
		c.Tasks[id] = task
		c.ReduceTask <- task
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
		for _, v := range c.Tasks {
			if v.State != Done {
				return false
			}
		}
		return true
	}

	if c.Phase == MapPhase {
		if alldone() {
			c.makeReduceTasks()
			c.Phase = ReducePhase
		}
	} else if c.Phase == ReducePhase {
		// 进入reducephase保证所有map任务均完成
		if alldone() {
			c.Phase = AllDone
		}
	}
}

func (c *Coordinator) SetTaskDone(args int, reply *Task) error {
	mu.Lock()
	defer mu.Unlock()

	task, ok := c.Tasks[args]
	if !ok {
		log.Fatalln("[ERROR] non-existent task id ", args)
		return errors.New("Bad task id" + strconv.Itoa(args))
	}

	switch task.State {
	case Mapping, Reducing:
		{
			task.State = Done
			log.Printf("[INFO] task %v is set done", task.Id)
		}
	case Done:
		{
			log.Printf("[INFO] task %v is already done", task.Id)
		}
	case Waiting:
		{
			log.Printf("[INFO] task %v has been assigned to another worker", task.Id)
		}
	default:
		{
			log.Fatalln("[ERROR] undefined task state")
		}
	}

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
	if c.Phase == AllDone {
		log.Println("[INFO] all tasks done,coordinator exit")
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
		Files:      files,
		ReduceNum:  nReduce,
		MapTask:    make(chan *Task, len(files)),
		ReduceTask: make(chan *Task, nReduce),
		Tasks:      make(map[int]*Task, len(files)+nReduce),
		Phase:      MapPhase,
	}
	c.makeMapTasks(files)

	c.server()
	go c.CrashDetector()
	return &c
}

func (c *Coordinator) CrashDetector() {
	// 每2s检查一次
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		mu.Lock()
		if c.Phase == AllDone {
			mu.Unlock()
			break
		}

		for _, task := range c.Tasks {
			// log.Println(*task)
			if (task.State == Mapping || task.State == Reducing) && time.Since(task.Begin) > 10*time.Second {
				log.Printf("[INFO] task %v is crash,begin at %v", task.Id, task.Begin)
				if task.State == Mapping {
					task.State = Waiting
					c.MapTask <- task
				} else {
					task.State = Waiting
					c.ReduceTask <- task
				}
			}
		}

		mu.Unlock()
	}
}
