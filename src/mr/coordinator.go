package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterTaskStatus int

const (
	Idle MasterTaskStatus = iota
	InProgress
	Completed
)

type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

type Master struct {
	TaskQueue	chan *Task
	TaskMeta	map[int]*MasterTask
	MasterPhase	State
	NReduce		int
	InputFiles	[]string
	Intermediates	[][]string
}

type MasterTask struct {
	TaskStatus	MasterTaskStatus
	StartTime	time.Time
	TaskReference 	*Task
}

type Task struct {
	Input		string
	TaskState	State
	NReducer	int
	TaskNumber	int
	Intermediates	[]string
	Output		string
}

var mu sync.Mutex

//
// Start a thread that listens for RPC from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l,e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// 
//  main/mrmaster.go calls Exit() periodically to find out
//  if the entire job has finished
//
func (m *Master) Done() bool {
	mu.Lock()
	defer mu.Unlock()
	ret := m.MasterPhase == Exit
	return ret
}

//
//  create a Master
//  main/mrmaster.go calls this function
//  nReduce is the number of reduce tasks to use
func MakeMaster(files []string, nReduce int) *Master {
	m := Master {
		TaskQueue:	make(chan *Task, max(nReduce, len(files))),
		TaskMeta:	make(map[int]*MasterTask),
		MasterPhase:	Map,
		NReduce:	nReduce,
		InputFiles:	files,
		Intermediates:	make([][]string, nReduce),
	}

	m.createMapTask()
	
	// start server 
	m.server()
	
	// check if any task times out
	go m.catchTimeOut()
	return &m
}

func (m *Master) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		if m.MasterPhase == Exit {
			mu.Unlock()
			return
		}
		for _, masterTask := range m.TaskMeta {
			if masterTask.TaskStatus == InProgress && time.Now().Sub(masterTask.StartTime) > 30*time.Second {
				m.TaskQueue <- masterTask.TaskReference
				masterTask.TaskStatus = Idle
		}
		}
		mu.Unlock()	
	}
}

func (m *Master) createMapTask() {
	mu.Lock()
	for idx, filename := range m.InputFiles {
		taskMeta := Task {
			Input:		filename,
			TaskState: 	Map,
			NReducer:	m.NReduce,
			TaskNumber:	idx,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:	Idle,
			TaskReference:	&taskMeta,
		}
	}
	mu.Unlock()	
}

func (m *Master) createReduceTask() {
	m.TaskMeta = make(map[int]*MasterTask)
	for idx, files := range m.Intermediates {
		taskMeta := Task {
			TaskState:	Reduce,
			NReducer: 	m.NReduce,
			TaskNumber:	idx,
			Intermediates: 	files,
		}
		m.TaskQueue <- &taskMeta
		m.TaskMeta[idx] = &MasterTask{
			TaskStatus:	Idle,
			TaskReference:  &taskMeta,
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

// master waits for worker to assign tasks
func (m *Master) AssignTask(args *ExampleArgs, reply *Task) error {
	mu.Lock()
	defer mu.Unlock() 
	if len(m.TaskQueue) > 0 {
		*reply = *<-m.TaskQueue	
		// record start time of the task
		m.TaskMeta[reply.TaskNumber].TaskStatus = InProgress
		m.TaskMeta[reply.TaskNumber].StartTime	= time.Now()
	} else if m.MasterPhase == Exit {
		*reply = Task{TaskState: Exit}
	} else {
		// ask task to wait
		*reply = Task{TaskState: Wait}
	}
	return nil
}


func (m *Master) TaskCompleted(task *Task, reply *ExampleReply) error {
	// update task status
	mu.Lock()
	defer mu.Unlock()
	if task.TaskState != m.MasterPhase || m.TaskMeta[task.TaskNumber].TaskStatus == Completed {
		return nil
	}
	m.TaskMeta[task.TaskNumber].TaskStatus = Completed
	go m.processTaskResult(task)
	return nil 
} 

func (m *Master) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		// collect intermediate files
		for reduceTaskId, filePath := range task.Intermediates {
			m.Intermediates[reduceTaskId] = append(m.Intermediates[reduceTaskId], filePath)
		}
		if m.allTaskDone() {
			// enter reduce phase
			m.createReduceTask()
			m.MasterPhase = Reduce
		}
	case Reduce:
		if m.allTaskDone() {
			// enter exit phase
			m.MasterPhase = Exit
		}
	}
}

func (m *Master) allTaskDone() bool{
	for _, task := range m.TaskMeta {
		if task.TaskStatus != Completed {
			return false
		}
	}
	return true
}


