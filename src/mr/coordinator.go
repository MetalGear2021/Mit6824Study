package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"time"
)

type TaskStatu int

const (
	TaskPending    TaskStatu = iota // 0: 未分配
	TaskInProgress                  // 1: 已分配，执行中
	TaskCompleted                   // 2: 已完成
)

type TaskInfo struct {
	TaskId        int
	TaskType      TaskType
	TaskStatu     TaskStatu //0未领取，1已领取未完成，2已完成
	TaskFileName  string    //Map任务操作的文件
	TaskReduceNum int       //总Reduce数量
	AssignedTime  time.Time //任务的注册时间，用于超时检测
}

type Coordinator struct {
	files         []string //文件名的数组
	nReduce       int
	mapTasks      []TaskInfo //map任务状态
	reduceTasks   []TaskInfo //reduce任务状态
	mu            sync.Mutex //保护数组的mutex锁
	done          bool       //任务是否完成
	lastHeartbeat time.Time  //记录上一次心态的时间
}

// RPC方法，用来被call
// TaskRequest分配一个 Map 或 Reduce 任务
// 如果没有任务了，返回 Wait 或 Exit
// 最后检查所有任务的状态
func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 1.先尝试分配Map任务
	for i := range c.mapTasks {
		if c.mapTasks[i].TaskStatu == TaskPending {
			c.mapTasks[i].TaskStatu = TaskInProgress
			c.mapTasks[i].AssignedTime = time.Now()

			reply.TaskType = MapTask
			reply.TaskId = c.mapTasks[i].TaskId
			reply.Filename = c.mapTasks[i].TaskFileName
			reply.NReduce = c.mapTasks[i].TaskReduceNum
			reply.NMap = len(c.mapTasks)
			return nil
		}
	}
	// 2.如果没Map任务了，那分配Reduce任务
	//但前提要等全部Map任务完成再继续
	allMapDone := true
	for i := range c.mapTasks {
		if c.mapTasks[i].TaskStatu != TaskCompleted {
			allMapDone = false
			break
		}
	}

	if allMapDone {
		for i := range c.reduceTasks {
			if c.reduceTasks[i].TaskStatu == TaskPending {
				c.reduceTasks[i].TaskStatu = TaskInProgress
				c.reduceTasks[i].AssignedTime = time.Now()

				reply.TaskType = ReduceTask
				reply.TaskId = c.reduceTasks[i].TaskId
				reply.NReduce = c.reduceTasks[i].TaskReduceNum
				reply.NMap = len(c.mapTasks)
				return nil
			}
		}
	}

	// 3.没有任务，但也都没完成，先等待
	// 4.如果都完成了，那就通知Worker退出程序
	if !c.done {
		reply.TaskType = WaitTask
	} else {
		reply.TaskType = ExitTask
	}
	return nil
}

func (c *Coordinator) TaskReport(args *TaskReportArgs, reply *TaskReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 标记某个任务完成,判定任务类型
	if args.TaskType == MapTask {
		c.mapTasks[args.TaskId].TaskStatu = TaskCompleted
	} else if args.TaskType == ReduceTask {
		c.reduceTasks[args.TaskId].TaskStatu = TaskCompleted
	}
	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()

	allMapDone := true
	for i := range c.mapTasks {
		if c.mapTasks[i].TaskStatu != TaskCompleted {
			allMapDone = false
			break
		}
	}

	allReduceDone := true
	for i := range c.reduceTasks {
		if c.reduceTasks[i].TaskStatu != TaskCompleted {
			allReduceDone = false
			break
		}
	}

	if allMapDone && allReduceDone && !c.done {
		c.done = true
		files, _ := filepath.Glob("mr-*-*")
		//正则表达式，匹配 mr-数字-数字
		re := regexp.MustCompile(`^mr-(\d+)-(\d+)$`)
		for _, f := range files {
			//mr-out-*也会被mr-*-*匹配到，所以要筛选一下，只删掉目标
			if re.MatchString(f) {
				os.Remove(f)
			}
		}
	}
	return c.done
}

// main/mrcoordinator.go 会调用这个方法
// 初始化 Coordinator 结构体
// 创建 Map 任务列表（每个文件一个 Map 任务）
// 创建 Reduce 任务列表（nReduce 个）
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.files = files
	c.nReduce = nReduce
	//初始化Map任务
	c.mapTasks = make([]TaskInfo, len(files))
	for i := 0; i < len(files); i++ {
		c.mapTasks[i] = TaskInfo{
			TaskId:        i,
			TaskType:      MapTask,
			TaskStatu:     TaskPending, //初始状态都是TaskPending，未领取未完成
			TaskFileName:  files[i],
			TaskReduceNum: nReduce,
			AssignedTime:  time.Time{},
		}
	}
	//初始化Reduce任务
	c.reduceTasks = make([]TaskInfo, nReduce)
	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = TaskInfo{
			TaskId:        i,
			TaskType:      ReduceTask,
			TaskStatu:     TaskPending, //初始状态都是TaskPending，未领取未完成
			TaskReduceNum: nReduce,
			AssignedTime:  time.Time{},
		}
	}

	c.done = false
	c.server()
	//启动超时检测的go routine
	go c.checkTimeout()
	return &c
}

// server()用来发起一个线程，监听来自worker的RPC请求
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//------1.TCP法-------------
	//l, e := net.Listen("tcp", ":1234")
	//------2.Unix Socket法-----
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)

	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) checkTimeout() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		c.mu.Lock()
		now := time.Now()

		//检查Map任务是否超时
		for i := range c.mapTasks {
			if c.mapTasks[i].TaskStatu == TaskInProgress && now.Sub(c.mapTasks[i].AssignedTime) > 10*time.Second {
				c.mapTasks[i].TaskStatu = TaskPending
			}
		}
		//检查Reduce任务是否超时
		for i := range c.reduceTasks {
			if c.reduceTasks[i].TaskStatu == TaskInProgress && now.Sub(c.reduceTasks[i].AssignedTime) > 10*time.Second {
				c.reduceTasks[i].TaskStatu = TaskPending
			}
		}
		c.mu.Unlock()
	}
}
