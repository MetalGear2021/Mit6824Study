package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Add your RPC definitions here.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaitTask
	ExitTask
)

type TaskRequestArgs struct {
}

type TaskRequestReply struct {
	TaskType TaskType // 任务的类型，Map，Reduce，Wait，Exit
	Filename string   // 输入文件名
	TaskId   int      //任务的ID
	NMap     int      //Map任务的数量
	NReduce  int      //Reduce任务的数量
}

type TaskReportArgs struct {
	TaskType TaskType // 任务的类型，Map，Reduce，Wait，Exit
	TaskId   int      //任务的ID
}

type TaskReportReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
