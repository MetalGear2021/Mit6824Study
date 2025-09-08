package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		// 1.向Coordinator请求任务
		args := TaskRequestArgs{}
		reply := TaskRequestReply{}
		ok := call("Coordinator.TaskRequest", &args, &reply)
		if !ok {
			time.Sleep(time.Second)
			continue
		}

		//如果走到这，说明收到coordinator的回复了
		//2.根据回复的任务类型执行{

		switch reply.TaskType {
		case MapTask:
			if DoMap(reply.TaskId, reply.Filename, reply.NReduce, mapf) == nil {
				//3.报告任务完成
				reportArgs := TaskReportArgs{
					TaskId:   reply.TaskId,
					TaskType: reply.TaskType,
				}
				reportReply := TaskReportReply{}
				//重试直到上报成功(或worker退出)
				for {
					ok := call("Coordinator.TaskReport", &reportArgs, &reportReply)
					if ok {
						break
					}
					time.Sleep(100 * time.Millisecond) //重试间隔
				}
			}
		case ReduceTask:
			if DoReduce(reply.TaskId, reply.NMap, reducef) == nil {
				//3.报告任务完成
				reportArgs := TaskReportArgs{
					TaskId:   reply.TaskId,
					TaskType: reply.TaskType,
				}
				reportReply := TaskReportReply{}
				//重试直到上报成功(或worker退出)
				for {
					ok := call("Coordinator.TaskReport", &reportArgs, &reportReply)
					if ok {
						break
					}
					time.Sleep(100 * time.Millisecond) //重试间隔
				}
			}
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}

	}
}

func DoMap(mapTaskId int, filename string, nReduce int, mapf func(string, string) []KeyValue) error {
	//Map 任务要生成 nReduce 个中间文件
	//1.读文件
	file, err := os.Open(filename)
	if err != nil {
		log.Printf("Error:cannot open %v;%v", filename, err)
		return err
	}
	content, err := ioutil.ReadAll(file)
	file.Close()
	if err != nil {
		log.Printf("Error:cannot read %v;%v", filename, err)
		return err
	}

	//2.执行Map函数
	kva := mapf(filename, string(content))

	//创建nReduce个临时文件(用指针),用数组存储
	ofiles := make([]*os.File, nReduce)
	// 在创建完所有 ofiles 后，确保所有文件最后正确关闭
	defer func() {
		for _, f := range ofiles {
			if f != nil {
				f.Close()
			}
		}
	}()
	tempFiles := make([]string, nReduce)

	for i := 0; i < nReduce; i++ {
		//Sprintf返回字符串,Fpringf返回File
		oname := fmt.Sprintf("mr-%d-%d", mapTaskId, i)
		tempFiles[i] = oname
		ofile, err := os.Create(oname)
		if err != nil {
			log.Printf("Error:cannot create %v;%v", oname, err)
			//如果有错，清理已创建的文件
			for j := 0; j < i; j++ {
				os.Remove(tempFiles[j])
			}
			return err
		}
		ofiles[i] = ofile
	}

	//把每个kv写入对应的中间文件
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		//写入时，可能有错
		_, err := fmt.Fprintf(ofiles[r], "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			log.Printf("Error:write to %v failed:%v", ofiles[r].Name(), err)
			//所有 nReduce 个文件都已创建成功，但现在写入出错，必须全部清理
			for _, fname := range tempFiles {
				os.Remove(fname)
			}
			return err
		}
	}

	return nil
}

func DoReduce(reduceTaskId int, nMap int, reducef func(string, []string) string) error {
	// 创建一个 intermediate，用来存储map函数的输出
	intermediate := []KeyValue{}
	// 读所有的临时文件
	for i := 0; i < nMap; i++ {
		//读每个文件
		filename := fmt.Sprintf("mr-%d-%d", i, reduceTaskId)
		file, err := os.Open(filename)
		if err != nil {
			fmt.Println("Error:cannot open ", filename)
			return err
		}
		scanner := bufio.NewScanner(file)
		//读取每一行
		//scanner.Scan()读取一行，返回bool,如果是true，说明读取成功，反之失败，没有后续内容
		for scanner.Scan() {
			//获取这一行
			line := scanner.Text()
			kv := strings.SplitN(line, " ", 2)
			if len(kv) == 2 {
				intermediate = append(intermediate, KeyValue{Key: kv[0], Value: kv[1]})
			}
		}
		file.Close()
	}
	//-------下面和串行版的无异-------------
	//排序
	sort.Sort(ByKey(intermediate))

	//创建一个输出流，文件名为mr-out-0
	oname := "mr-out-" + strconv.Itoa(reduceTaskId)
	ofile, _ := os.Create(oname)
	//shuffle与reduce
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()

	return nil
}

// 向coordinator发起RPC请求，然后等待回应
// 正常情况下返回true，如果返回false，说明RPC过程出错
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
