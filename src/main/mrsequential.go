package main

//
// simple sequential MapReduce.
//
// go run mrsequential.go wc.so pg*.txt
//

import "fmt"
import "6.824/mr"
import "plugin"
import "os"
import "log"
import "io/ioutil"
import "sort"

// for sorting by key.
type ByKey []mr.KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: mrsequential xxx.so inputfiles...\n")
		os.Exit(1)
	}
	// 从插件文件中加载 map和 reduce 函数
	// 每个mapreduce应用有一个map和一个reduce函数
	// wc.go就是一个mapreduce应用，有一个map和一个reduce函数
	// 将wc.go编译为so
	mapf, reducef := loadPlugin(os.Args[1])

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//

	// 创建一个 intermediate，用来存储map函数的输出
	intermediate := []mr.KeyValue{}
	// 读每个input文件，可以看到os.Args[2:]，是可以输入多个文件的
	for _, filename := range os.Args[2:] {
		//读每个文件
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//

	sort.Sort(ByKey(intermediate))

	//创建一个输出流，文件名为mr-out-0
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)

	//
	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		//下面就是shuffle
		j := i + 1
		//找到重复序列的开头和结尾,i,j
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		//values就是1的序列
		//比如连续两个hello,那values处理完就是"1","1"
		values := []string{}
		//[i, j) 区间
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		//把values放入reduce函数里处理，对于wordcount来说，结果就是1的数量，["1","1"]就是2
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		//把结果输入到ofile流里,格式为%v %v，值为intermediate[i].Key和output，例如hello 2
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		//因为处理了一部分，所以i变成j，跳过处理的那部分
		i = j
	}

	ofile.Close()
}

// load the application Map and Reduce functions
// from a plugin file, e.g. ../mrapps/wc.so
func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {
		log.Fatalf("cannot load plugin %v", filename)
	}

	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
