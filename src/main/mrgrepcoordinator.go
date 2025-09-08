package main

import (
	"6.824/mr"
	"fmt"
	"log"
	"os"
	"time"
)

// mrcoordinator.go的另外一个版本，专门用来执行grep.so
func main() {
	if len(os.Args) < 3 {
		fmt.Println("Usage: mrcoordinator pattern inputfiles...")
		os.Exit(1)
	}

	pattern := os.Args[1]
	files := os.Args[2:]

	err := os.WriteFile("grep_pattern.conf", []byte(pattern), 0644)
	if err != nil {
		log.Fatalf("无法写入配置文件: %v", err)
	}
	defer os.Remove("grep_pattern.conf") // 作业完成后删除

	// 设置环境变量，plugin 的 init() 会读取
	os.Setenv("GREP_PATTERN", pattern)

	m := mr.MakeCoordinator(files, 1)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
