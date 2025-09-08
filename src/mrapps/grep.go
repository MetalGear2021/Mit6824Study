package main

import (
	"6.824/mr"
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
)

// grep.go
// A grep application "plugin" for MapReduce.
// Build with: go build -race -buildmode=plugin grep.go

var searchPattern *regexp.Regexp

// init 函数：在 plugin 被加载时执行，用于初始化搜索模式
func init() {
	// 先尝试从文件读取,文件不存在或为空，使用默认
	data, err := os.ReadFile("grep_pattern.conf")
	var pattern string
	if err != nil || len(data) == 0 {
		pattern = ".*"
	} else {
		pattern = strings.TrimSpace(string(data))
	}

	searchPattern, err = regexp.Compile(pattern)
	if err != nil {
		// 编译失败时 panic，plugin 加载失败
		panic(fmt.Sprintf("无法编译正则表达式 '%s': %v", pattern, err))
	}
}

// Map 函数对每个输入文件调用一次。
// 输入：文件名、文件内容
// 输出：所有匹配 pattern 的行，key 为空（统一 reduce），value 为匹配的行
func Map(filename string, contents string) []mr.KeyValue {
	var kva []mr.KeyValue

	scanner := bufio.NewScanner(strings.NewReader(contents))
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Text()

		// 检查是否匹配
		if searchPattern.MatchString(line) {
			// 可选：包含文件名和行号用于调试
			// output := fmt.Sprintf("%s:%d: %s", filename, lineNum, line)
			output := line // 只输出行内容

			kva = append(kva, mr.KeyValue{
				Key:   "", // 所有匹配行归入同一个 reduce 任务
				Value: output,
			})
		}
	}

	return kva
}

// Reduce 函数对每个唯一的 key 调用一次。
// 这里 key 是 ""，values 是所有匹配的行。
// 输出：将所有匹配的行用换行符连接起来。
func Reduce(key string, values []string) string {
	// values 是所有 map 任务中匹配到的行
	return strings.Join(values, "\n") + "\n"
}
