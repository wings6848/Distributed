package mapreduce

import (
	"fmt"
)

const debugEnabled = true

func debugf(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

func debugln(a ...interface{})  {
	if debugEnabled {
		fmt.Println(a...)
	}
	return
}

// jobPhase代表Work当前的job类型，map阶段或者reduce阶段
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase jobPhase = "Reduce"
)

// KeyValue代表供Map、Reduce函数所用的键值对数据
type KeyValue struct {
	Key   string
	Value string
}

// MapFunc类型代表用户所定义的Map函数
type MapFunc func(string, string) []KeyValue

// ReduceFunc类型代表用户所定义的Reduce函数
type ReduceFunc func(string, []string) string

// Map阶段会为Reduce阶段提供文件，这是生成文件名称的方法
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return fmt.Sprintf("mrtmp.%s-%d-%d", jobName, mapTask, reduceTask)
}

// reduce阶段最终生成的文件
func mergeName(jobName string, reduceTask int) string {
	return fmt.Sprintf("mrtmp.%s-res-%d", jobName, reduceTask)
}

func Fatal(v ...interface{}) {
	panic(fmt.Sprintln(v...))
}