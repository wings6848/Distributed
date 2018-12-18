package mapreduce

import (
	"fmt"
	"net"
	"sync"
)

type Master struct {
	sync.Mutex

	address         string
	registerChannel chan string	// 通知master那些worker处于空闲状态。
	doneChannel     chan bool	// 用来通知使用方，一次Map-Reduce是否完成的channel
	workers         []string        // protected by the mutex, master下面含有的worker的名字

	jobName string   // Name of currently executing job
	files   []string // Input files
	nReduce int      // Number of reduce partitions

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	debugf("Register: worker %s\n", args.Worker)
	mr.Lock()
	defer mr.Unlock()

	mr.workers = append(mr.workers, args.Worker)
	go func() {
		mr.registerChannel <- args.Worker
	}()
	return nil
}

// master初始化
func newMaster(addr string) *Master {
	return &Master{
		address: addr,
		shutdown: make(chan struct{}),
		doneChannel: make(chan bool),
		registerChannel: make(chan string),
	}
}

// 提供序列化的Map-Reduce操作
func Sequential(jobName string, files []string, nreduce int,
	mapFunc MapFunc, reduceFunc ReduceFunc) *Master {
	mr := newMaster("Master")

	// 单机版的话，直接调用doMap、doReduce方法即可
	schedule := func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, v := range files {
				doMap(mr.jobName, i, v, nreduce, mapFunc)
			}
		case reducePhase:
			for i := 0; i < nreduce; i ++ {
				doReduce(mr.jobName, i, len(files), reduceFunc)
			}
		}
	}

	finish := func() {
		mr.stats = []int{len(files) + nreduce}
	}

	go mr.run(jobName, files, nreduce, schedule, finish)

	return mr
}

// 提供分布式环境下的操作
// 1. 先开启rpc监听
func Distributed(jobName string, files []string, nreduce int, addr string) *Master {
	mr := newMaster(addr)
	mr.startRPCServer()

	finish := func() {
		mr.stats = mr.killWorkers()
		mr.stopRPCServer()
	}

	go mr.run(jobName, files, nreduce, mr.schedule, finish)

	return mr
}

// schedule函数负责调度，finish代表完成之后的回调
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase), finish func()) {
	mr.files = files
	mr.jobName = jobName
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase) // 首先执行map阶段
	fmt.Println("Begin Reduce Phase")
	schedule(reducePhase) // 其次执行reduce阶段
	finish() // 任务完成后的回调

	mr.merge() // 最后合并任务

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)
	mr.doneChannel <- true // 发送通知信号
}

func (mr *Master) Wait() {
	<-mr.doneChannel  // 等待run运行完成
}

func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()

	numTasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debugln("Master: shutdown worker %s", w)
		reply := new(ShutdownReply)

		err := call(w, WorkerShutDownRPC, new(struct{}), reply)
		if err != nil {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		}else {
			numTasks = append(numTasks, reply.Ntasks)
		}
	}

	return numTasks
}