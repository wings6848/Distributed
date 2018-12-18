package mapreduce

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
)

// Worker承担 Map阶段与Reduce阶段，然后会做一些统计，并且能够控制是否掉线
type Worker struct {
	sync.Mutex
	nRPC   	int // nRPC是用来限制这个Worker经过多少次RPC调用之后失败
	nTasks 	int // nTasks这个是记录Worker承担多少Task（Map或者Reduce）=

	l 		net.Listener // 端口监听
	name	string
	Map 	MapFunc
	Reduce 	ReduceFunc
}

// API，供Master调用
// 分为Map阶段与Reduce阶段
func (wk *Worker) DoTask(args *DoTaskArgs, _ *struct{}) error {
	fmt.Printf("%s: given %v task #%d on file %s (nios: %d)\n",
		wk.name, args.Phase, args.TaskNumber, args.File, args.NumOtherPhase)

	switch args.Phase {
	case mapPhase:
		doMap(args.JobName, args.TaskNumber, args.File, args.NumOtherPhase, wk.Map)
	case reducePhase:
		doReduce(args.JobName, args.TaskNumber, args.NumOtherPhase, wk.Reduce)
	default:
		fmt.Println("Error Task Args Phase")
	}

	fmt.Printf("%s: %v task #%d done\n", wk.name, args.Phase, args.TaskNumber)
	return nil
}

// API, 供Master调用
// 将Worker正常关闭，Worker将nTasks信息返还给Master
func (wk *Worker) Shutdown(_ *struct{}, res *ShutdownReply) error {
	debugln("Shutdown]", wk.name)
	wk.Lock()

	wk.nTasks -- // 因为我们之前统计了此API的，排除掉
	res.Ntasks = wk.nTasks
	wk.nRPC = 1 // TODO: 为什么设置为1 ???

	wk.Unlock()
	return nil
}

// Worker用来向Master注册自己的
func (wk *Worker) register(masterAddr string) {
	args := &RegisterArgs{
		Worker: wk.name,
	}

	debugln("Worker Call Addr...", masterAddr)
	err := call(masterAddr, MasterRegisterRPC, args, new(struct{}))
	if err != nil {
		fmt.Println(wk.name, "Register To Master Error: ", masterAddr, err)
	}
}

// Worker启动，进行一些初始化设置
// limit是用来退出端口监听的。
// worker初始化之后，要向Master注册自己
func RunWorker(masterAddr string, myName string,
	mapFunc MapFunc, reduceFunc ReduceFunc, limit int) {
	debugln("RunWorker]", myName)

	wk := &Worker{
		Map: mapFunc,
		name: myName,
		nRPC: limit,
		Reduce: reduceFunc,
	}

	// go内部的rpc
	rs := rpc.NewServer()
	if err := rs.Register(wk); err != nil {
		Fatal("Worker Server Start Error", err)
	}

	// 为了清除上次监听留下的 Unix Socket域
	os.Remove(myName)
	// 这里使用的是Unix Socket域
	l, err := net.Listen("unix", myName)
	if err != nil {
		panic(err)
	}

	wk.l = l
	wk.register(masterAddr)

	for {
		wk.Lock()
		if wk.nRPC == 0 {
			wk.Unlock()
			break
		}
		wk.Unlock()

		conn, err := wk.l.Accept()
		if err != nil {
			debugln("RunWorker Accept Error", err)
			break
		}

		wk.Lock()
		wk.nRPC --
		wk.Unlock()

		go rs.ServeConn(conn)

		wk.Lock()
		wk.nTasks ++ // 记录此Worker已经完成了多少个Task，需要排除掉 Shutdown API的调用。
		wk.Unlock()
	}

	wk.l.Close()
	debugln("RunWorker Exit]", myName)
}

