package mapreduce

import "net/rpc"

const MasterRegisterRPC = "Master.Register"
const WorkerShutDownRPC = "Worker.Shutdown"
const WorkerDoTaskRPC  	= "Worker.DoTask"

// worker DoTask API参数
type DoTaskArgs struct {
	JobName    string   // 工作的名字
	File       string   // 待处理的文件名
	Phase      jobPhase // 工作类型是map还是reduce
	TaskNumber int      // 任务的索引？

	// 全部任务数量，mapper需要这个数字去计算输出的数量, 同时reducer需要知道有多少输入文件需要收集。
	NumOtherPhase int
}

// worker Shutdown API参数
type ShutdownReply struct {
	Ntasks int // 返回这个Worker执行了多少Task
}

// master Register API参数
type RegisterArgs struct {
	Worker string
}

// 封装好的rpc调用方法
func call(addr string, rpcName string, args interface{}, reply interface{}) error {
	// 在本测试中使用的是Unix Socket域
	c, err := rpc.Dial("unix", addr)
	if err != nil {
		return err
	}
	defer c.Close()

	return c.Call(rpcName, args, reply)
}