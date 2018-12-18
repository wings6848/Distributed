package mapreduce

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
)

// Master开启RPC监听
func (mr *Master) startRPCServer() {
	rpcServer := rpc.NewServer()
	rpcServer.Register(mr) // 注册自己的方法
	os.Remove(mr.address) // only needed for "unix"

	l, err := net.Listen("unix", mr.address)  // unix套接字，套接字和文件绑定
	if err != nil {
		Fatal("startRPCServer", mr.address, " error: ", err)
	}
	mr.l = l

	go func() {
	LOOP:
		for {
			select {
			case <-mr.shutdown:
				break LOOP
			default:
			}

			conn, err := mr.l.Accept()
			if err != nil {
				debugln("RegistrationServer: accept error", err)
				break
			}

			go func() {
				rpcServer.ServeConn(conn)
				conn.Close()
			}()
		}

		debugln("RegistrationServer: done")
	}()
}

// Master API, 关闭Master Server
func (mr *Master) Shutdown(_, _ *struct{}) error {
	debugln("Shutdown: registration server")
	close(mr.shutdown)
	mr.l.Close() // causes the Accept to fail
	return nil
}

// 用来关闭Master
func (mr *Master) stopRPCServer() {
	var reply ShutdownReply
	err := call(mr.address, "Master.Shutdown", new(struct{}), &reply)
	if err != nil {
		fmt.Printf("Cleanup: RPC %s error\n", mr.address)
	}
	debugln("cleanupRegistration: done")
}