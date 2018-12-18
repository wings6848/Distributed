package mapreduce

import (
	"fmt"
	"time"
)

const maxRetry = 4

func (mr *Master) schedule(phase jobPhase) {
	var nTask int
	var numOtherPhase int
	switch phase {
	case mapPhase:
		nTask = len(mr.files)
		numOtherPhase = mr.nReduce
	case reducePhase:
		nTask = mr.nReduce
		numOtherPhase = len(mr.files)
	}


	fmt.Printf("Schedule: %v %v tasks (%d otherPhase)\n", nTask, phase, numOtherPhase)

	type task struct {
		id int
		err error

		file string
		retry int
	}

	doneNotice := make(chan *task, nTask)
	taskConsumer := make(chan *task, nTask)

	taskUnit := func(addr string, t *task) {
		args := &DoTaskArgs{
			File: t.file,
			Phase: phase,
			JobName: mr.jobName,
			TaskNumber: t.id,
			NumOtherPhase: numOtherPhase,
		}

		for i := 0; i < 2; i ++ {
			err := call(addr, WorkerDoTaskRPC, args, new(struct{}))
			t.err = err
			if err != nil {
				t.retry ++
				if t.retry > maxRetry {
					break
				}
				time.Sleep(time.Second * 10) // 对同一个Addr出错，给10s时间恢复。
				continue
			}
		}

		doneNotice <- t // 不会阻塞
		if t.err == nil {
			mr.registerChannel <- addr // 归还
		}
	}

	timeout := time.Second * 3
	clock := time.NewTimer(timeout) // 设置超时

	defer func() {
		close(doneNotice)
		close(taskConsumer)
	}()

	go func() {
		for i := 0; i < nTask; i ++ {
			t := &task{
				id: i,
			}

			if phase == mapPhase {
				t.file = mr.files[i]
			}

			taskConsumer <- t // 不会阻塞
		}
	}()

	var tick int
	for {
		select {
		case t, ok := <- doneNotice:
			if ! ok {
				debugln("Schedule Done Notice Error")
				return
			}
			if t.err != nil {
				debugln("Schedule Task Need Retry", phase, t.id, t.file, t.err)
				taskConsumer <- t // 不会阻塞
			}else {
				tick ++
				if t.retry > maxRetry {
					fmt.Println("Schedule Failed, Task Unit Retry > ", maxRetry)
				}
				if tick == nTask {
					debugln("Schedule All Success")
					return
				}
			}

		case t, ok := <- taskConsumer:
			if ! ok {
				debugln("Schedule Task Consumer Error")
				return
			}

			t.err = nil // 确保err为nil
			select {
			case <-clock.C:
				debugln("Wait Register Timeout... Reset Now")
				clock.Reset(timeout)
				taskConsumer <- t
			case addr := <-mr.registerChannel:
				go taskUnit(addr, t)
			}
		}
	}
}