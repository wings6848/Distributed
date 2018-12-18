package mapreduce

import (
	"encoding/json"
	"hash/fnv"
	"io/ioutil"
	"os"
)

func doMap(jobName string, taskNumber int, inFile string, nReduce int, mapFunc MapFunc) {
	files := make([]*os.File, 0, nReduce)
	writers := make([]*json.Encoder, 0, nReduce)
	for i := 0; i < nReduce; i ++ {
		f, err := os.Create(reduceName(jobName, taskNumber, i))
		if err != nil {
			Fatal("Create Reduce File Error", err)
		}
		files = append(files, f)
		writers = append(writers, json.NewEncoder(f))
	}

	defer func() {
		for _, v := range files {
			v.Close()
		}
	}()

	source, err := os.Open(inFile)
	if err != nil {
		Fatal("Open Source File", inFile, "Error", err)
	}

	info, err := ioutil.ReadAll(source)
	if err != nil {
		Fatal("Read Source File", inFile, "Error", err)
	}

	keyValue := mapFunc(inFile, string(info))

	for _, kv := range keyValue {
		index := ihash(kv.Key) % uint32(nReduce)
		if err := writers[index].Encode(kv); err != nil {
			Fatal("Write Encode Error", err)
		}
	}
}

func ihash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
