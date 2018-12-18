package mapreduce

import (
	"encoding/json"
	"os"
)

func doReduce(jobName string, taskNumber int, nMap int, reduceFunc ReduceFunc) {
	data := make(map[string][]string)
	for i := 0; i < nMap; i ++ {
		f, err := os.Open(reduceName(jobName, i, taskNumber))
		if err != nil {
			Fatal("Reduce Open Source File Error", err)
		}

		decode := json.NewDecoder(f)
		kv := new(KeyValue)
		for decode.More() {
			if err := decode.Decode(kv); err != nil {
				Fatal("Reduce Decode Error", err)
			}
			data[kv.Key] = append(data[kv.Key], kv.Value)
		}

		f.Close()
	}

	file, err := os.Create(mergeName(jobName, taskNumber))
	if err != nil {
		Fatal("Create Merge File Error", err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)

	for k, vs := range data {
		err := encoder.Encode(KeyValue{
			Key: k,
			Value: reduceFunc(k, vs),
		})
		if err != nil {
			Fatal("Reduce Encode Error", err)
		}
	}
}