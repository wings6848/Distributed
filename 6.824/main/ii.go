package main

import (
	"fmt"
	"github.com/wings6848/Distributed/6.824/mapreduce"
	"os"
	"sort"
	"strings"
	"unicode"
)

func mapF(document string, value string) (res []mapreduce.KeyValue) {
	values := strings.FieldsFunc(value, func(r rune) bool {
		if unicode.IsSpace(r) || unicode.IsPunct(r) {
			return true
		}

		return false
	})

	for _, v := range values {
		res = append(res, mapreduce.KeyValue{
			Key: v,
			Value: document,
		})
	}
	return
}

func reduceF(key string, values []string) string {
	one := make(map[string]bool)
	need := make([]string, 0)
	for _, v := range values {
		if ! one[v] {
			need = append(need, v)
		}
		one[v] = true
	}

	sort.Strings(need)

	return fmt.Sprintf("%d %s", len(need), strings.Join(need, ","))
}

func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("iiseq", os.Args[3:], 3, mapF, reduceF)
		} else {
			mr = mapreduce.Distributed("iiseq", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapF, reduceF, 100)
	}
}