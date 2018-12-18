package mapreduce

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	nNumber = 100000
	nMap    = 100
	nReduce = 50
)

// 按行分割
func MapFuncTest(file string, value string) (res []KeyValue) {
	words := strings.Fields(value) // 分隔空格隔开的单词
	for _, w := range words {
		kv := KeyValue{w, ""}
		res = append(res, kv)
	}
	return
}

// 测试示例中不需要对reduce特殊处理
func ReduceFuncTest(key string, values []string) string {
	return ""
}

func TestSequentialSingle(t *testing.T) {
	mr := Sequential("test", makeInputs(1), 3, MapFuncTest, ReduceFuncTest)
	mr.Wait()

	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestSequentialMany(t *testing.T) {
	mr := Sequential("test", makeInputs(5), 3, MapFuncTest, ReduceFuncTest)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestBasic(t *testing.T) {
	mr := setup()
	for i := 0; i < 2; i++ {
		go RunWorker(mr.address, port("worker"+strconv.Itoa(i)),
			MapFuncTest, ReduceFuncTest, -1)
	}
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestOneFailure(t *testing.T) {
	mr := setup()
	// Start 2 workers that fail after 10 tasks
	go RunWorker(mr.address, port("worker"+strconv.Itoa(0)),
		MapFuncTest, ReduceFuncTest, 10)
	go RunWorker(mr.address, port("worker"+strconv.Itoa(1)),
		MapFuncTest, ReduceFuncTest, -1)
	mr.Wait()
	check(t, mr.files)
	checkWorker(t, mr.stats)
	cleanup(mr)
}

func TestManyFailures(t *testing.T) {
	mr := setup()
	i := 0
	done := false
	for !done {
		select {
		case done = <-mr.doneChannel:
			check(t, mr.files)
			cleanup(mr)
			break
		default:
			// Start 2 workers each sec. The workers fail after 10 tasks
			w := port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFuncTest, ReduceFuncTest, 10)
			i++
			w = port("worker" + strconv.Itoa(i))
			go RunWorker(mr.address, w, MapFuncTest, ReduceFuncTest, 10)
			i++
			time.Sleep(1 * time.Second)
		}
	}
}

// 将 nNumber个数字分布到不同的文件里面
func makeInputs(num int) []string {
	var names []string
	var i = 0
	for f := 0; f < num; f++ {
		names = append(names, fmt.Sprintf("824-mrinput-%d.txt", f))
		file, err := os.Create(names[f])
		if err != nil {
			panic(err)
		}
		w := bufio.NewWriter(file)
		for i < (f+1)*(nNumber/num) {
			fmt.Fprintf(w, "%d\n", i)
			i++
		}
		w.Flush()
		file.Close()
	}
	return names
}

// check是将原本的数据文件合并排序之后，与最终的输出文件对比
func check(t *testing.T, files []string) {
	output, err := os.Open("mrtmp.test")
	if err != nil {
		Fatal("check: ", err)
	}
	defer output.Close()

	var lines []string
	for _, f := range files {
		input, err := os.Open(f)
		if err != nil {
			Fatal("check: ", err)
		}
		inputScanner := bufio.NewScanner(input)
		for inputScanner.Scan() {
			lines = append(lines, inputScanner.Text())
		}

		input.Close()
	}

	sort.Strings(lines)

	outputScanner := bufio.NewScanner(output)
	i := 0
	for outputScanner.Scan() {
		var v1 int
		var v2 int
		text := outputScanner.Text()
		n, err := fmt.Sscanf(lines[i], "%d", &v1)
		if n == 1 && err == nil {
			n, err = fmt.Sscanf(text, "%d", &v2)
		}
		if err != nil || v1 != v2 {
			t.Fatalf("line %d: %d != %d err %v\n", i, v1, v2, err)
		}
		i++
	}
	if i != nNumber {
		t.Fatalf("Expected %d lines in output\n", nNumber)
	}
}

// 用来检查每个worker是否工作了
func checkWorker(t *testing.T, l []int) {
	for _, tasks := range l {
		if tasks == 0 {
			t.Fatalf("Some worker didn't do any work\n")
		}
	}
}

// 清理之前的临时文件
func cleanup(mr *Master) {
	mr.CleanupFiles()
	for _, f := range mr.files {
		removeFile(f)
	}
}

func setup() *Master {
	files := makeInputs(nMap)
	master := port("master")
	mr := Distributed("test", files, nReduce, master)
	return mr
}

// Unix-Socket域
func port(suffix string) string {
	s := "/var/tmp/824-"
	s += strconv.Itoa(os.Getuid()) + "/"
	os.Mkdir(s, 0777)
	s += "mr"
	s += strconv.Itoa(os.Getpid()) + "-"
	s += suffix
	return s
}