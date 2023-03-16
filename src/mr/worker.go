package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "io/ioutil"
import "strconv"
import "os"
import "bufio"
import "strings"
import "sort"
import "time"

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	for{
		args := ExampleArgs{}
		args.Types = 1
		args.Filename = ""
		reply := ExampleReply{}
		reply.Types = 0
		reply.Filename = ""
		
		//获取文件
		CallExample(&args,&reply)
		
		if reply.Types==2{
			time.Sleep(time.Second)
			//fmt.Printf("worker等待\n")
			continue
		}
		if reply.Types==3{
			//fmt.Printf("worker已完成，退出\n")
			return 
		}


		var filename string = reply.Filename
		//fmt.Printf("filename is  %v\n", filename)
			
		if reply.Types==0 {
			//获取文件句柄，文件内容
			file, err := os.Open(filename)
			if err != nil {
				fmt.Printf("cannot open %s\n", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %s\n", filename)
			}
			file.Close()
			//Map处理文件
			kva := mapf(filename, string(content))
			//kv对装入内存
			intermediate:=make([][]KeyValue,reply.NReduce)
			for _,val:= range kva {
				intermediate[ihash(val.Key)%reply.NReduce]=append(intermediate[ihash(val.Key)%reply.NReduce],val)
			}
			//写入中间文件
			for i:=0; i < reply.NReduce;i++ {
				oname := "mr-mid-"+strconv.Itoa(i)
				ofile, _ := os.OpenFile(oname, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
				for _,v :=range intermediate[i]{
					fmt.Fprintf(ofile, "%v %v\n", v.Key, v.Value)
				}
				ofile.Close()
			}
			//rpc告诉master已完成
			args.Types = 2
			args.Filename=filename
			CallExample(&args,&reply)

		}else if reply.Types==1{
			//提取文件信息
			file, _ := os.Open(filename)
			reader := bufio.NewReader(file);
			
			//转化成kv对
			intermediate := []KeyValue{}
			for{
				v, _, err := reader.ReadLine()
				if err != nil{
					break
				}
				word := strings.Split(string(v)," ")
				tmp := KeyValue{word[0], word[1]}
				intermediate=append(intermediate,tmp)
			}
			file.Close()
			//排序
			sort.Sort(ByKey(intermediate))
			//临时文件
			tmp :=strings.Split(filename,"-")[2]
			oname := "mr-out-"+tmp
			tmp_file := "mr-tmp-"+tmp
			tfile, _ := os.OpenFile(tmp_file, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)
				fmt.Fprintf(tfile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			os.Rename(tmp_file,oname)
			os.Remove(tmp_file)
			os.Remove(filename)
			tfile.Close()
			args.Types=3
			args.Filename=filename
			CallExample(&args,&reply)
		}
		
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample(args interface{}, reply interface{}) {
	call("Coordinator.Example", args, reply)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := CoordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
