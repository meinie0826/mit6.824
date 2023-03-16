package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
//mport "fmt"
import "strconv"
import "sync"
import "time"




type Coordinator struct {
	//0 未开始 1 进行中 2 已完成
	Files map[string]int
	Intermediates map[string]int
	//nReduce
	Nreduce int
	//完成情况
	ReduceSucc int
	MapSucc int
	//当前阶段 0 Map 1 Reduce 2 Done
	Mode int 
	//
	mu sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	//分配任务
	c.mu.Lock()
	reply.NReduce = c.Nreduce
	t := args.Types
	
	if t==1 {
		if c.Mode == 0 {
			var find bool = false
			for key,val:=range c.Files {
				if val==0 {
					reply.Filename = key
					reply.Types = 0
					find = true
					c.Files[key]=1
					//fmt.Printf("分配Map任务，文件名为%s\n",key)
					go func(key string){
						time.Sleep(10*time.Second)
						c.mu.Lock()
						if c.Files[key]!=2 {
							c.Files[key]=0
						}
						c.mu.Unlock()
					}(key)
					break
				}
				if find == false {
					reply.Types=2
				}
			}
			
		}else if c.Mode == 1 {
			var find bool = false
			for key,val:= range c.Intermediates {
				if val==0 {
					reply.Filename = key
					reply.Types=1
					c.Intermediates[key]=1
					find=true
					//fmt.Printf("分配Reduce任务，文件名为%s\n",key)
					go func(key string){
						time.Sleep(10*time.Second)
						c.mu.Lock()
						if c.Intermediates[key]!=2 {
							c.Intermediates[key]=0
						}
						c.mu.Unlock()
					}(key)
					break
				}
				if find == false {
					reply.Types=2
				}
			}
		}else {
			reply.Types=3
		}
	//回复
	}else if t==2 {
		//fmt.Printf("完成Map任务，文件名为%s\n",args.Filename)
		c.Files[args.Filename]=2
		c.MapSucc+=1
		if c.MapSucc == len(c.Files){
			c.Mode = 1
		}
	}else {
		//fmt.Printf("完成Reduce任务，文件名为%s\n",args.Filename)
		c.Intermediates[args.Filename]=2
		c.ReduceSucc+=1
		if c.ReduceSucc == c.Nreduce {
			c.Mode = 2
		}
		
	}	
	c.mu.Unlock()
		
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := CoordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	var res bool = false
	c.mu.Lock()
	if c.Mode==2 {
		//fmt.Printf("全部任务已完成，退出程序\n")
		res = true
	}
	c.mu.Unlock()
	return res
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.Files=make(map[string] int)
	c.Intermediates=make(map[string] int)
	c.Nreduce = nReduce
	c.ReduceSucc = 0
	c.MapSucc = 0
	c.Mode = 0

	for _,v:= range files{
		c.Files[v]=0
	}
	for i:=0;i<nReduce;i++{
		filename := "mr-mid-"+strconv.Itoa(i)
		c.Intermediates[filename]=0
	}

	c.server()
	return &c
}
