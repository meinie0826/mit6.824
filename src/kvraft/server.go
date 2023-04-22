package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type string 
	Key   string
	Value string
	RequestId int64
	ClerkId int64
}

type CommandRes struct {
	Err Err 
	Value string 
	Term int
}

type CommandContext struct {
	LastId int64 
	Res CommandRes
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	m map[string]string
	opId map[int64]CommandContext
	res map[int] chan CommandRes 

	lastApplied int 
}

func (kv *KVServer) CloseChan(index int) {
	kv.mu.Lock()
	//DPrintf("kvserver[%d]: 开始删除通道index: %d\n", kv.me, index)
	defer kv.mu.Unlock()
	ch, ok := kv.res[index]
	if !ok {
		//若该index没有保存通道,直接结束
		DPrintf("kvserver[%d]: 无该通道index: %d\n", kv.me, index)
		return
	}
	close(ch)
	delete(kv.res, index)
	DPrintf("kvserver[%d]: 成功删除通道index: %d\n", kv.me, index)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
    cmd := Op{"Get",args.Key, "",  args.RequestId, args.ClerkId}
	
    kv.mu.Lock()
    if tmp,ok := kv.opId[cmd.ClerkId]; ok {
        if cmd.RequestId <= tmp.LastId {
			reply.Err, reply.Value =  OK, tmp.Res.Value
			kv.mu.Unlock()
			return 
		}
    }
    kv.mu.Unlock()

    index, term, isLeader := kv.rf.Start(cmd)
    if !isLeader {
        reply.Err, reply.Value = ErrWrongLeader, ""
		return 
    }

	
	ch := make(chan CommandRes ,1)
    kv.mu.Lock()
    kv.res[index] = ch
    kv.mu.Unlock()

	select {
	case response := <- ch:
		if term == response.Term {
			reply.Err, reply.Value = response.Err, response.Value
		}else {
	    	reply.Err, reply.Value = ErrWrongLeader,""
		}
	case <-time.After(500*time.Millisecond):
        reply.Err, reply.Value = ErrTimeout, ""
    }

	go kv.CloseChan(index)

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
    cmd := Op{args.Op,args.Key, args.Value, args.RequestId, args.ClerkId}
	
    kv.mu.Lock()
    if tmp,ok := kv.opId[cmd.ClerkId]; ok {
        if cmd.RequestId == tmp.LastId {
			reply.Err = tmp.Res.Err
			kv.mu.Unlock()
			return 
		}
    }
    kv.mu.Unlock()

    index, term, isLeader := kv.rf.Start(cmd)
    if !isLeader {
        reply.Err = ErrWrongLeader
		return 
    }

	ch := make(chan CommandRes ,1)
    kv.mu.Lock()
    kv.res[index] = ch
    kv.mu.Unlock()

	select {
	case response := <- ch:
		if term == response.Term {
			reply.Err = response.Err
		}else {
	    	reply.Err = ErrWrongLeader
		}
	case <-time.After(500*time.Millisecond):
        reply.Err = ErrTimeout
    }

	go kv.CloseChan(index)
}



func (kv *KVServer) applier(){
	for !kv.killed() {
		for m := range kv.applyCh {
			if m.CommandValid {
                kv.mu.Lock()
                op := m.Command.(Op)
				index := m.CommandIndex
                var response CommandRes
                if tmp,ok := kv.opId[op.ClerkId]; ok && tmp.LastId >= op.RequestId {
					response = tmp.Res
					kv.mu.Unlock()
					continue 
				}

				CommandTerm , _ := kv.rf.GetState()
				
				if op.Type == "Put"{
					kv.m[op.Key] = op.Value
					response = CommandRes{OK, op.Value,CommandTerm}
				}else if op.Type == "Append"{
					kv.m[op.Key] += op.Value
					response = CommandRes{OK, kv.m[op.Key],CommandTerm}
				}else if op.Type == "Get" {
					if value, ok := kv.m[op.Key]; ok {
						response = CommandRes{OK, value, CommandTerm}
					} else {
						response = CommandRes{ErrNoKey, value, CommandTerm}
					}
				}
			
				if ch, ok := kv.res[index]; ok{
					ch <- response
				}
				kv.opId[op.ClerkId] = CommandContext{op.RequestId, response}

                kv.mu.Unlock()
            }
        }
		
	}
}


// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.lastApplied = 0
	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.m = make(map[string]string)
	kv.opId = make(map[int64]CommandContext)
	kv.res = make(map[int]chan CommandRes)

	go kv.applier()

	return kv
}
