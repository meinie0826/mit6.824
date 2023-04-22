package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
	"sync"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clerkId int64
	requestId int64
	leaderId int64
	mu sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand() 
	ck.leaderId = 0
	ck.requestId = 0 
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	
	args := GetArgs{key,ck.requestId, ck.clerkId}
	DPrintf("[%v] want get Key : %v", ck.clerkId, key)
	for {
		reply := GetReply{}
		//DPrintf("[%v] send to serverid : %d to get", ck.clerkId, i)
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader  {
            DPrintf("clerkId[%v] requestId[%v] leaderId: %v GetReply: Key[%v] val:%v ", ck.clerkId, ck.requestId, ck.leaderId, args.Key,reply.Value)
			ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
        } else {
            //commit超时，或者wrongleader，需要尝试连接其他kvServer
			DPrintf("Client[%v] SeqId[%v] leaderServer: %v GetReply: Key[%v] val: %v, fail:%v ", ck.clerkId, ck.requestId, ck.leaderId, args.Key,reply.Value,reply.Err)
			ck.requestId++
			return reply.Value
        }
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{key,value,op,ck.requestId, ck.clerkId}
	for {
		reply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrTimeout || reply.Err == ErrWrongLeader   {
            DPrintf("Client[%v] SeqId[%v] leaderServer: %v GetReply: Key[%v] ", ck.clerkId, ck.requestId, ck.leaderId, args.Key)
            ck.leaderId = (ck.leaderId + 1) % int64(len(ck.servers))
        } else {
            ck.requestId++
            return 
        }	
	}
}

func (ck *Clerk) Put(key string, value string) {
	DPrintf("[%v] want put Key : %v, val : %v", ck.clerkId, key,value)
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	DPrintf("[%v] want append Key : %v, val : %v", ck.clerkId,key,value)
	ck.PutAppend(key, value, "Append")
}
