package shardctrler


import "6.824/raft"
import "6.824/labrpc"
import "sync"
import "6.824/labgob"
import "time"
import "sort"
import "log"


type CommandContext struct {
	LastId int 
	Res CommandRes
}

type CommandRes struct {
	Err Err
	WrongLeader bool
	Term int 
	Config Config
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	configs []Config // indexed by config num

	m map[int]Config
	opId map[int64]CommandContext
	res map[int]chan CommandRes
}


type Op struct {
	// Your data here.
	Type string
	RequestId int
	ClerkId int64

	//join 
	Servers map[int][]string // new GID -> servers mappings
	//leave
	GIDs  []int
	//move 
	Shard int
	GID   int
	// query
	Num int
}


func (sc *ShardCtrler) CloseChan(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	ch, ok := sc.res[index]
	if !ok {
		return
	}
	close(ch)
	delete(sc.res, index)
}

func updateShard (group map[int][]string) (shard [NShards]int) {
	id := make([]int, len(group))
	idx := 0 
	for i := range group {
		id[idx]=i
		idx++
	}

	sort.Ints(id)
	lenG := len(group)
	rate := NShards /lenG
	
	if rate == 0 {
		for i := range shard {
			shard[i] = id[i]
		}
		return 
	}

	for i := range shard {
		if i/rate >= lenG {
			shard[i] = id[lenG-1]
		} else {
			shard[i] = id[i/rate]
		}
	}
	return 
}

func (sc *ShardCtrler ) Apply( ){
	for m := range sc.applyCh {
		if m.CommandValid {
			if m.Command.(Op).Type == "Join" {
				sc.applierJoin(m)
			} else if m.Command.(Op).Type == "Leave" {
				sc.applierLeave(m)
			} else if m.Command.(Op).Type == "Move" {
				sc.applierMove(m)
			} else if m.Command.(Op).Type == "Query" {
				sc.applierQuery(m)
			} 
		}
	}
}




func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	
	if lastOp,ok := sc.opId[args.ClerkId]; ok && args.RequestId <= lastOp.LastId {
		reply.Err = "repeat"
		sc.mu.Unlock()
		return 
	}
	sc.mu.Unlock()

	DPrintf("[%v] is going to join : %v",args.ClerkId, args.Servers)

	cmd := Op{
		Type : "Join",
		Servers : args.Servers ,
		RequestId : args.RequestId,
		ClerkId : args.ClerkId,
	}

	index, term , isLeader := sc.rf.Start(cmd) 

	if !isLeader {
		reply.Err = "not leader 1"
		return 
	}

	ch := make(chan CommandRes, 1) 

	sc.mu.Lock()
	sc.res[index] = ch
	sc.mu.Unlock()

	select {
	case ans := <- ch:
		if term == ans.Term {
			reply.Err, reply.WrongLeader  = OK , false
		} else {
			reply.Err, reply.WrongLeader  =  "not leader 2", true
		}
	case <- time.After(500*time.Millisecond): 
		reply.Err, reply.WrongLeader  = "timeout", true
	}

	go sc.CloseChan(index)
}


func (sc *ShardCtrler) applierJoin(m raft.ApplyMsg){	
	sc.mu.Lock()
	op := m.Command.(Op)
	index := m.CommandIndex
	var response CommandRes
	if tmp,ok := sc.opId[op.ClerkId]; ok && tmp.LastId >= op.RequestId {
		sc.mu.Unlock()
		return  
	}


	DPrintf("[%v] in applierJoin : %v",op.ClerkId, op.Servers)

	n := len(sc.configs)
	lastConfig := sc.configs[n-1]
	var newConfig Config
	newConfig.Num = lastConfig.Num + 1
	if newConfig.Num == 1 {
		newConfig.Groups = op.Servers  
		newConfig.Shards = updateShard(newConfig.Groups)
	}else {
		newConfig.Groups = make(map[int][]string)
		for i,lcfg := range lastConfig.Groups {
			newConfig.Groups[i] = lcfg
		}
		for i,ncfg := range op.Servers {
			newConfig.Groups[i] = ncfg
		}
		newConfig.Shards = updateShard(newConfig.Groups)
	}
	sc.configs = append(sc.configs, newConfig)

	CommandTerm , _ := sc.rf.GetState()
	
	response = CommandRes {
		Err :OK, 
		Term : CommandTerm,
	}

	sc.opId[op.ClerkId] = CommandContext{op.RequestId, response}
	if ch, ok := sc.res[index]; ok{
		ch <- response
	}

	sc.mu.Unlock()
}


func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	
	if lastOp, ok:= sc.opId[args.ClerkId]; ok && args.RequestId <= lastOp.LastId {
		reply.Err = "repeat"
		sc.mu.Unlock()
		return 
	}
	sc.mu.Unlock()

	cmd := Op{
		Type : "Leave",
		RequestId : args.RequestId,
		ClerkId : args.ClerkId,
		GIDs : args.GIDs ,
	}

	index, term , isLeader := sc.rf.Start(cmd) 

	if !isLeader {
		reply.Err = "not leader 1"
		return 
	}

	ch := make(chan CommandRes, 1) 

	sc.mu.Lock()
	sc.res[index] = ch
	sc.mu.Unlock()

	select {
	case ans := <- ch:
		if term == ans.Term {
			reply.Err, reply.WrongLeader  = OK, false
		} else {
			reply.Err, reply.WrongLeader  =  "not leader 2", true
		}
	case <- time.After(500*time.Millisecond): 
		reply.Err, reply.WrongLeader  = "timeout", true
	}

	go sc.CloseChan(index)
}

func (sc *ShardCtrler) applierLeave(m raft.ApplyMsg){	
	sc.mu.Lock()
	op := m.Command.(Op)
	index := m.CommandIndex
	var response CommandRes
	if tmp,ok := sc.opId[op.ClerkId]; ok && tmp.LastId >= op.RequestId {
		sc.mu.Unlock()
		return  
	}

	n := len(sc.configs)
	lastConfig := sc.configs[n-1]

	var newConfig Config
	newConfig.Num = lastConfig.Num + 1
	newConfig.Groups = make(map[int][]string) 
	
	for gId, servers := range lastConfig.Groups {
		newConfig.Groups[gId] = servers
	}

	for _,t := range op.GIDs {
		delete(newConfig.Groups,t)
	}

	if len(newConfig.Groups) > 0 {
		newConfig.Shards = updateShard(newConfig.Groups)
	}
	sc.configs = append(sc.configs, newConfig)

	CommandTerm , _ := sc.rf.GetState()
	
	response = CommandRes {
		Err :OK, 
		Term : CommandTerm,
	}
	sc.opId[op.ClerkId] = CommandContext{op.RequestId, response}
	if ch, ok := sc.res[index]; ok{
		ch <- response
	}

	sc.mu.Unlock()
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	
	if lastOp, ok := sc.opId[args.ClerkId]; ok && args.RequestId <= lastOp.LastId {
		reply.Err = "repeat"
		sc.mu.Unlock()
		return 
	}
	sc.mu.Unlock()

	cmd := Op{
		Type : "Move",
		RequestId : args.RequestId,
		ClerkId : args.ClerkId,
		Shard : args.Shard,
		GID : args.GID,
	}

	index, term , isLeader := sc.rf.Start(cmd) 

	if !isLeader {
		reply.Err = "not leader 1"
		return 
	}

	ch := make(chan CommandRes, 1) 

	sc.mu.Lock()
	sc.res[index] = ch
	sc.mu.Unlock()

	select {
	case ans := <- ch:
		if term == ans.Term {
			reply.Err, reply.WrongLeader  = OK, false
		} else {
			reply.Err, reply.WrongLeader  =  "not leader 2", true
		}
	case <- time.After(500*time.Millisecond): 
		reply.Err, reply.WrongLeader  = "timeout", true
	}

	go sc.CloseChan(index)
}

func (sc *ShardCtrler) applierMove(m raft.ApplyMsg){	
	sc.mu.Lock()
	op := m.Command.(Op)
	index := m.CommandIndex
	var response CommandRes
	if tmp,ok := sc.opId[op.ClerkId]; ok && tmp.LastId >= op.RequestId {
		sc.mu.Unlock()
		return  
	}

	n := len(sc.configs)
	lastConfig := sc.configs[n-1]
	var newConfig Config
	newConfig.Num = lastConfig.Num + 1
	newConfig.Shards = lastConfig.Shards 
	newConfig.Groups = lastConfig.Groups
	newConfig.Shards[op.Shard] = op.GID

	sc.configs = append(sc.configs, newConfig)

	CommandTerm , _ := sc.rf.GetState()
	
	response = CommandRes {
		Err :OK, 
		Term : CommandTerm,
	}
	sc.opId[op.ClerkId] = CommandContext{op.RequestId, response}
	if ch, ok := sc.res[index]; ok{
		ch <- response
	}

	sc.mu.Unlock()
}

/*

	
} */

func max(a int,b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.mu.Lock()

	if lastOp, ok:= sc.opId[args.ClerkId]; ok && args.RequestId <= lastOp.LastId {
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = lastOp.Res.Config
		sc.mu.Unlock()
		return 
	}

	if args.Num > 0 && args.Num < len(sc.configs) {
		reply.Err, reply.Config = OK, sc.configs[args.Num]
		if request, ok := sc.opId[args.ClerkId]; ok {
			tmp := max(request.LastId, args.RequestId)
			sc.opId[args.ClerkId] = CommandContext{tmp,request.Res}
		} else {
			tmp := request.LastId
			sc.opId[args.ClerkId] = CommandContext{tmp,request.Res}
		}
		sc.mu.Unlock()
		return
	} 
	

	sc.mu.Unlock()

	cmd := Op{
		Num : args.Num,
		Type : "Query",
		RequestId : args.RequestId,
		ClerkId : args.ClerkId,
	}

	index, term , isLeader := sc.rf.Start(cmd) 

	if !isLeader {
		reply.Err = "not leader 1"
		return 
	}

	ch := make(chan CommandRes, 1) 

	sc.mu.Lock()
	sc.res[index] = ch
	sc.mu.Unlock()

	select {
	case ans := <- ch:
		if term == ans.Term {
			reply.Err, reply.WrongLeader, reply.Config  = OK, false, ans.Config
		} else {
			reply.Err, reply.WrongLeader  = "not leader 2", true
		}
	case <- time.After(100*time.Millisecond): 
		reply.Err, reply.WrongLeader  = "timeout", true
	}

	go sc.CloseChan(index)
}

func (sc *ShardCtrler) applierQuery(m raft.ApplyMsg) (config Config) {	
	sc.mu.Lock()
	op := m.Command.(Op)
	index := m.CommandIndex
	var response CommandRes
	if tmp,ok := sc.opId[op.ClerkId]; ok && tmp.LastId >= op.RequestId {
		sc.mu.Unlock()
		return  
	}
	if op.Num == -1 || op.Num > len(sc.configs)-1 {
		config = sc.configs[len(sc.configs)-1]
	} else {
		config = sc.configs[op.Num]
	}

	CommandTerm , _ := sc.rf.GetState()
	
	DPrintf("[%v] query Config : %v", op.ClerkId, len(config.Groups))

	response = CommandRes {
		Err :OK, 
		Term : CommandTerm,
		Config : config,
	}
	sc.opId[op.ClerkId] = CommandContext{op.RequestId, response}
	if ch, ok := sc.res[index]; ok{
		ch <- response
	}

	sc.mu.Unlock()

	return config
}


//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.m = make(map[int]Config)
	sc.opId = make(map[int64]CommandContext)
	sc.res = make(map[int]chan CommandRes)

	go sc.Apply()

	return sc
}


const Debug = 0

func DPrintf(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}
