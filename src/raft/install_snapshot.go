package raft

type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) leaderInstallSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	ok := rf.sendInstallSnapshot(server, args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//过期的请求直接结束
	if !ok {
		DPrintf("id[%d].state[%v].term[%d]: 发送installSnapshot to [%d] error\n", rf.me, rf.state, rf.currentTerm, server)
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = Leader
		rf.votedFor = -1
		rf.persist()
		DPrintf("id[%d].state[%v].term[%d]: 发送installSnapshot to [%d] 过期,转变为follower\n", rf.me, rf.state, rf.currentTerm, server)
		return
	}
	//若安装成功,则更新nextIndex和matchIndex
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
	DPrintf("id[%d].state[%v].term[%d]: 发送installSnapshot to [%d] 成功,更新nextIndex->[%d];matchIndex->[%d]\n", rf.me, rf.state, rf.currentTerm, server, rf.nextIndex[server], rf.matchIndex[server])
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func() {
		reply.Term = rf.currentTerm
	}()
	//1.判断参数中的term是否小于currentTerm
	if args.Term < rf.currentTerm {
		//该快照为旧的,直接丢弃并返回
		return
	}
	DPrintf("id[%d].state[%v].term[%d]: 接收到leader[%d]的快照:lastLogIndex[%d],lastLogTerm[%d]\n", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm)
	//2.若参数中term大于currentTerm
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	//3.重置选举时间
	rf.resetElectionTimer()
	//4.转变为follower
	rf.state = Follower
	//5.若快照过期
	if args.LastIncludedIndex <= rf.commitIndex {
		DPrintf("id[%d].state[%v].term[%d]: leader[%d]的快照:lastLogIndex=[%d],lastLogTerm=[%d]已过期,commitIndex=[%d]\n", rf.me, rf.state, rf.currentTerm, args.LeaderId, args.LastIncludedIndex, args.LastIncludedTerm, rf.commitIndex)
		return
	}
	//5.通过applyCh传至service
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	go func(msg ApplyMsg) {
		rf.applyCh <- msg
	}(applyMsg)
}
