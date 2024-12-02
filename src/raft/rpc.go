package raft

type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) genRequestVoteArgs() *RequestVoteArgs {
	args := &RequestVoteArgs{
		Term:			rf.currentTerm,
		CandidateId: 	rf.me,
		// 当前节点日志的最后一个条目索引，用于后续同步对齐信息
		LastLogIndex: 	rf.getLastLog().Index,
		LastLogTerm: 	rf.getLastLog().Term,
	}
	return args
}
// 处理节点选举的RPC请求
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing RequestVote,  RequestVoteArgs %v and RequestVoteReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)
	// 请求的任期小于当前节点任期
	// 请求的任期等于当前节点的任期，但是当前节点以及投票给另一个候选人，则拒绝
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term , reply.VoteGranted = rf.currentTerm , false
		return
	}
	// 当前节点（可能是Leader状态）比较落后，收到了更高任期的请求，自动变follows
	if args.Term > rf.currentTerm {
		rf.ChangeState(Follower)
		rf.currentTerm , rf.votedFor = args.Term , -1
		rf.persist()
	}
	// RF代表的就是普通节点，args表示发起相应请求的节点
	if !rf.isLogUpToDate(args.LastLogIndex , args.LastLogTerm) {
		reply.Term , reply.VoteGranted = rf.currentTerm , false
		return
	}

	rf.votedFor = args.CandidateId
	rf.persist()
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term , reply.VoteGranted = rf.currentTerm , true
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// 向所有服务器发送投票RPC
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
// 实现日志复制
type AppendEntriesArgs struct {
	Term			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	LeaderCommit	int
	// 复制到更随者的日志条目列表
	Entries 		[]LogEntry
}

type AppendEntriesReply struct {
	Term 			int
	Success 		bool
	ConflictIndex	int
	ConflictTerm	int
}

func (rf *Raft) genAppendEntriesArgs(prevLogIndex int) *AppendEntriesArgs {
	firstLogIndex := rf.getFirstLog().Index
	// 从某一段复制过来强行同步
	entries := make([]LogEntry, len(rf.logs[prevLogIndex-firstLogIndex+1:]))
	copy(entries, rf.logs[prevLogIndex-firstLogIndex+1:])
	args := &AppendEntriesArgs{
		Term: 			rf.currentTerm,
		LeaderId:		rf.me,
		PrevLogIndex: 	prevLogIndex,//??
		PrevLogTerm: 	rf.logs[prevLogIndex-firstLogIndex].Term,//??
		LeaderCommit: 	rf.commitIndex,
		Entries: 		entries,//??
	}	
	return args
}

// 处理日志复制的RPC请求
func (rf *Raft) AppendEntries(args *AppendEntriesArgs , reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer DPrintf("{Node %v}'s state is {state %v, term %v}} after processing AppendEntries,  AppendEntriesArgs %v and AppendEntriesReply %v ", rf.me, rf.state, rf.currentTerm, args, reply)

	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// 当前节点是leader节点
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.persist()
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())

	// 如果当前节点上一条日志小于当前服务器的第一条日志索引，说明当前节点落后一点
	// 直接当响应的Term设置为当前服务器任期
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	// 发送args方日志落后于当前节点
	// 这段代码下的冲突在怎么处理呢
	if !rf.isLogMatched(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		lastLogIndex := rf.getLastLog().Index
		//raft节点的log小于leader节点复制过来的log
		if lastLogIndex < args.PrevLogIndex {
			reply.ConflictIndex,reply.ConflictTerm = lastLogIndex + 1 ,-1
		}else {
			// 从raft日志中从后往前找第一个冲突的任期,
			firstLogIndex := rf.getFirstLog().Index
			index := args.PrevLogIndex
			// 这里的args.Term不是不会变吗？
			// 其实就是在这里找是哪个Term冲突了
			for index >= firstLogIndex && rf.logs[index-firstLogIndex].Term == args.PrevLogTerm {
				index --
			}
		reply.ConflictIndex, reply.ConflictTerm = index + 1, args.PrevLogTerm
		}
		return
	}
	firstLogIndex := rf.getFirstLog().Index
	// raft节点更新本地条目与leader保持一致
	for index, entry := range args.Entries {
		// 如果当前的index起点已经大于等于raft节点中所有logs的长度，就直接冲这里开始复制
		// 或者从某个日志开始Term不匹配也就找到了复制的位置
		if entry.Index - firstLogIndex >= len(rf.logs) || rf.logs[entry.Index-firstLogIndex].Term != entry.Term {
			rf.logs = append(rf.logs[:entry.Index - firstLogIndex], args.Entries[index:]...)
			rf.persist()
			break
		}
	}
	// 确定从哪里开始复制后，就开始执行复制和Commit
	newCommitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
	if newCommitIndex > rf.commitIndex {
		DPrintf("{Node %v} advanced commitIndex from %v to %v with leaderCommit %v in term %v",rf.me,rf.commitIndex,newCommitIndex,args.LeaderCommit,rf.currentTerm)
		rf.commitIndex = newCommitIndex
		rf.applyCond.Signal()//通知相应groutinue可以同步日志了 !!!!!
	}

	reply.Term , reply.Success = rf.currentTerm ,true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}