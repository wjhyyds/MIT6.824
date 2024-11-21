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
	}
	rf.votedFor = args.CandidateId
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
}

func (rf *Raft) genAppendEntriesArgs() *AppendEntriesArgs {
	args := &AppendEntriesArgs{
		Term: 		rf.currentTerm,
		LeaderId:	rf.me,
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
	}
	rf.ChangeState(Follower)
	rf.electionTimer.Reset(RandomElectionTimeout())
	reply.Term , reply.Success = rf.currentTerm ,true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}