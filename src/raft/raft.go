package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex          // Lock to protect shared access to this peer's state
	// 目标服务器索引
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Persistent state on all servers(Updated on stable storage before responding to RPCs)
	currentTerm int			// latest term server has seen(initialized to 0 on first boot, increases monotonically)
	//当前节点是否已经投票        
	votedFor    int        // candidateId that received vote in current term(or null if none)
	// 日志条目列表
	logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

	// Volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

	// Volatile state on leaders(Reinitialized after election)
	// 下一个将要同步的日志索引S
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	// Leader节点维护的大多数已经统一的日志
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	// other properties
	state          NodeState     // current state of the server
	electionTimer  *time.Timer   // timer for election timeout
	heartbeatTimer *time.Timer   // timer for heartbeat
	applyCh        chan ApplyMsg // channel to send apply message to service
	// 通知一个goroutine，确认同步状态
	applyCond	   *sync.Cond
	// 通知多个goroutine应用复制
	replicatorCond []*sync.Cond
}

func (rf *Raft) ChangeState(state NodeState) {
	if rf.state == state {
		return
	}
	DPrintf("{Node %v} changes state from %v to %v", rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		// 保持一致性的措施
		// 重新设置选举时间，引入随即防止多个节点同时发起选举
		// Leader超时会变成candidate,然后随即发起选举
		rf.electionTimer.Reset(RandomElectionTimeout())
		rf.heartbeatTimer.Stop() // stop heartbeat
	case Candidate:
	case Leader:
		//变成leafer后不用再选举，
		// 定期发送心跳给follow保持联系避免产生不必要的选举
		rf.electionTimer.Stop() // stop election
		rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).

// 就是个序列化数据的
func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	//fig2中强调要持久化的三个数据
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor int
	var logs []LogEntry
		// 只要有一个东西从字节流中解码出来nil就打印报错信息
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		DPrintf("{Node %v} fails to decode persisted state", rf.me)
	}
	// 从持久化的数据里面读取三个数据，快速同步
	rf.currentTerm, rf.votedFor, rf.logs = currentTerm, votedFor, logs
	rf.lastApplied, rf.commitIndex = rf.getFirstLog().Index, rf.getFirstLog().Index
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// 核心保存快照的函数，什么时候会调用这个函数呢
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	snapshotIndex := rf.getFirstLog().Index
	//如果想要保存快照的index小于raft节点第一个index或者大于最后一份index
	if index <= snapshotIndex || index > rf.getLastLog().Index {
		DPrintf( "{Node %v} rejects replacing log with snapshotIndex %v as current snapshotIndex %v is larger in term %v",rf.me,index, snapshotIndex,rf.currentTerm)
		return
	}
	//压缩之前的日志
	rf.logs = shrinkEntries(rf.logs[index-snapshotIndex : ])
	//第一个位置为nil
	rf.logs[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot with index %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), index)
}
// 验证并应用系统快照
func (rf *Raft) CondInstallSnapshot(lastIncludeTerm int, lastIncludeIndex int, snapshot []byte) bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//快照的index比节点已经同步的还要落后
	if lastIncludeIndex <= rf.commitIndex {
		DPrintf("{Node %v} rejects outdated snapshot with lastIncludeIndex %v as current commitIndex %v is larger in term %v", rf.me, lastIncludeIndex, rf.commitIndex, rf.currentTerm)
		return false
	}
	//如果快照的index比系统最后一个还要先进
	if lastIncludeIndex > rf.getLastLog().Index {
		rf.logs = make([]LogEntry, 1)
	}else {//快照index正常落入log区间，截取log进行同步
		rf.logs = shrinkEntries(rf.logs[lastIncludeIndex-rf.getFirstLog().Index:])
		rf.logs[0].Command = nil
	}
	rf.logs[0].Term, rf.logs[0].Index = lastIncludeTerm, lastIncludeIndex
	rf.commitIndex, rf.lastApplied = lastIncludeIndex, lastIncludeIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
	DPrintf("{Node %v}'s state is {state %v,term %v,commitIndex %v,lastApplied %v,firstLog %v,lastLog %v} after accepting the snapshot which lastIncludedTerm is %v, lastIncludedIndex is %v", rf.me, rf.state, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstLog(), rf.getLastLog(), lastIncludeTerm, lastIncludeIndex)
	return true
}


// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	newLogIndex := rf.getLastLog().Index + 1
	rf.logs = append(rf.logs, LogEntry{
		Term: 		rf.currentTerm,
		Command: 	command,
		Index: 		newLogIndex,
	})
	rf.persist()
	// 每个节点先初始化自己的第一个并且默认统一
	rf.matchIndex[rf.me], rf.nextIndex[rf.me] = newLogIndex, newLogIndex + 1
	DPrintf("{Node %v} starts agreement on a new log entry with command %v in term %v", rf.me, command, rf.currentTerm)
	// 然后广播所有的raft节点统一
	rf.BroadcastHeartbeat(false)
	return newLogIndex, rf.currentTerm, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) StartElection() {
	rf.votedFor = rf.me
	rf.persist()
	args := rf.genRequestVoteArgs()
	grantedVotes := 1
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		// 使用goroutine增加并行，加快选举过程
		go func(peer int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				DPrintf("{Node %v} receives RequestVoteReply %v from {Node %v} after sending RequestVoteArgs %v", rf.me, reply, peer, args)
				if args.Term == rf.currentTerm && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVotes += 1
						if grantedVotes > len(rf.peers)/2 {
							DPrintf("{Node %v} receives over half votes", rf.me)
							rf.ChangeState(Leader)
							rf.BroadcastHeartbeat(true)
						}
					//受到的任期已经大于当前的任期，表明有leader当选了
					}else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term , -1
						rf.persist()
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat(isHeartbeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartbeat {
			//处理心跳
			go rf.replicateOnceRound(peer)
		} else {
			rf.replicatorCond[peer].Signal()
		}
	}
}

// 判断当前节点日志（Term、Index）是否是最新的S
func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
}
// 索引在arg索引内，任期相等
func (rf *Raft) isLogMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.logs[index-rf.getFirstLog().Index].Term

}
// 超半数就commit，控制每个Raft同步的日志index
func(rf *Raft) advanceCommitIndexForLeader() {
	// n表示服务器数量
	n := len(rf.matchIndex)
	// 对每个Raft节点同意的索引值进行排列
	sortMatchIndex := make([]int, n)
	copy(sortMatchIndex, rf.matchIndex)
	sort.Ints(sortMatchIndex)
	// 对每个Raft节点同意的索引值进行排列
	newCommitIndex := sortMatchIndex[n-(n/2+1)]
	// 执行同步，唤醒分布式锁
	if newCommitIndex > rf.commitIndex {
		if rf.isLogMatched(newCommitIndex, rf.currentTerm){
			DPrintf("{Node %v} advances commitIndex from %v to %v in term %v", rf.me, rf.commitIndex, newCommitIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			rf.applyCond.Signal()
		}
	}
}

//
func(rf *Raft) replicateOnceRound(peer int) {
	// 这里的rf是leader节点，reply是Raft节点
	rf.mu.RLock()
	if rf.state != Leader {
		rf.mu.RUnlock()
		return
	}
	// Leader节点维护的与每一个Raft节点的同步index
	prevLogIndex := rf.nextIndex[peer] - 1
	// ##################快照RPC逻辑##############################
	if prevLogIndex < rf.getFirstLog().Index { //太久同步，Leader太落后了？todo
		args := rf.genInstallSnapshotArgs()
		rf.mu.RUnlock()
		reply := new(InstallSnapshotReply)
		if rf.sendInstallSnapshot(peer, args, reply) {
			rf.mu.Lock()
			//确保当前还是在Leader的任期和Term中
			if rf.state == Leader && rf.currentTerm == args.Term {
				//防止在应用snapshot中Term又发生了改变，即reply要更先进
				if reply.Term > rf.currentTerm {
					rf.ChangeState(Follower)
					rf.currentTerm, rf.votedFor = reply.Term, -1
					rf.persist()
				}else { //没有异常，成功快照
					//TODO3这里成功应用这两个index和channel里面的数据有什么区别
					// 是不是log在test_test.go中应用了，raft这里只需要维护index就行
					rf.nextIndex[peer]  = args.LastIncludeIndex + 1
					rf.matchIndex[peer] = args.LastIncludeIndex
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends InstallSnapshotArgs %v to {Node %v} and receives InstallSnapshotReply %v", rf.me, args, peer, reply)
		}
	}else{
	// ##################Leader同步日志逻辑##############################
		args := rf.genAppendEntriesArgs(prevLogIndex)
		rf.mu.RUnlock()
		reply := new(AppendEntriesReply)
		// 发送心跳同步日志
		if (rf.sendAppendEntries(peer, args, reply)) {
			rf.mu.Lock()
			if args.Term == rf.currentTerm && rf.state == Leader {
				if !reply.Success {
					// Leader节点发送给Raft节点的reply发现raft落后了
					if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term , -1
						rf.persist()
					// 确保当前是Leader任期内
					}else if reply.Term == rf.currentTerm {
						// 直接让下一个需要同步的任期为冲突index
						rf.nextIndex[peer] = reply.ConflictIndex
						//就是说出现了其他任期的冲突日志
						if reply.ConflictTerm != -1 {
							firstLogIndex := rf.getFirstLog().Index
							// 倒退Leader的log找到第一条相同的日志
							for index := args.PrevLogIndex - 1; index > firstLogIndex ; index-- {
								if rf.logs[index-firstLogIndex].Term == reply.ConflictIndex {
									// TODO:用二分查找？？这里怎么个查找法？？万一出现001001111这种呢
									rf.nextIndex[peer] = index
									break
								}
							}
						}
					}
				}else {
					// 如果发送的心跳reply成功，直接就复制相应的日志，然后修改这两个矩阵就ok
					rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[peer] = rf.matchIndex[peer] + 1
					rf.advanceCommitIndexForLeader()
				}
			}
			rf.mu.Unlock()
			DPrintf("{Node %v} sends AppendEntriesArgs %v to {Node %v} and receives AppendEntriesReply %v", rf.me, args, peer, reply)
		}
	}		
}

func(rf *Raft) needReplicating(peer int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	// 检查leader节点的同步日志是否落后于最后一条日志
	return rf.state == Leader && rf.matchIndex[peer] < rf.getLastLog().Index
}

func (rf *Raft) replicator(peer int) {
	// 上分布式琐防止并发出问题
	rf.replicatorCond[peer].L.Lock()
	defer rf.replicatorCond[peer].L.Unlock()
	for rf.killed() == false {
		if !rf.needReplicating(peer) {
			// 如果没有新的需要复制就等待
			rf.replicatorCond[peer].Wait()
		}
		rf.replicateOnceRound(peer)
	}

}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		// reset会重新向管道里面写入东西吗
		case <- rf.electionTimer.C:
			rf.mu.Lock()
			rf.ChangeState(Candidate)
			rf.currentTerm += 1
			rf.persist()
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}	
	}
}

func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex < rf.lastApplied {
			// 提交的index小于最后同步的index
			rf.applyCond.Wait()
		}
		firstLogIndex, commitIndex, lastApplied := rf.getFirstLog().Index, rf.commitIndex, rf.lastApplied
		entries := make([]LogEntry, commitIndex-lastApplied)
		copy(entries, rf.logs[lastApplied-firstLogIndex+1:commitIndex-firstLogIndex+1])
		rf.mu.Unlock()
		// 这个节点是什么，raft还是只有leader节点

		for _,entry := range entries {
			rf.applyCh <- ApplyMsg{
				CommandValid: 	true,
				Command:		entry.Command,
				CommandIndex: 	entry.Index,
			}
		}
		rf.mu.Lock()
		DPrintf("{Node %v} applies log entries from index %v to %v in term %v", rf.me, lastApplied+1, commitIndex, rf.currentTerm)
		// use commitIndex rather than rf.commitIndex because rf.commitIndex may change during the Unlock() and Lock()
		//todo why max
		rf.lastApplied = Max(commitIndex,rf.lastApplied)
		rf.mu.Unlock()
	}	
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu: 			sync.RWMutex{},	
		peers: 			peers,
		persister:  	persister,
		me:				me,
		dead: 			0,		
		currentTerm:	0,
		votedFor:		-1,
		logs:			make([]LogEntry, 1),
		commitIndex:	0,
		lastApplied:	0,
		nextIndex: 		make([]int, len(peers)),
		matchIndex:		make([]int, len(peers)),
		state: 			Follower,
		electionTimer: 	time.NewTimer(RandomElectionTimeout()),
		heartbeatTimer: time.NewTimer(StableHeartbeatTimeout()),
		applyCh: 		applyCh,
		// 用于每个goroutine之间的同步
		// 每个在满足特定条件时才进行通信/等待
		replicatorCond: make([]*sync.Cond, len(peers)),			
	}

	// Your initialization code here (3A, 3B, 3C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// 创建互斥锁
	rf.applyCond = sync.NewCond(&rf.mu)
	for peer := range peers {
		rf.matchIndex[peer], rf.nextIndex[peer] = 0, rf.getLastLog().Index+1
		if peer != rf.me {
			rf.replicatorCond[peer] = sync.NewCond(&sync.Mutex{})
			// 启动复制的goroutine
			go rf.replicator(peer)
		}
	}
	go rf.ticker()
	go rf.applier()
	return rf
}
