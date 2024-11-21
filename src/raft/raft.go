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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

	// other properties
	state          NodeState     // current state of the server
	electionTimer  *time.Timer   // timer for election timeout
	heartbeatTimer *time.Timer   // timer for heartbeat
	applyCh        chan ApplyMsg // channel to send apply message to service
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
func (rf *Raft) persist() {
	
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).


	return index, term, isLeader
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
	args := rf.genRequestVoteArgs()
	grantedVotes := 1
	DPrintf("{Node %v} starts election with RequestVoteArgs %v", rf.me, args)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
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
							rf.BroadcastHeartbeat()
						}
					//受到的任期已经大于当前的任期，表明有leader当选了
					}else if reply.Term > rf.currentTerm {
						rf.ChangeState(Follower)
						rf.currentTerm, rf.votedFor = reply.Term , -1
					}
				}
			}
		}(peer)
	}
}

func (rf *Raft) BroadcastHeartbeat() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func (peer int) {
			rf.mu.RLock()
			if rf.state != Leader {
				rf.mu.RUnlock()
				return
			}
			args := rf.genAppendEntriesArgs()
			rf.mu.RUnlock()
			reply := new(AppendEntriesReply)
			// 发送心跳同步日志
			if (rf.sendAppendEntries(peer, args, reply)) {
				rf.mu.Lock()
				if args.Term == rf.currentTerm && rf.state == Leader {
					if !reply.Success {
						if reply.Term > rf.currentTerm {
							rf.ChangeState(Follower)
							rf.currentTerm, rf.votedFor = reply.Term , -1
						}
					}
				}
				rf.mu.Unlock()
			}
		}(peer)
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
			rf.StartElection()
			rf.electionTimer.Reset(RandomElectionTimeout())
			rf.mu.Unlock()
		case <- rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				rf.BroadcastHeartbeat()
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}	
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
	}

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
