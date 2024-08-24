package raft

import (
	//	"bytes"

	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	None             = -1
	heatbeatInterval = 150
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's role
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted role
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// role a Raft server must maintain.
	role     Role
	term     int
	votedFor int
	voteMe   []bool

	lastElection     time.Time
	electionInterval time.Duration

	lastHeartBeat    time.Time
	heatbeatInterval time.Duration

	log Log

	applyCh chan ApplyMsg

	trackers []Tracker

	logger Logger
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.term, !rf.killed() && rf.role == Leader
}

// save Raft's persistent role to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.term) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log.entries) != nil {
		panic("Encode failed")
	}
	rf.persister.Save(w.Bytes(), nil)
}

// restore previously persisted role.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any role?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.term) != nil || d.Decode(&rf.votedFor) != nil || d.Decode(&rf.log.entries) != nil {
		panic("Decode failed")
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
}

func (rf *Raft) Start(command interface{}) (index int, term int, isLeader bool) {
	// Your code here (3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = !rf.killed() && rf.role == Leader
	if !isLeader {
		return 0, 0, false
	}

	index = rf.log.lastIndex() + 1
	term = rf.term
	rf.log.entries = append(rf.log.entries, Entry{Index: index, Term: term, Cmd: command})
	rf.persist()

	rf.broadcastAppendEntries(true)

	return
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		rf.mu.Lock()

		switch rf.role {
		case Follower, Candidate:
			if rf.electionTimeout() {
				rf.becomeCandidate()
				rf.broadcastRequestVote()
			}
		case Leader:
			force := false
			if rf.heatbeatTimeout() {
				force = true
				rf.resetHeatbeatTimer()
			}
			rf.broadcastAppendEntries(force)
		}

		rf.mu.Unlock()
		interval := time.Duration(50 + rand.Int63n(25))
		time.Sleep(interval * time.Millisecond)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.applyCh = applyCh

	rf.logger = *makeLogger(false, "out")
	rf.logger.r = rf

	rf.log = NewLog()
	rf.log.logger = &rf.logger
	rf.log.toBeApplied = *sync.NewCond(&rf.mu)

	rf.term = 0
	rf.votedFor = None
	rf.voteMe = make([]bool, len(rf.peers))

	rf.trackers = make([]Tracker, len(rf.peers))
	rf.resetTrackers()

	rf.role = Follower
	rf.resetElectionTimer()
	rf.heatbeatInterval = heatbeatInterval * time.Millisecond

	// initialize from role persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.committer()

	return rf
}
