package raft

type Role int

const (
	Leader Role = iota
	Candidate
	Follower
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "L"
	case Candidate:
		return "C"
	case Follower:
		return "F"
	default:
		panic("Undifined role")
	}
}

func (rf *Raft) becomeFollower(term int) (termChanged bool) {
	oldTerm := rf.term
	rf.role = Follower
	rf.resetElectionTimer()

	if term > rf.term {
		rf.term = term
		rf.votedFor = None
		termChanged = true
	}

	rf.logger.becomeFollower(oldTerm)
	return termChanged
}

func (rf *Raft) becomeCandidate() {
	defer rf.persist()

	rf.term++
	rf.voteMe = make([]bool, len(rf.peers))
	rf.votedFor = rf.me
	rf.role = Candidate

	rf.logger.becomeCandidate()
	rf.resetElectionTimer()
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	rf.resetTrackers()
	rf.broadcastAppendEntries(true)

	rf.logger.becomeLeader()
}
