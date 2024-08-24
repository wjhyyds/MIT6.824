package raft

type MessageType string

const (
	Vote        MessageType = "RequestVote"
	VoteReply   MessageType = "RequestVoteReply"
	Append      MessageType = "AppendEntries"
	AppendReply MessageType = "AppendEntriesReply"
	Snap        MessageType = "InstallSnapshot"
	SnapReply   MessageType = "InstallSnapshotReply"
)

type Message struct {
	Type         MessageType
	From         int // warning: not used for now.
	Term         int // the term in the PRC args or RPC reply.
	ArgsTerm     int // the term in the RPC args. Used to different between the term in a RPC reply.
	PrevLogIndex int // used for checking AppendEntriesReply.
}

// return (termIsStale, termChanged).
func (rf *Raft) checkTerm(m Message) (valid, termChanged bool) {
	if m.Term < rf.term {
		return false, false
	}

	valid = true
	if m.Term > rf.term || m.Type == Append {
		termChanged = rf.becomeFollower(m.Term)
	}
	return
}

// return true if the raft peer is eligible to handle the message.
func (rf *Raft) checkState(m Message) (eligible bool) {
	switch m.Type {
	case Vote, Append:
		eligible = rf.role == Follower // 在checkTerm中，不符号的Candidate和Leader都会成为Follwer

	case VoteReply:
		eligible = rf.role == Candidate && rf.term == m.ArgsTerm

	case AppendReply:
		eligible = rf.role == Leader && rf.term == m.ArgsTerm && rf.trackers[m.From].next-1 == m.PrevLogIndex
	}
	return
}

func (rf *Raft) checkMessage(m Message) (eligible, termChanged bool) {
	ok, termChanged := rf.checkTerm(m)
	if !ok || !rf.checkState(m) {
		return false, termChanged
	}
	return true, termChanged
}
