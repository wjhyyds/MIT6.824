package raft

import "time"

type AppendEntriesArgs struct {
	From         int
	To           int
	Term         int
	Committed    int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
}

type Status int

const (
	Reject Status = iota
	Match
	EntryNotFound
	TermConflict
)

type AppendEntriesReply struct {
	From int
	To   int
	Term int

	Xterm  int
	Xindex int
	Xlen   int

	Status Status
}

func (rf *Raft) heatbeatTimeout() bool {
	return time.Since(rf.lastHeartBeat) > rf.heatbeatInterval
}

func (rf *Raft) resetHeatbeatTimer() {
	rf.lastHeartBeat = time.Now()
}

func (rf *Raft) newAppendEntriesArgs(to int) *AppendEntriesArgs {
	next := rf.trackers[to].next
	entries := rf.log.clone(next, rf.log.lastIndex()+1)

	prevLogIndex := next - 1
	prevLogTerm, _ := rf.log.term(prevLogIndex)

	return &AppendEntriesArgs{
		From:         rf.me,
		To:           to,
		Term:         rf.term,
		Committed:    rf.log.committed,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
	}
}

func (rf *Raft) hasNewEntry(to int) bool {
	return rf.trackers[to].next <= rf.log.lastIndex()
}

func (rf *Raft) checkPrefix(leaderPrevLogIndex, leaderPrevLogTerm int) Status {
	prevLogTerm, err := rf.log.term(leaderPrevLogIndex)
	if err != nil {
		return EntryNotFound
	}
	if prevLogTerm != leaderPrevLogTerm {
		return TermConflict
	}
	return Match
}

func (rf *Raft) findFirstConflict(leaderPrevLogIndex int) (conflictTerm, index int) {
	conflictTerm, _ = rf.log.term(leaderPrevLogIndex)
	index = leaderPrevLogIndex
	for i := leaderPrevLogIndex - 1; i > rf.log.firstIndex(); i-- {
		if term, _ := rf.log.term(i); term != conflictTerm {
			break
		}
		index = i
	}
	return
}

func (rf *Raft) quorumMatch(index int) bool {
	match := 1
	for _, tracker := range rf.trackers {
		if tracker.match >= index {
			match++
		}
	}
	return 2*match > len(rf.peers)
}

func (rf *Raft) sendAppendEntries(args *AppendEntriesArgs) {
	rf.logger.sendAR(args)

	var reply AppendEntriesReply
	if ok := rf.peers[args.To].Call("Raft.AppendEntries", args, &reply); ok {
		rf.handleAppendEntriesReply(args, &reply)
	}
}

func (rf *Raft) broadcastAppendEntries(force bool) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		if force || rf.hasNewEntry(i) {
			args := rf.newAppendEntriesArgs(i)
			go rf.sendAppendEntries(args)
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvAR(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term
	reply.Status = Reject

	m := Message{
		Type: Append,
		From: args.From,
		Term: args.Term,
	}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	if !ok {
		return
	}

	reply.Status = rf.checkPrefix(args.PrevLogIndex, args.PrevLogTerm)

	if reply.Status != Match {
		if reply.Status == EntryNotFound {
			reply.Xlen = rf.log.lastIndex()
		} else if reply.Status == TermConflict {
			reply.Xterm, reply.Xindex = rf.findFirstConflict(args.PrevLogIndex)
		}
		rf.logger.rejectEnts(args)
		return
	}

	rf.logger.acceptEnts(args)

	for i, e := range args.Entries {
		if term, err := rf.log.term(e.Index); err != nil || term != e.Term {
			rf.log.truncateAfter(e.Index)
			rf.log.entries = append(rf.log.entries, args.Entries[i:]...)
			if !termChanged {
				defer rf.persist()
			}
			break
		}
	}

	newCommit := min(args.Committed, args.PrevLogIndex+len(args.Entries))
	rf.log.committedTo(newCommit)
}

func (rf *Raft) handleAppendEntriesReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvARR(reply)

	m := Message{
		Type:         AppendReply,
		From:         reply.From,
		Term:         reply.Term,
		ArgsTerm:     args.Term,
		PrevLogIndex: args.PrevLogIndex,
	}

	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	oldTracker := rf.trackers[reply.From]

	switch reply.Status {
	case Reject:
		// 表示reply.term>rf.term,在checkMessage中已经变为Follower
	case Match:
		rf.trackers[reply.From].match = args.PrevLogIndex + len(args.Entries)
		rf.trackers[reply.From].next = rf.trackers[reply.From].match + 1

		if oldTracker != rf.trackers[reply.From] {
			rf.logger.updateTrackers(reply.From, oldTracker, rf.trackers[reply.From])
		}

		// 更新commit
		newMatch := rf.trackers[reply.From].match
		for N := newMatch; N > rf.log.committed; N-- {
			if term, _ := rf.log.term(N); term == rf.term && rf.quorumMatch(N) {
				rf.log.committedTo(N)
				rf.broadcastAppendEntries(true)
				break
			}
		}

	case EntryNotFound:
		rf.trackers[reply.From].next = min(reply.Xlen, rf.log.lastIndex()) + 1
		rf.broadcastAppendEntries(true)

		if oldTracker != rf.trackers[reply.From] {
			rf.logger.updateTrackers(reply.From, oldTracker, rf.trackers[reply.From])
		}

	case TermConflict:
		newNext := reply.Xindex
		for i := rf.log.lastIndex(); i > rf.log.firstIndex(); i-- {
			if term, _ := rf.log.term(i); term == reply.Xterm {
				newNext = i
				break
			}
		}

		rf.trackers[reply.From].next = newNext
		rf.broadcastAppendEntries(true)

		if oldTracker != rf.trackers[reply.From] {
			rf.logger.updateTrackers(reply.From, oldTracker, rf.trackers[reply.From])
		}
	}
}
