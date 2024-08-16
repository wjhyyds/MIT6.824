package raft

import (
	"math/rand"
	"time"
)

type RequestVoteArgs struct {
	From         int
	To           int
	Term         int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	From    int
	To      int
	Term    int
	Granted bool
}

const baseElectionInterval = 300

func (rf *Raft) electionTimeout() bool {
	return time.Since(rf.lastElection) > rf.electionInterval
}

func (rf *Raft) resetElectionTimer() {
	rf.electionInterval = time.Duration(baseElectionInterval+rand.Int63n(baseElectionInterval)) * time.Millisecond
	rf.lastElection = time.Now()
}

func (rf *Raft) newRequestVoteArgs(to int) *RequestVoteArgs {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)

	return &RequestVoteArgs{
		From:         rf.me,
		To:           to,
		Term:         rf.term,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
}

func (rf *Raft) sendRequestVote(args *RequestVoteArgs) {
	var reply RequestVoteReply
	if ok := rf.peers[args.To].Call("Raft.RequestVote", args, &reply); ok {
		rf.handleRequestVoteReply(args, &reply)
	}
}

func (rf *Raft) broadcastRequestVote() {
	rf.logger.bcastRV()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		args := rf.newRequestVoteArgs(i)
		go rf.sendRequestVote(args)
	}
}

func (rf *Raft) canVote(candidate, candidateLastLogIndex, candidateLastLogTerm int) bool {
	lastLogIndex := rf.log.lastIndex()
	lastLogTerm, _ := rf.log.term(lastLogIndex)
	return (rf.votedFor == None || rf.votedFor == candidate) &&
		(candidateLastLogTerm > lastLogTerm || (candidateLastLogTerm == lastLogTerm && candidateLastLogIndex >= lastLogIndex))
}

func (rf *Raft) collectVote() bool {
	count := 1
	for i, vote := range rf.voteMe {
		if i != rf.me && vote {
			count++
		}
	}
	return 2*count > len(rf.peers)
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvRV(args)

	reply.From = rf.me
	reply.Term = rf.term
	reply.To = args.From
	reply.Granted = false

	m := Message{
		Type: Vote,
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

	if rf.canVote(args.From, args.LastLogIndex, args.LastLogTerm) {
		rf.logger.voteTo(args)

		rf.votedFor = args.From
		rf.resetElectionTimer()
		reply.Granted = true
	} else {
		idx := rf.log.lastIndex()
		term, _ := rf.log.term(idx)
		rf.logger.rejectVote(args, idx, term)
	}
}

func (rf *Raft) handleRequestVoteReply(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvRVR(reply)

	m := Message{
		Type:     VoteReply,
		From:     reply.From,
		Term:     reply.Term,
		ArgsTerm: args.Term,
	}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	if reply.Granted {
		rf.voteMe[reply.From] = true
		if rf.collectVote() {
			rf.becomeLeader()
		}
	}
}
