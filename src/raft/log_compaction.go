package raft

type InstallSnapshotArgs struct {
	From     int
	To       int
	Term     int
	Snapshot Snapshot
}
type InstallSnapshotReply struct {
	From int
	To   int
	Term int
}

func (rf *Raft) needSnapshot(to int) bool {
	// log is 1_index,firstLog is dummy
	return rf.trackers[to].next <= rf.log.firstIndex()
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.pullSnap(index)

	if index <= rf.log.snapshot.Index || index > rf.log.committed {
		return
	}

	if snapshotTerm, err := rf.log.term(index); err == nil {
		rf.log.compactedTo(Snapshot{Data: snapshot, Index: index, Term: snapshotTerm})
		rf.persist()
	}
}

func (rf *Raft) newInstallSnapshotArgs(to int) *InstallSnapshotArgs {
	return &InstallSnapshotArgs{
		From:     rf.me,
		To:       to,
		Term:     rf.term,
		Snapshot: rf.log.snapshot,
	}
}

func (rf *Raft) sendInstallSnapshot(args *InstallSnapshotArgs) {

	rf.logger.sendIS(args)

	var reply InstallSnapshotReply
	if ok := rf.peers[args.To].Call("Raft.InstallSnapshot", args, &reply); ok {
		rf.handleInstallSnapshotReply(args, &reply)
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvIS(args)

	reply.From = rf.me
	reply.To = args.From
	reply.Term = rf.term

	m := Message{Type: Snap, From: args.From, Term: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		reply.Term = rf.term
		defer rf.persist()
	}
	if !ok {
		return
	}

	// 该peer能够跟上leader,不需要snapshot
	if args.Snapshot.Index <= rf.log.committed {
		return
	}

	rf.log.compactedTo(args.Snapshot)
	if !termChanged {
		defer rf.persist()
	}

	rf.log.hasPendingSnapshot = true
	rf.log.toBeApplied.Signal()
}

func (rf *Raft) handleInstallSnapshotReply(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.logger.recvISR(reply)

	m := Message{Type: SnapReply, From: reply.From, Term: reply.Term, ArgsTerm: args.Term}
	ok, termChanged := rf.checkMessage(m)
	if termChanged {
		defer rf.persist()
	}
	if !ok {
		return
	}

	oldTracker := rf.trackers[reply.From]

	rf.trackers[reply.From].match = max(args.Snapshot.Index, rf.trackers[reply.From].match)
	rf.trackers[reply.From].next = rf.trackers[reply.From].match + 1

	newTracker := rf.trackers[reply.From]

	if oldTracker != newTracker {
		rf.logger.updateTrackers(reply.From, oldTracker, newTracker)
	}

	rf.broadcastAppendEntries(true)
}
