package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

func (rf *Raft) committer() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.log.hasPendingSnapshot {
			snap := rf.log.snapshot
			rf.mu.Unlock()

			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      snap.Data,
				SnapshotIndex: snap.Index,
				SnapshotTerm:  snap.Term,
			}

			rf.mu.Lock()
			rf.log.hasPendingSnapshot = false
			rf.logger.pushSnap(snap.Index, snap.Term)
		} else if newCommitEntries := rf.log.newCommitEntries(); newCommitEntries != nil {
			rf.mu.Unlock()
			for _, e := range newCommitEntries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      e.Cmd,
					CommandIndex: e.Index,
					CommandTerm:  e.Term,
				}
			}
			rf.mu.Lock()
			rf.log.appliedTo(newCommitEntries[len(newCommitEntries)-1].Index)
		} else {
			rf.log.toBeApplied.Wait()
		}
		rf.mu.Unlock()
	}
}
