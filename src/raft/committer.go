package raft

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotIndex int
	SnapshotTerm  int
}

func (rf *Raft) committer() {
	for !rf.killed() {
		rf.mu.Lock()

		if newCommitEntries := rf.log.newCommitEntries(); newCommitEntries != nil {
			rf.mu.Unlock()
			for _, e := range newCommitEntries {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      e.Cmd,
					CommandIndex: e.Index,
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
