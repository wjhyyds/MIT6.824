package raft

type Tracker struct {
	next  int
	match int
}

func (rf *Raft) resetTrackers() {
	for i := range rf.trackers {
		rf.trackers[i].next = rf.log.lastIndex() + 1
		rf.trackers[i].match = 0
	}
}
