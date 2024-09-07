package raft

import (
	"errors"
	"sync"
)

var ErrOutOfBound = errors.New("OutOfBound")

type Entry struct {
	Index int
	Term  int
	Cmd   interface{}
}

type Snapshot struct {
	Index int // lastIncludedIndex
	Term  int
	Data  []byte
}

type Log struct {
	entries []Entry

	applied   int
	committed int

	logger      *Logger
	toBeApplied sync.Cond

	snapshot           Snapshot
	hasPendingSnapshot bool
}

func NewLog() Log {
	return Log{
		snapshot:  Snapshot{Data: nil, Index: 0, Term: 0},
		entries:   []Entry{{Index: 0, Term: 0}}, // dummy entry
		applied:   0,
		committed: 0,
	}
}

func (l *Log) firstIndex() int {
	return l.entries[0].Index
}

func (l *Log) lastIndex() int {
	return l.entries[len(l.entries)-1].Index
}

func (l *Log) term(index int) (int, error) {
	if index < l.firstIndex() || index > l.lastIndex() {
		return 0, ErrOutOfBound
	}
	return l.entries[l.toLogIndex(index)].Term, nil
}

func (l *Log) toLogIndex(index int) int {
	return index - l.firstIndex()
}

func (l *Log) committedTo(index int) {
	if l.committed < index {
		old := l.committed
		l.committed = index
		l.toBeApplied.Signal()
		l.logger.updateCommitted(old)
	}
}

func (l *Log) appliedTo(index int) {
	if l.applied < index {
		old := l.applied
		l.applied = index
		l.logger.updateApplied(old)
	}
}

func (l *Log) compactedTo(snapshot Snapshot) {
	var suffix []Entry
	dummy := Entry{Index: snapshot.Index, Term: snapshot.Term}
	start := snapshot.Index + 1
	if start <= l.lastIndex() {
		start = l.toLogIndex(start)
		suffix = l.entries[start:]
	}

	l.entries = append(make([]Entry, 1), suffix...)
	l.snapshot = snapshot

	l.entries[0] = dummy

	l.committedTo(l.snapshot.Index)
	l.appliedTo(l.snapshot.Index)

	lastLogIndex := l.lastIndex()
	lastLogTerm, _ := l.term(lastLogIndex)
	l.logger.compactedTo(lastLogIndex, lastLogTerm)
}

// log = log[:index]
func (l *Log) truncateAfter(index int) {
	if index <= l.firstIndex() || index > l.lastIndex() {
		return
	}
	index = l.toLogIndex(index)
	l.entries = l.entries[:index]
}

func (l *Log) newCommitEntries() []Entry {
	start, end := l.applied+1, l.committed+1
	if start >= end {
		return nil
	}
	return l.clone(start, end)
}

// clone [start,end)
func (l *Log) clone(start, end int) []Entry {
	start, end = l.toLogIndex(start), l.toLogIndex(end)
	if start >= end || start < 0 || end > len(l.entries) {
		return nil
	}

	cloned := make([]Entry, end-start)
	copy(cloned, l.entries[start:end])
	return cloned
}
