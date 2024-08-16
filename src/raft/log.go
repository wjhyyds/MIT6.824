package raft

import "errors"

var OutOfBound = errors.New("OutOfBound")

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
}

func NewLog() Log {
	return Log{
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
		return 0, OutOfBound
	}
	return l.entries[l.toLogIndex(index)].Term, nil
}

func (l *Log) toLogIndex(index int) int {
	return index - l.firstIndex()
}

func (l *Log) committedTo(index int) {
	l.committed = max(l.committed, index)
}

func (l *Log) appliedTo(index int) {
	l.applied = max(l.applied, index)
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
