package raft

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"
)

// true to turn on debugging/logging.
const (
	debug     = true
	LOGTOFILE = false
	printEnts = false
)

func (l *Logger) printEnts(topic logTopic, me int, ents []Entry) {
	if printEnts {
		for _, ent := range ents {
			if ent.Index != 0 {
				l.printf(topic, "N%v    (I:%v T:%v D:%v)", me, ent.Index, ent.Term, ent.Cmd.(int))
				// l.printf(topic, "N%v    (I:%v T:%v)", me, ent.Index, ent.Term)
			}
		}
	}
}

// what topic the log message is related to.
// logs are organized by topics which further consists of events.
type logTopic string

const (
	ELEC logTopic = "ELEC"
	LRPE logTopic = "LRPE"
	BEAT logTopic = "BEAT"
	PERS logTopic = "PERS"
	PEER logTopic = "PEER"
	SNAP logTopic = "SNAP"
	INFO logTopic = "INFO"
	WARN logTopic = "WARN"
	ERRO logTopic = "ERRO"
	TRCE logTopic = "TRCE"
)

type Logger struct {
	logToFile      bool
	logFile        *os.File
	verbosityLevel int // logging verbosity is controlled over environment verbosity variable.
	startTime      time.Time
	r              *Raft
}

func makeLogger(logToFile bool, logFileName string) *Logger {
	logger := &Logger{}
	logger.init(logToFile, logFileName)
	return logger
}

func (logger *Logger) init(logToFile bool, logFileName string) {
	logger.logToFile = logToFile
	logger.verbosityLevel = getVerbosityLevel()
	logger.startTime = time.Now()

	// set log config.
	if logger.logToFile {
		logger.setLogFile(logFileName)
	}
	log.SetFlags(log.Flags() & ^(log.Ldate | log.Ltime)) // not show date and time.
}

func (logger *Logger) setLogFile(filename string) {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		log.Fatalf("failed to create file %v", filename)
	}
	log.SetOutput(f)
	logger.logFile = f
}

func (logger *Logger) printf(topic logTopic, format string, a ...interface{}) {
	// print iff debug is set.
	if debug {
		time := time.Since(logger.startTime).Milliseconds()
		prefix := fmt.Sprintf("%010d %v ", time, string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

// retrieve the verbosity level from an environment variable
// VERBOSE=0/1/2/3 <=>
func getVerbosityLevel() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

// general

func (l *Logger) becomeLeader() {
	r := l.r
	l.printf(INFO, "S%d become leader(T:%d)", r.me, r.term)
}

func (l *Logger) becomeCandidate() {
	r := l.r
	l.printf(INFO, "S%d become Candidate(T:%d)", r.me, r.term)
}

func (l *Logger) becomeFollower(oldTerm int) {
	r := l.r
	l.printf(INFO, "S%d become Follower(T:%d -> T:%d)", r.me, oldTerm, r.term)
}

//
// leader election events.
//

func (l *Logger) bcastRV() {
	r := l.r
	l.printf(ELEC, "N%v @ RVOT (T:%v)", r.me, r.term)
}

func (l *Logger) recvRV(args *RequestVoteArgs) {
	l.printf(ELEC, "S%d <- S%d RV From:[T=%d],To:[T=%d]", l.r.me, args.From, args.Term, l.r.term)
}

func (l *Logger) recvRVR(reply *RequestVoteReply) {
	l.printf(ELEC, "S%d <- S%d RVR reply:[T=%d V=%v]", l.r.me, reply.From, reply.Term, reply.Granted)
}

func (l *Logger) voteTo(args *RequestVoteArgs) {
	l.printf(ELEC, "S%d v-> S%d", l.r.me, args.From)
}

func (l *Logger) rejectVote(args *RequestVoteArgs, idx int, term int) {
	l.printf(ELEC, "S%d !v-> S%d (CID:%d CT:%d ID:%d T%d)", l.r.me, args.From, args.LastLogIndex, args.LastLogTerm, idx, term)
}

//
// log replication events.
//

var stMap = [...]string{
	"RJ",  // rejected.
	"MT",  // matched.
	"ENF", // entry not found
	"TC",  // term conflict
}

func (l *Logger) appendEnts(ents []Entry) {
	r := l.r
	l.printf(LRPE, "S%v +e (LN:%v)", r.me, len(ents))
}

func (l *Logger) sendAR(args *AppendEntriesArgs) {
	r := l.r
	l.printf(LRPE, "S%v e-> S%v (T:%v CI:%v PI:%v PT:%v LN:%v)", r.me, args.To, r.term, r.log.committed, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
	l.printEnts(LRPE, r.me, args.Entries)
}

func (l *Logger) recvAR(args *AppendEntriesArgs) {
	r := l.r
	l.printf(LRPE, "S%v <- S%v AR (T:%v CI:%v PI:%v PT:%v LN:%v)", r.me, args.From, args.Term, args.Committed, args.PrevLogIndex, args.PrevLogTerm, len(args.Entries))
}

func (l *Logger) rejectEnts(args *AppendEntriesArgs) {
	r := l.r
	l.printf(LRPE, "N%v !e<- N%v", r.me, args.From)
}

func (l *Logger) acceptEnts(args *AppendEntriesArgs) {
	r := l.r
	l.printf(LRPE, "N%v &e<- N%v", r.me, args.From)
}

func (l *Logger) recvARR(reply *AppendEntriesReply) {
	r := l.r
	l.printf(LRPE, "S%v <- S%v ARR (T:%v St:%v CT:%v FCI:%v LI:%v)", r.me, reply.From, reply.Term, stMap[reply.Status], reply.Xterm, reply.Xindex, reply.Xlen)
}

func (l *Logger) updateTrackers(peer int, oldTracker, newTracker Tracker) {
	r := l.r
	l.printf(LRPE, "N%v ^pr N%v (NI:%v MI:%v) -> (NI:%v MI:%v)", r.me, peer, oldTracker.next, oldTracker.match, newTracker.next, newTracker.match)
}

func (l *Logger) updateCommitted(oldCommitted int) {
	r := l.r
	l.printf(LRPE, "N%v ^ci (CI:%v) -> (CI:%v)", r.me, oldCommitted, r.log.committed)
}

func (l *Logger) updateApplied(oldApplied int) {
	r := l.r
	l.printf(LRPE, "N%v ^ai (AI:%v) -> (AI:%v)", r.me, oldApplied, r.log.applied)
}
