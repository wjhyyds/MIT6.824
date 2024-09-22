package shardctrler

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

var ExecuteTimeout = 500 * time.Millisecond

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.

	stateMachine *MemoryConfigStateMachine
	lastReply    map[int64]Reply    // cid/reply
	notify       map[int]chan Reply // log index / chan op

	dead int32
}

type Op struct {
	// Your data here.
	OpType OpType
	Cid    int64
	Seq    int64

	// for Join,new GID -> servers mappings
	Servers map[int][]string

	// for Leave
	GIDs []int

	// for Move
	Shard int
	GID   int

	// for Query,desired config number
	Num int
}

func (sc *ShardCtrler) Kill() {
	// Your code here, if desired.
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
}

func (sc *ShardCtrler) Killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.stateMachine = newMemoryConfigStateMachine()

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastReply = map[int64]Reply{}
	sc.notify = map[int]chan Reply{}

	go sc.applier()

	return sc
}

func (sc *ShardCtrler) Do(args *Request, reply *Reply) {
	if args.Op != OpQuery && sc.isDuplicate(args.Cid, args.Seq) {
		last := sc.lastReply[args.Cid]
		reply.Seq, reply.Config, reply.Err = last.Seq, last.Config, last.Err
		return
	}

	op := Op{
		OpType: args.Op,
		Cid:    args.Cid,
		Seq:    args.Seq,

		Servers: args.Servers,
		GIDs:    args.GIDs,
		Shard:   args.Shard,
		GID:     args.GID,
		Num:     args.Num,
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := sc.getNotifyCh(index)

	select {
	case res := <-ch:
		reply.Seq, reply.Config, reply.Err = res.Seq, res.Config, res.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go sc.removeOutdatedCh(index)
}

func (sc *ShardCtrler) applier() {
	for !sc.Killed() {
		msg := <-sc.applyCh
		if msg.CommandValid {
			var reply Reply
			index, op := msg.CommandIndex, msg.Command.(Op)
			reply.Seq = op.Seq

			if op.OpType != OpQuery && sc.isDuplicate(op.Cid, op.Seq) {
				last := sc.lastReply[op.Cid]
				reply.Config, reply.Err = last.Config, last.Err
			} else {
				sc.mu.Lock()
				var (
					config Config
					err    Err
				)
				switch op.OpType {
				case OpJoin:
					err = sc.stateMachine.Join(op.Servers)
				case OpLeave:
					err = sc.stateMachine.Leave(op.GIDs)
				case OpMove:
					err = sc.stateMachine.Move(op.Shard, op.GID)
				case OpQuery:
					config, err = sc.stateMachine.Query(op.Num)
				default:
					panic("Unexpected op type")
				}
				reply.Config, reply.Err = config, err
				sc.mu.Unlock()
			}

			if term, isLeader := sc.rf.GetState(); isLeader && term == msg.CommandTerm {
				sc.getNotifyCh(index) <- reply
			}
		}
	}
}

func (sc *ShardCtrler) isDuplicate(cid int64, seq int64) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	lastRep, ok := sc.lastReply[cid]
	return ok && seq <= lastRep.Seq
}

func (sc *ShardCtrler) getNotifyCh(index int) chan Reply {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if _, exist := sc.notify[index]; !exist {
		sc.notify[index] = make(chan Reply)
	}
	return sc.notify[index]
}

func (sc *ShardCtrler) removeOutdatedCh(index int) {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if ch, exist := sc.notify[index]; exist {
		close(ch)
		delete(sc.notify, index)
	}
}
