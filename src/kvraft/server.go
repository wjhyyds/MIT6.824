package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

var ExecuteTimeout = 500 * time.Millisecond

type Op struct {
	Type string // Put/Append/Get
	Sid  uint64
	Cid  int64

	Key string
	Val string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db        *MemoryDb          // 保存状态
	notify    map[int]chan Reply // log index / chan op
	lastReply map[int64]Reply    // cid/reply
}

func (kv *KVServer) Do(args *Request, reply *Reply) {

	DPrintf("S%d <- C%d,args=%+v", kv.me, args.Cid, args)

	if args.Op != "Get" && kv.isDuplicate(args.Cid, args.Sid) {
		last := kv.lastReply[args.Cid]
		reply.Value, reply.Err = last.Value, last.Err
		return
	}

	op := Op{
		Type: args.Op,
		Cid:  args.Cid,
		Sid:  args.Sid,

		Key: args.Key,
		Val: args.Value,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyCh(index)

	select {
	case res := <-ch:
		reply.Value, reply.Err = res.Value, res.Err
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

	go kv.removeOutdatedCh(index)
}

func (kv *KVServer) applier() {
	for !kv.killed() {
		msg := <-kv.applyCh
		if msg.CommandValid {
			var reply Reply
			index, op := msg.CommandIndex, msg.Command.(Op)
			reply.Sid = op.Sid

			if op.Type != "Get" && kv.isDuplicate(op.Cid, op.Sid) {
				reply = kv.lastReply[op.Cid]
			} else {
				kv.mu.Lock()
				switch op.Type {
				case "Get":
					reply.Value, reply.Err = kv.db.Get(op.Key)
				case "Put":
					reply.Value, reply.Err = kv.db.Put(op.Key, op.Val)
					kv.lastReply[op.Cid] = reply
				case "Append":
					reply.Value, reply.Err = kv.db.Append(op.Key, op.Val)
					kv.lastReply[op.Cid] = reply
				default:
					panic("Unexpected op type!")
				}
				kv.mu.Unlock()
			}

			if term, isLeader := kv.rf.GetState(); isLeader && term == msg.CommandTerm {
				kv.getNotifyCh(index) <- reply
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = NewMemoryDb()
	kv.notify = make(map[int]chan Reply)
	kv.lastReply = make(map[int64]Reply)

	go kv.applier()

	return kv
}

func (kv *KVServer) removeOutdatedCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if ch, exist := kv.notify[index]; exist {
		close(ch)
		delete(kv.notify, index)
	}
}

func (kv *KVServer) getNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, exist := kv.notify[index]; !exist {
		kv.notify[index] = make(chan Reply)
	}
	return kv.notify[index]
}

func (kv *KVServer) isDuplicate(cid int64, sid uint64) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	last, exist := kv.lastReply[cid]
	return exist && sid <= last.Sid
}
