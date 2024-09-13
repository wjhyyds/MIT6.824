package kvraft

import (
	"bytes"
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
	db          *MemoryDb          // 保存状态
	notify      map[int]chan Reply // log index / chan op
	lastReply   map[int64]Reply    // cid/reply
	lastApplied int                // 防止apply快照时db回滚
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
			// 过时的日志
			kv.mu.Lock()
			if msg.CommandIndex <= kv.lastApplied {
				kv.mu.Unlock()
				continue
			}
			kv.lastApplied = msg.CommandIndex
			kv.mu.Unlock()

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

			// snapshot
			if kv.maxraftstate != -1 && kv.rf.RaftStateSize() > kv.maxraftstate {
				DPrintf("trigger snapshot,lastApplied=%v,raftsize=%d", kv.lastApplied, kv.rf.RaftStateSize())
				snapshot := kv.encode()
				kv.rf.Snapshot(kv.lastApplied, snapshot)
			}
		}

		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.decode(msg.Snapshot)
			kv.lastApplied = msg.SnapshotIndex
			kv.mu.Unlock()
		}
	}
}

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
	kv.lastApplied = -1

	snapshot := persister.ReadSnapshot()
	kv.decode(snapshot)

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

func (kv *KVServer) encode() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.lastReply)
	e.Encode(kv.db.kv)
	return w.Bytes()
}

func (kv *KVServer) decode(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var lastReply map[int64]Reply
	var db map[string]string

	if err1, err2 := d.Decode(&lastReply), d.Decode(&db); err1 != nil || err2 != nil {
		log.Fatalf("fail to decode,err1=%+v,err2=%+v", err1, err2)
	} else {
		kv.db.kv = db
		kv.lastReply = lastReply
	}
}
