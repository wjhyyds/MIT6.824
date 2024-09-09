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

const Debug = true

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
	db     *MemoryDb        // 保存状态
	notify map[int]chan Op  // log index / chan op
	tokens map[int64]uint64 // cid/sid
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	op := Op{
		Type: "Get",
		Sid:  args.Sid,
		Cid:  args.Cid,
		Key:  args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.notify, index)
		kv.mu.Unlock()
	}()

	select {
	case <-ch:
		kv.mu.Lock()
		reply.Value, reply.Err = kv.db.Get(args.Key)
		kv.mu.Unlock()
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if kv.killed() {
		reply.Err = ErrWrongLeader
		return
	}

	// DPrintf("S%d %v <- C%d,args=%+v", kv.me, args.Op, args.Cid, args)

	op := Op{
		Type: args.Op,
		Sid:  args.Sid,
		Cid:  args.Cid,
		Key:  args.Key,
		Val:  args.Value,
	}

	if kv.isDuplicate(op) {
		reply.Err = OK
		return
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	ch := kv.getNotifyCh(index)
	defer func() {
		kv.mu.Lock()
		delete(kv.notify, index)
		close(ch)
		kv.mu.Unlock()
	}()

	select {
	case <-ch:
		reply.Err = OK
	case <-time.After(ExecuteTimeout):
		reply.Err = ErrTimeout
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
	kv.notify = make(map[int]chan Op)
	kv.tokens = make(map[int64]uint64)

	go kv.applier()

	return kv
}

func (kv *KVServer) getNotifyCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	ch, exist := kv.notify[index]
	if !exist {
		ch = make(chan Op)
		kv.notify[index] = ch
	}

	return ch
}

func (kv *KVServer) isDuplicate(op Op) bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if lastSid, exist := kv.tokens[op.Cid]; exist {
		return op.Sid <= lastSid
	}
	return false
}

func (kv *KVServer) applier() {
	for !kv.killed() {

		msg := <-kv.applyCh
		// DPrintf("S%d <- rf,msg=%+v", kv.me, msg)
		if msg.CommandValid {
			index, op := msg.CommandIndex, msg.Command.(Op)
			if !kv.isDuplicate(op) {
				kv.mu.Lock()
				switch op.Type {
				case "Put":
					kv.db.Put(op.Key, op.Val)
				case "Append":
					kv.db.Append(op.Key, op.Val)
				}
				kv.tokens[op.Cid] = op.Sid
				kv.mu.Unlock()
			}
			// DPrintf("S%d flag", kv.me)
			// NOTE:只有Leader才需要notify,否则会导致阻塞
			if _, isLeader := kv.rf.GetState(); isLeader {
				kv.getNotifyCh(index) <- op
			}
		}
		// DPrintf("S%d finish a apply loop", kv.me)
	}
}
