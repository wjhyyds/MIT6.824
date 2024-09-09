package kvraft

import (
	"crypto/rand"
	"math/big"
	mrand "math/rand"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	cid    int64  // 客户端id
	sid    uint64 // 上一次操作的序列号
	leader int    // raft的leader所在的服务器
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.cid = nrand()
	ck.sid = 0
	ck.leader = mrand.Intn(len(ck.servers))
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.sid++
	args := GetArgs{
		Key: key,
		Sid: ck.sid,
		Cid: ck.cid,
	}

	for {
		var reply GetReply
		// DPrintf("C%d %v -> S%v,args=%+v", ck.cid, "Get", ck.leader, args)
		if ok := ck.servers[ck.leader].Call("KVServer.Get", &args, &reply); ok {
			// DPrintf("C%d <- S%v,reply=%+v", ck.cid, ck.leader, reply)
			switch reply.Err {
			case ErrWrongLeader, ErrTimeout:
				ck.leader = (ck.leader + 1) % len(ck.servers)
				continue
			case OK, ErrNoKey:
				return reply.Value
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.sid++
	args := PutAppendArgs{
		Key:   key,
		Value: value,

		Op:  op,
		Sid: ck.sid,
		Cid: ck.cid,
	}

	for {
		var reply PutAppendReply
		// DPrintf("C%d %v -> S%v,args=%+v", ck.cid, op, ck.leader, args)
		if ok := ck.servers[ck.leader].Call("KVServer.PutAppend", &args, &reply); ok {
			// DPrintf("C%d <- S%v,reply=%+v", ck.cid, ck.leader, reply)
			switch reply.Err {
			case ErrWrongLeader, ErrTimeout:
				ck.leader = (ck.leader + 1) % len(ck.servers)
				continue
			case OK:
				return
			default:
				panic("unexpected Err")
			}
		}
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
