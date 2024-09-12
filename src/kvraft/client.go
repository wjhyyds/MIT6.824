package kvraft

import (
	"crypto/rand"
	"math/big"

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
	ck.leader = 0
	return ck
}

func (ck *Clerk) Request(args *Request) string {
	args.Cid, args.Sid = ck.cid, ck.sid
	for {
		var reply Reply
		DPrintf("C%d %v-> S%d", ck.cid, args.Op, ck.leader)
		if !ck.servers[ck.leader].Call("KVServer.Do", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			DPrintf("C%d <- S%d,reply=%+v", ck.cid, ck.leader, reply)
			ck.leader = (ck.leader + 1) % len(ck.servers)
			continue
		}
		ck.sid++
		return reply.Value
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.Request(&Request{Op: "Get", Key: key})
}

func (ck *Clerk) Put(key string, value string) {
	ck.Request(&Request{Op: "Put", Key: key, Value: value})
}
func (ck *Clerk) Append(key string, value string) {
	ck.Request(&Request{Op: "Append", Key: key, Value: value})
}
