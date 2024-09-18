package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	cid    int64 // 客户端id
	seq    int64 // 上一次操作的序列号
	leader int
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
	// Your code here.
	ck.cid = nrand()
	ck.seq = 0
	ck.leader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	return ck.Request(&Request{Op: OpQuery, Num: num})
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.Request(&Request{Op: OpJoin, Servers: servers})
}

func (ck *Clerk) Leave(gids []int) {
	ck.Request(&Request{Op: OpLeave, GIDs: gids})
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.Request(&Request{Op: OpMove, Shard: shard, GID: gid})
}

func (ck *Clerk) Request(args *Request) Config {
	args.Cid, args.Seq = ck.cid, ck.seq
	for {
		var reply Reply
		if !ck.servers[ck.leader].Call("ShardCtrler.Do", args, &reply) || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leader = (ck.leader + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		ck.seq++
		return reply.Config
	}
}
