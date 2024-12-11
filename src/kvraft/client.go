package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leaderId	int
	clientId	int64
	commandId 	int64  
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

//创建一个clerk客户端与服务端进行交互
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	return &Clerk{
		servers: 	servers,
		leaderId: 	0,
		clientId: 	nrand(),//这里取随机数会不会有问题TODO
		commandId:  0, //每次自增
	}
}

func (ck *Clerk) Get(key string) string {
	return ck.ExecuteCommand(&CommandArgs{Key: key, Op: OpGet})
}
func (ck *Clerk) Put(key string, value string) {
	ck.ExecuteCommand(&CommandArgs{Key: key, Value: value, Op: OpPut})
}
func (ck *Clerk) Append(key string, value string) {
	ck.ExecuteCommand(&CommandArgs{Key: key, Value: value, Op: OpAppend})
}

func (ck *Clerk) ExecuteCommand(args *CommandArgs ) string {
	//获取标志着这个指令的唯一标识符，即进程ID和命令ID
	args.ClientId, args.CommandId = ck.clientId, ck.commandId
	for {
		reply := new(CommandReply)
		//如果调用失败了、不是leader了、超时了
		if !ck.servers[ck.leaderId].Call("KVServer.ExecuteCommand",args, reply) || 
			reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)//找到底哪一个是leader
			continue //TODO这里不用加锁用来并发控制吗
		}
		//防止重复
		ck.commandId += 1
		return reply.Value
	}
}