package kvraft

import (
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft //调用raft底层代码
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big
	lastApplied  int //记录最后一个index避免重复请求
	//实现内存中的键值对存储
	stateMachine	KVStateMachine 
	//用于判断有没有重复请求
	//存储上一次操作的上下文
	lastOperations	map[int64]OperationContext //对于put、append的缓存？？
	//Raft返回值的channel
	notifyChs		map[int] chan *CommandReply
}

//预防重复命令
func (kv *KVServer) isDuplicatedCommand(clientId, commandId int64) bool {
	OperationContext,ok := kv.lastOperations[clientId]
	//如果这个命令id小于当前应用的最大id
	return ok && commandId <= OperationContext.MaxAppliedCommandId
}

//服务端的执行命令函数，在哪里被调用了呢
func (kv *KVServer) ExecuteCommand(args *CommandArgs, reply *CommandReply) {
	kv.mu.RLock()
	//如果当前是其他方法（append、put）并且查询的ID是之前重复的
	if args.Op != OpGet && kv.isDuplicatedCommand(args.ClientId, args.CommandId) {
		// 直接返回缓存
		lastReply := kv.lastOperations[args.ClientId].LastReply
		reply.Value, reply.Err = lastReply.Value, lastReply.Err
		kv.mu.RUnlock()
		return
	}
	//这里把锁释放掉了是防止下面的start函数调用RPC产生死锁
	kv.mu.RUnlock() 
	//判断完不是重复查询，从这里进到rf的底层
	//判断当前节点还是不是Leader
	//返回当前index、Term、是否领导
	//通知raft复制并应用日志
	index,_, isLeader:= kv.rf.Start(Command{args})
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.getNotifyCh(index)
	kv.mu.Unlock()

	select {
	case reslut := <-ch : //chan 中有数据可读
		reply.Value, reply.Err = reslut.Value, reslut.Err
	case <-time.After(ExecuteTimeout) : //处理超时情况
		reply.Err =  ErrTimeout
	}
	go func() {
		kv.mu.Lock()
		kv.deleteNotifyCh(index) //notifyChs是一个并发的channel 要加锁操作
		kv.mu.Unlock()
	}()
}

//获取notice 如果channel里面没有就得重新创建
//什么时候会出现！ok的情况呢
func (kv *KVServer) getNotifyCh (index int)chan *CommandReply {
	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan *CommandReply, 1)
	}
	return kv.notifyChs[index]
}

// 删除notice
func (kv *KVServer) deleteNotifyCh(index int) {
	delete(kv.notifyChs, index)
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

//根据command里的op执行相应操作
func(kv *KVServer) applyLogToStateMachine(command Command) *CommandReply {
	reply := new(CommandReply)
	switch command.Op {
	case OpGet:
		reply.Value,reply.Err = kv.stateMachine.Get(command.Key)
	case OpPut:
		reply.Err = kv.stateMachine.Put(command.Key, command.Value)
	case OpAppend:
		reply.Err = kv.stateMachine.Append(command.Key, command.Value)
	}
	return reply
}

func (kv *KVServer) applier() {
	for kv.killed() == false {
		select {
		case message := <- kv.applyCh: //raft的applier进程里面有写消息进去
			DPrintf("{Node %v} tries to apply message %v",kv.rf.GetId(), message)
			if message.CommandValid {
				kv.mu.Lock()
				if message.CommandIndex <= kv.lastApplied { //Index过期重复请求
					DPrintf("{Node %v} discards outdated message %v because a newer snapshot which lastApplied is %v has been restored", kv.rf.GetId(), message,kv.lastApplied)
					kv.mu.Unlock()
					continue
				}
				//更新最后应用的index，这一步不能出错
				kv.lastApplied = message.CommandIndex
				reply := new(CommandReply)
				command := message.Command.(Command)
				//防止重复请求，这不应该第一个过期请求就给他过滤掉了吗
				//这里的意思是put和append方法会记录一下缓存
				if command.Op != OpGet && kv.isDuplicatedCommand(command.ClientId, command.CommandId) {
					DPrintf("{Node %v} doesn't apply duplicated message %v to stateMachine because maxAppliedCommandId is %v for client %v", kv.rf.GetId(), message, kv.lastOperations[command.ClientId], command.ClientId)
					reply = kv.lastOperations[command.ClientId].LastReply
				}else{
					reply = kv.applyLogToStateMachine(command)
					if command.Op != OpGet {//如果是Put、append方法，有点类似缓存的感觉
						kv.lastOperations[command.ClientId] = OperationContext{
							MaxAppliedCommandId:	command.CommandId,
							LastReply: 				reply,	
						}
					}
				}
				//确保还是在当前Leader的任期里面
				if currentTerm, isLeader := kv.rf.GetState(); isLeader && message.CommandTerm == currentTerm {
					ch := kv.getNotifyCh(message.CommandIndex)
					ch <-reply
				}
				kv.mu.Unlock()
			}
		}
	}
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
	labgob.Register(Command{})
	applyCh := make(chan raft.ApplyMsg) //这个是用于raft组件之间通信的channel

	kv := &KVServer{
		mu:				sync.RWMutex{},
		me: 			me,
		rf: 			raft.Make(servers,me,persister,applyCh), //这里也调用了make服务
		applyCh: 		applyCh,//raft之间通信的channel
		dead: 			0,
		maxraftstate: 	maxraftstate,
		stateMachine: 	&KV_dataset{KV: make(map[string]string)},
		lastOperations: make(map[int64]OperationContext),
		notifyChs: 		make(map[int]chan *CommandReply),
	}
	go kv.applier()
	return kv
}
