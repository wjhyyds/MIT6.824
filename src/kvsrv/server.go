package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu     sync.Mutex
	data   map[string]string
	record sync.Map
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
	}
	res, ok := kv.record.Load(args.MessageID) //cache to store
	if ok {
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	old := kv.data[args.Key]
	kv.data[args.Key] = args.Value
	reply.Value = old
	kv.mu.Lock()

	kv.record.Store(args.MessageID, old) //create the record as cache
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	if args.MessageType == Report {
		kv.record.Delete(args.MessageID)
	}
	res, ok := kv.record.Load(args.MessageID) //cache to store
	if ok {
		reply.Value = res.(string)
		return
	}
	kv.mu.Lock()
	old := kv.data[args.Key]
	kv.data[args.Key] = args.Value + old
	reply.Value = old
	kv.mu.Lock()

	kv.record.Store(args.MessageID, old) //
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.data = make(map[string]string)
	return kv
}
