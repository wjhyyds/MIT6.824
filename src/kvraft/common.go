package kvraft

import (
	"fmt"
	"log"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
//执行超时时间设置为1000毫秒
const ExecuteTimeout = 1000 * time.Millisecond //纳秒
//const ExecuteTimeout = 1000 * time.Microcond//微秒

type Err uint8

const (
	Ok	Err = iota
	ErrNoKey      
	ErrWrongLeader
	ErrTimeout
)

//报错的格式化输出
func (err Err) String() string {
	switch err {
	case Ok:
		return "Ok"
	case ErrNoKey:
		return "ErrNoKey"
	case ErrWrongLeader:
		return "ErrWrongLeader"
	case ErrTimeout:
		return "ErrTimeout"
	}
	panic(fmt.Sprintf("unexpected err %d", err))
}

type OpType uint8

const (
	OpPut	OpType = iota
	OpAppend
	OpGet
)

func (opType OpType) String() string {
	switch opType {
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	case OpGet:
		return "Get"
	}
	panic(fmt.Sprintf("unexpected OpType %d", opType))
}


type CommandArgs struct {
	Key			string
	Value 		string
	Op 			OpType //三种操作符
	ClientId	int64
	CommandId   int64
}
//fmt.Println(args),String就是在打印的时候自定义结构化字符串

func (args CommandArgs) String() string {
	return fmt.Sprintf("{Key:%v, Value:%v, Op:%v, ClientId:%v, Id:%v}",
					args.Key, args.Value, args.Op, args.ClientId, args.CommandId)
}

type CommandReply struct {
	Err   Err
	Value string
}

func (reply CommandReply) String() string {
	return fmt.Sprintf("{Err:%v, Value:%v}",reply.Err, reply.Value)
}

//储存最近执行的上下文
type OperationContext struct {
	MaxAppliedCommandId int64
	LastReply			*CommandReply
}

type Command struct {
	*CommandArgs
}