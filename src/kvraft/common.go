package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string

	Op  string // Put/Append
	Sid uint64
	Cid int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string

	Sid uint64
	Cid int64
}

type GetReply struct {
	Err   Err
	Value string
}
