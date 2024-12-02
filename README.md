raft是一种复制状态机协议，用于构建一个自带容错错KV存储系统
1.实现raft:作为一个模块供后续功能使用；Raft实例之间通过RPC通信，维持复制日志的一致性；支持无限数量的日志条目
1.日志选举（Leader Election）

2.设计Raft结构体

设计Raft结构体
type Raft struct {
    mu        sync.RWMutex        // Lock to protect shared access to this peer's state, to use RWLock for better performance
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]
    dead      int32               // set by Kill()

    // Persistent state on all servers(Updated on stable storage before responding to RPCs)
    currentTerm int        // latest term server has seen(initialized to 0 on first boot, increases monotonically)
    votedFor    int        // candidateId that received vote in current term(or null if none)
    logs        []LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader(first index is 1)

    // Volatile state on all servers
    commitIndex int // index of highest log entry known to be committed(initialized to 0, increases monotonically)
    lastApplied int // index of highest log entry applied to state machine(initialized to 0, increases monotonically)

    // Volatile state on leaders(Reinitialized after election)
    nextIndex  []int // for each server, index of the next log entry to send to that server(initialized to leader last log index + 1)
    matchIndex []int // for each server, index of highest log entry known to be replicated on server(initialized to 0, increases monotonically)

    // other properties
    state          NodeState     // current state of the server
    electionTimer  *time.Timer   // timer for election timeout
    heartbeatTimer *time.Timer   // timer for heartbeat
    applyCh        chan ApplyMsg // channel to send apply message to service
}

3通用函数
3.1获取当前纳秒级时间戳
var GlobalRand = &LockedRand{
    rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}
随机选举、心跳超时时间间隔。超时处理为什么要引入随机数呢？
func RandomElectionTimeout() time.Duration {return time.Duration(ElectionTimeout+GlobalRand.Intn(ElectionTimeout)) * time.Millisecond}

func StableHeartbeatTimeout() time.Duration {return time.Duration(HeartbeatTimeout) * time.Millisecond}
4
4.1Raft一致性
case Follower:
//重置一个新的随机选举时间，引入随机防止多个节点同时发起选举
//Leader超时会变成candidate,然后随机发起选举
        rf.electionTimer.Reset(RandomElectionTimeout())
//Leader节点定期发送心跳给Follower节点
        rf.heartbeatTimer.Stop()
case Leader:
//同理leader停止选举
        rf.electionTimer.Stop() // stop election
//leader定期发送心跳给Follow保持联系，维持领导避免不必要的选举
        rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
    }
