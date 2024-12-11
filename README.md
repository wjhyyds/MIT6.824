Lab3A

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

Lab3B
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

lab 3C

1 实验目标
如果基于 Raft 的服务器重新启动，它应该从中断处恢复服务。这要求 Raft 保持在重启后仍然存在的持久状态。论文的图 2 提到了哪种状态应该是持久的，即logs、currentTerm和votedFor。在Lab3C中，我们的任务便是实现persist()和readPersist()这两个核心函数，前者负责保存Raft的状态，后者则是在Raft启动时恢复之前保存的数据。

2 序列化数据，然后持久化给raftstate



只要修改了currentTerm、votedFor、logs、其中的一个就要调用persist函数
3 一些思考
生产环境中，至少对于 raft 日志，应该是通过一个类似于 wal 的方式来顺序写磁盘并有可能在内存和磁盘上都 truncate 未提交的日志。当然，是不是每一次变化都要 async 一下可能就是性能和安全性之间的考量了。

lab3D

按照目前的情况，重新启动的服务器会重放完整的 Raft 日志以恢复其状态。然而，对于一个长期运行的服务来说，永远记录完整的 Raft 日志是不切实际的。需要使用快照服务配合，此时Raft会丢弃快照之前的日志条目。lab3D就是需要我们实现日志压缩，具体来说是核心是Snapshot（快照保存函数）以及InstallSnapshotRPC，快照压缩的流程：
1. 每个peer都会通过Snapshot捕获当前系统状态的一个快照。这通常包括但不限于状态机的当前状态、任何必要的元数据、以及快照生成时的任期信息。
2. 当Leader认为有必要向Follower发送快照时，它将发起InstallSnapshotRPC调用。这通常发生在Follower的日志状态与Leader严重脱节时，例如日志冲突无法通过常规的AppendEntriesRPC解决。
3. Follower接收到快照后，会验证其完整性和一致性，然后应用快照以替换其当前状态和日志。这包括清除快照点之前的所有日志条目，并将状态机恢复到快照所表示的状态。
4. Follower在成功应用快照后，应通过RPC回复向Leader确认，表明快照已被正确安装。Leader据此更新其matchIndex和nextIndex数组，以反映Follower的最新状态。

1 捕获系统快照

2

压缩日志 可以说为一个创新点

##Lab4A


1 Clerk、Service、Raft之间的交互
本实验旨在利用lab 3中的Raft库，构建一个具备容错能力的键值存储服务。服务将作为一个复制状态机，由多个服务器组成，各服务器通过Raft协议同步数据库状态。即使在部分故障或网络隔离的情况下，只要大多数服务器正常，服务仍需继续响应客户端请求。在lab 4完成后，你将实现图中Raft交互的所有部分（Clerk、Service和Raft）。

1 客户端通过Clerk与键值服务交互，发送RPC请求，支持Put、Append和Get三种操作。
2 服务需确保这些操作线性化，如果逐个调用，这些方法应表现得好像系统只有一个状态副本，每个3调用都应观察到前序调用序列对状态的修改。
3 对于并发调用，返回值和最终状态必须与操作按某种顺序逐个执行时相同。如果调用在时间上重叠，则认为是并发调用。
2 实验思路
基于lab3实现的Raft，实现一个可用的KV服务，这意味着我们需要保证线性一致性（要求从外部观察者的角度来看，所有操作都按照某个全局顺序执行，并且结果与这些操作按该顺序串行执行的结果相同）。尽管 Raft 共识算法本身支持线性化语义，但要真正保证线性化语义在整个系统中生效，仍然需要上层服务的配合


Raft解决方法：客户端为每个命令分配一个唯一的序列号。状态机会记录每个客户端的最新序列号及其对应的执行结果。如果一个命令的序列号已经被处理过，则系统会直接返回先前的结果，而不会重新执行该命令。这样可以确保每个命令只被应用到状态机一次，避免了重复执行可能带来的线性一致性问题。（有点三次握手的意思啊）
具体实现：
客户端命令唯一化：ClientId + CommandId（客户端唯一标识符+整数）
服务器端状态记录 ：在服务器端，维护一个映射表，这个映射表以ClientId作为主键，其值是一个结构体包含：
● 最近执行的来自该客户端的CommandId。
● 对应的命令执行结果。
重复命令检测与处理 (感觉这个思路可以用到很多地方)
● 当一个新命令到达时，首先检查映射表中是否存在对应的ClientId条目。
● 如果存在，则比较新命令的CommandId与映射表中记录的CommandId。
  ○ 如果新命令的CommandId小于或等于记录的CommandId，则说明这是一个重复命令，服务器可以直接返回之前存储的结果。
  ○ 如果新命令的CommandId大于记录的CommandId，则说明这是新的命令，服务器应该正常处理这个命令，并更新映射表中对应ClientId的CommandId及结果。
● 如果不存在对应的ClientId条目，则将此命令视为首次出现的命令进行处理，并添加一个新的条目到映射表中。
3 具体代码实现
3.1 小改Raft用于识别CommandId

3.2 client.go定于客户端参数、三种调用RPC方法

客户端向服务端发起RPC调用的具体函数


3.3 common.go定义通用方法
    
调试或者生产环境下debug模式					String（）格式化报错输出

                           
                                               标准化操作符
3.4 kvsm.go 定义数据库底层的三种方法 Get、Put、Append
