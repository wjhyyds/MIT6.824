package shardctrler

type stateMachine interface {
	Join(Servers map[int][]string) Err
	Leave(GIDs []int) Err
	Move(Shard, GID int) Err
	Query(Num int) (Err, Config)
}

type MemoryConfigStateMachine struct {
	configs []Config
}

func newMemoryConfigStateMachine() *MemoryConfigStateMachine {
	configs := make([]Config, 1)
	configs[0].Groups = map[int][]string{}

	return &MemoryConfigStateMachine{configs: configs}
}

func (sm *MemoryConfigStateMachine) Join(Servers map[int][]string) Err {
	oldConfig := sm.configs[len(sm.configs)-1]
	newGroups := make(map[int][]string)

	for gid, servsers := range oldConfig.Groups {
		// newGroups[gid] = slices.Clone(servers)
		newGroups[gid] = append(newGroups[gid], servsers...)
	}

	for gid, servers := range Servers {
		newGroups[gid] = servers
	}

	// 负载均衡
	newShards := loadBalanceWhenJoin(oldConfig, newGroups)

	newConfig := Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *MemoryConfigStateMachine) Leave(Gids []int) Err {
	oldConfig := sm.configs[len(sm.configs)-1]

	newGroups := make(map[int][]string)
	for gid, servers := range oldConfig.Groups {
		newGroups[gid] = append(newGroups[gid], servers...)
	}
	for _, gid := range Gids {
		delete(newGroups, gid)
	}

	newShards := loadBalanceWhenLeave(oldConfig, newGroups)

	newConfig := Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *MemoryConfigStateMachine) Move(Shard int, Gid int) Err {
	oldConfig := sm.configs[len(sm.configs)-1]

	var newShards [NShards]int
	for i := 0; i < NShards; i++ {
		newShards[i] = oldConfig.Shards[i]
	}
	newShards[Shard] = Gid

	// groups不变
	newGroups := make(map[int][]string)
	for gid, servers := range oldConfig.Groups {
		newGroups[gid] = append(newGroups[gid], servers...)
	}

	newConfig := Config{
		Num:    len(sm.configs),
		Shards: newShards,
		Groups: newGroups,
	}

	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *MemoryConfigStateMachine) Query(Num int) (Err, Config) {
	if Num < 0 || Num >= len(sm.configs) {
		// log.Fatalf("Query index out of range,index=%d,len(configs)=%d", Num, len(sm.configs))
		return OK, sm.configs[len(sm.configs)-1]
	}
	return OK, sm.configs[Num]
}
