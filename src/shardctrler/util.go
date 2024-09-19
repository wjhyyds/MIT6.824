package shardctrler

import (
	"sort"
)

func loadBalanceWhenJoin(oldConfig Config, newGroup map[int][]string) [NShards]int {
	allGroups := make([]int, len(newGroup))
	for g := range newGroup {
		allGroups = append(allGroups, g)
	}

	g2s := group2Shards(oldConfig.Shards, allGroups)
	// log.Printf("Before join: g2s=%v", g2s)

	for {
		src, dst := gidWithMostShards(g2s), gidWithLeastShards(g2s)
		if src != 0 && len(g2s[src])-len(g2s[dst]) <= 1 {
			break
		}
		// log.Println(src, dst)
		shard := g2s[src][0]
		g2s[src] = g2s[src][1:]
		g2s[dst] = append(g2s[dst], shard)

	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	// log.Printf("After join:g2s=%v,shards=%v", g2s, newShards)

	return newShards
}

func loadBalanceWhenLeave(oldConfig Config, newGroup map[int][]string) [NShards]int {
	allGroups := make([]int, len(newGroup))
	for g := range newGroup {
		allGroups = append(allGroups, g)
	}

	// g2s只包含保留下来的groups
	g2s := group2Shards(oldConfig.Shards, allGroups)
	// log.Printf("Before leave: g2s=%v", g2s)

	// 统计空缺的shards
	leaveShards := []int{}
	for s, g := range oldConfig.Shards {
		if _, ok := g2s[g]; !ok {
			leaveShards = append(leaveShards, s)
		}
	}
	// log.Printf("Before leave: leaveShards=%v", leaveShards)

	// 将空缺的shards分配给剩下的group
	for _, shard := range leaveShards {
		target := gidWithLeastShards(g2s)
		// log.Println(target, shard)
		g2s[target] = append(g2s[target], shard)
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	// log.Printf("After leave:g2s=%v,shards=%v", g2s, newShards)

	return newShards
}

func gidWithMostShards(g2s map[int][]int) int {
	// gid 0代表未分配
	if unAllocatedShards, ok := g2s[0]; ok && len(unAllocatedShards) > 0 {
		return 0
	}

	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	gid, maxSz := 0, -1
	for _, g := range keys {
		if len(g2s[g]) > maxSz {
			gid = g
			maxSz = len(g2s[g])
		}
	}

	return gid
}

func gidWithLeastShards(g2s map[int][]int) int {
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	gid, minSz := 0, NShards+1
	for _, g := range keys {
		if g != 0 && len(g2s[g]) < minSz {
			gid = g
			minSz = len(g2s[g])
		}
	}

	return gid
}

func group2Shards(oldShards [NShards]int, allGroups []int) (g2s map[int][]int) {
	g2s = map[int][]int{}
	for _, g := range allGroups {
		g2s[g] = make([]int, 0)
	}
	for i, g := range oldShards {
		if _, ok := g2s[g]; ok {
			g2s[g] = append(g2s[g], i)
		}
	}

	return g2s
}
