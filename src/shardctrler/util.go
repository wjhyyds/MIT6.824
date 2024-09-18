package shardctrler

import "sort"

func loadBalanceWhenJoin(oldConfig Config, newGroup map[int][]string) [NShards]int {
	allGroups := make([]int, len(newGroup))
	for g := range newGroup {
		allGroups = append(allGroups, g)
	}

	g2s := group2Shards(oldConfig.Shards, allGroups)
	for {
		src, dst := gidWithMostShards(g2s), gidWithLeastShards(g2s)
		if len(g2s[src])-len(g2s[dst]) <= 1 {
			break
		}
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
	return newShards
}

func loadBalanceWhenLeave(oldConfig Config, newGroup map[int][]string) [NShards]int {
	allGroups := make([]int, len(newGroup))
	for g := range newGroup {
		allGroups = append(allGroups, g)
	}

	// g2s只包含保留下来的groups
	g2s := group2Shards(oldConfig.Shards, allGroups)

	// 统计空缺的shards
	leaveShards := []int{}
	for s, g := range oldConfig.Shards {
		if _, ok := g2s[g]; !ok {
			leaveShards = append(leaveShards, s)
		}
	}

	// 将空缺的shards分配给剩下的group
	for _, shard := range leaveShards {
		target := gidWithLeastShards(g2s)
		g2s[target] = append(g2s[target], shard)
	}

	var newShards [NShards]int
	for gid, shards := range g2s {
		for _, shard := range shards {
			newShards[shard] = gid
		}
	}

	return newShards
}

func gidWithMostShards(g2s map[int][]int) int {
	var keys []int
	for k := range g2s {
		keys = append(keys, k)
	}

	sort.Ints(keys)

	gid, maxSz := -1, -1
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

	gid, minSz := -1, NShards+1
	for _, g := range keys {
		if len(g2s[g]) < minSz {
			gid = g
			minSz = len(g2s[g])
		}
	}

	return gid
}

func group2Shards(oldShards [NShards]int, allGroups []int) (g2s map[int][]int) {
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
