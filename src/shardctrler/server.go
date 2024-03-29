package shardctrler

import "6.5840/raft"
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"
import "sort"

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	dead      int32
	configs   []Config // indexed by config num
	waitChannels map[int]chan Op
	requests    map[int64]int
}

type Op struct {
	// Your data here.
	ClientId int64
	RequestId    int
	OpType   string
	OpIdx    int
	Servers map[int][]string
	GIDs []int
	Shard int
	GID   int
	Num int
	Cfg Config
	Err Err
}

const JOIN = "JOIN"
const MOVE = "MOVE"
const LEAVE = "LEAVE"
const QUERY = "QUERY"

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ErrWrongLeader
	if !sc.IsLeader(){
		return
	}

	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, OpType: JOIN, Servers: args.Servers}
	opIdx, _, _ := sc.rf.Start(op)
	op.OpIdx = opIdx

	response := <-sc.findChannelByOpIdx(opIdx)
	if op.RequestId == response.RequestId && op.ClientId == response.ClientId{
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ErrWrongLeader
	if !sc.IsLeader(){
		return
	}
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, OpType: LEAVE, GIDs: args.GIDs}

	opIdx, _, _ := sc.rf.Start(op)
	op.OpIdx = opIdx

	response := <-sc.findChannelByOpIdx(opIdx)
	if op.RequestId == response.RequestId && op.ClientId == response.ClientId{
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ErrWrongLeader
	if !sc.IsLeader(){
		return
	}
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, OpType: MOVE, GID: args.GID, Shard: args.Shard}
	opIdx, _, _ := sc.rf.Start(op)
	op.OpIdx = opIdx

	response := <-sc.findChannelByOpIdx(opIdx)
	if op.RequestId == response.RequestId && op.ClientId == response.ClientId{
		reply.Err = OK
		reply.WrongLeader = false
		return
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.WrongLeader = true
	reply.Err = ErrWrongLeader
	if !sc.IsLeader(){
		return
	}
	op := Op{ClientId: args.ClientId, RequestId: args.RequestId, OpType: QUERY, Num: args.Num}
	opIdx, _, _ := sc.rf.Start(op)
	op.OpIdx = opIdx

	response := <-sc.findChannelByOpIdx(opIdx)
	if op.RequestId == response.RequestId && op.ClientId == response.ClientId{
		reply.Err = OK
		reply.WrongLeader = false
		reply.Config = response.Cfg
		return
	}
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.requests = make(map[int64]int)
	sc.waitChannels = make(map[int]chan Op)

	go sc.executeOps()
	return sc
}

func (sc *ShardCtrler) executeOps(){
	for true {
		applyMsg := <-sc.applyCh
		op := applyMsg.Command.(Op)
		if sc.IsDuplicate(op.ClientId, op.RequestId){
			op.Err = ErrIsDuplicate
			sc.findChannelByOpIdx(applyMsg.CommandIndex) <- op
			continue
		}

		sc.mu.Lock()
		if op.OpType == JOIN{
			sc.executeJoinOp(op.Servers)
		}else if op.OpType == LEAVE{
			sc.executeLeaveOp(op.GIDs)
		}else if op.OpType == MOVE{
			sc.executeMoveOp(op.GID, op.Shard)
		}else if op.OpType == QUERY{
			op = sc.executeQueryOp(op)
		}
		sc.requests[op.ClientId] = op.RequestId
		op.Err = OK
		sc.mu.Unlock()

		if sc.IsLeader(){
			sc.findChannelByOpIdx(applyMsg.CommandIndex) <- op
		}
	}
}

func (sc *ShardCtrler) IsDuplicate(ClientId int64, RequestId int) bool{
	sc.mu.Lock()
	if latestRequestId, contains := sc.requests[ClientId]; contains{
		sc.mu.Unlock()
		return RequestId <= latestRequestId
	}
	sc.mu.Unlock()
	return false
}

func (sc *ShardCtrler) findChannelByOpIdx(opIdx int) chan Op{
	sc.mu.Lock()
	result, contains := sc.waitChannels[opIdx]
	if contains{
		sc.mu.Unlock()
		return result
	}else{
		sc.waitChannels[opIdx] = make(chan Op, 1)
		result = sc.waitChannels[opIdx]
	}
	sc.mu.Unlock()
	return result
}

func (sc *ShardCtrler) IsLeader() bool{
	_, isLeader := sc.rf.GetState()
	return isLeader
}

func (sc *ShardCtrler) executeJoinOp(ServerNamesMap map[int][]string){
	newConfig := sc.getNewConfigFromLatestConfig()

	for groupId, serverNames := range sc.getLatestConfig().Groups{
		newConfig.Groups[groupId] = serverNames
	}

	for groupId, serverNames := range ServerNamesMap{
		newConfig.Groups[groupId] = serverNames 
	}

	groupIds := make([]int, 0)
	for groupId, _ := range newConfig.Groups{
		groupIds = append(groupIds, groupId)
	} 

	newConfig.Shards = sc.getShardsForNewConfig(groupIds)
	sc.applySortedShardsToConfig(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) executeLeaveOp(groupIDs []int){
	newConfig := sc.getNewConfigFromLatestConfig()
	
	groupIdsMap := make(map[int]bool)
	for _, groupId := range groupIDs{
		groupIdsMap[groupId] = true
	}

	for groupId, serverNames := range sc.getLatestConfig().Groups{
		if !groupIdsMap[groupId]{
			newConfig.Groups[groupId] = serverNames
		}
	}

	groupIds := make([]int, 0)
	for groupId, _ := range newConfig.Groups{
		groupIds = append(groupIds, groupId)
	} 
	
	newConfig.Shards = sc.getShardsForNewConfig(groupIds)
	sc.applySortedShardsToConfig(&newConfig)
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) executeMoveOp(gid int, shardId int){
	newConfig := sc.getNewConfigFromLatestConfig()

	for groupId, serverNames := range sc.getLatestConfig().Groups{
		newConfig.Groups[groupId] = serverNames
	}

	newConfig.Shards[shardId] = gid
	sc.configs = append(sc.configs, newConfig) 
}

func (sc *ShardCtrler) executeQueryOp(op Op) Op{
	if op.Num != -1 && op.Num < len(sc.configs){
		op.Cfg = sc.configs[op.Num]
		return op
	}
	op.Cfg = sc.getLatestConfig()
	return op
}

func (sc *ShardCtrler) getLatestConfig() Config{
	return sc.configs[len(sc.configs) - 1]
}

func (sc *ShardCtrler) applySortedShardsToConfig(cfg *Config){
	copiedShards := make([]int, len(cfg.Shards))
	copy(copiedShards[:], cfg.Shards[:])
	sort.Ints(copiedShards)
	copy(cfg.Shards[:], copiedShards[:])
}

func (sc *ShardCtrler) getShardsForNewConfig(groupIds []int) [NShards]int{
	var result [NShards]int
	if len(groupIds) == 0{
		return result
	}
	
	for i := 0; i < NShards; i++{
		result[i] = groupIds[i % len(groupIds)]
	}
	return result
}

func (sc *ShardCtrler) getNewConfigFromLatestConfig() Config{
	return Config{Num: sc.getLatestConfig().Num + 1,
					Shards: sc.getLatestConfig().Shards, 
					Groups: make(map[int][]string)}
}