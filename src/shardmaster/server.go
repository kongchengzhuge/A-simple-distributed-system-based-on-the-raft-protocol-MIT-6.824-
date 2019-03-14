package shardmaster

import (
	"raft"
	"time"
)
import "labrpc"
import "sync"
import "labgob"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	clerkop		map[int64]int				//每一个客户端保留一个任务数，用来判断最后是否是过期任务
	clerkchan   map[int64]chan string		//为每一个任务定一个chan传递已经提交的信息

	// Your data here.

	configs []Config // indexed by config num
}

type Operation_type int

const(
	operation_join Operation_type = iota
	operation_leave
	operation_move
	operation_query
)

type Op struct {
	// Your data here.
	Optype	Operation_type
	Oxnum	int				//用来标注操作的次数
	Clerkid int64				//操作的客户端
	Opid	int64
	Servers map[int][]string	//for op join
	GIDs []int					//for op leave
	Shard int					//for op move
	Num int						//for op query

}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	cmd:=Op{Optype:operation_join,Servers:args.Servers,Clerkid:args.Clerkid,Oxnum:args.Oxnum,Opid:args.Opid}
	_,_,isleader:=sm.rf.Start(cmd)

	if isleader==false{
		reply.Err=""
		reply.WrongLeader=true
		return
	}

	sm.mu.Lock()
	ch , ok:=sm.clerkchan[args.Opid]
	if !ok{
		sm.clerkchan[args.Opid]=make(chan string,1)
		ch=sm.clerkchan[args.Opid]
	}
	sm.mu.Unlock()

	select {
		case <-ch:
			reply.Err=OK
			reply.WrongLeader=false
		case <-time.After(1000*time.Millisecond):
			reply.WrongLeader=true
			reply.Err="outoftime"
	}

}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	//初始化config[0]
	var shards	[NShards]int
	for i:=0;i<NShards;i++{
		shards[i]=0
	}
	var grounps map[int][]string
	grounps=make(map[int][]string)
	conf:=Config{Num:0,Shards:shards,Groups:grounps}
	sm.configs[0]=conf
	//初始化第一个config完成

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	go func() {
		for {
			//fmt.Printf("现在是kvraft[%d]\n",kv.me)
			applymsg := <-sm.applyCh
			//fmt.Printf("现在是kvraft[%d]收到applymsg消息\n",kv.me)
			if applymsg.CommandValid {
				op, ok := (applymsg.Command).(Op)
				if ok {
					sm.mu.Lock()
					if op.Oxnum >= sm.clerkop[op.Clerkid] { //已经被提交的command
						switch op.Optype {
						case operation_join:

						case operation_leave:

						case operation_move:

						case operation_query:
						}
						sm.clerkop[op.Clerkid]=op.Oxnum
					}
					ch, ok := sm.clerkchan[op.Opid]
					if ok {
						select {
						case <-ch:
						default:
						}
						sm.clerkchan[op.Opid] <- string(op.Opid)
					} else {
						sm.clerkchan[op.Opid] = make(chan string, 1)
					}
					sm.mu.Unlock()
				}
			}
		}
	}()


	// Your code here.

	return sm
}
