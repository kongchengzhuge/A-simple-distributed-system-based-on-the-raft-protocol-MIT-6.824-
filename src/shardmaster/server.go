package shardmaster

import (
	"fmt"
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
	Gid	int						//for op move
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

	fmt.Printf("now sm[%d] join %d begin\n",sm.me,args.Opid)

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
			fmt.Printf("now sm[%d] join %d done\n",sm.me,args.Opid)
			fmt.Println(sm.configs[len(sm.configs)-1].Shards)
		case <-time.After(1000*time.Millisecond):
			reply.WrongLeader=true
			reply.Err="outoftime"
	}
}

func (sm *ShardMaster) hand_join(cf_now Config,servers map[int][]string) Config{
	cf_res:=Config{Num:cf_now.Num+1}
	//添加新的gidmap
	ma:=make(map[int][]string)
	//计数map，为之后均匀分配做准备
	evenly_map:=make(map[int]int)
	var tlk	int
	for k,v:=range servers{
		tlk=k
		ma[k]=v
		evenly_map[k]=0
	}
	for k, v := range cf_now.Groups {
		ma[k] = v
		evenly_map[k] = 0
	}
	cf_res.Groups=ma
	//添加新的完成

	//初始化新的shards，并且将evenlymap的值相应的写入，方便之后均匀分配
	var sh [NShards]int
	if cf_now.Shards[0]==0{	//第一次join，改变
		for i:=0;i<10;i++{
			sh[i]=tlk
		}
		evenly_map[tlk]=10
	}else{
		for i,v:=range cf_now.Shards{
			sh[i]=v
			evenly_map[v]++
		}
	}
	//初始化完成

	//准备开始分配，将拥有最多shards的gid分配给拥有最少shards的gid，直到最多shards gid只比最少的多1，就达到均匀的目的了
	for;;{
		max:=0
		min:=10000
		var maxkey int
		var minkey int
		for k,v:=range evenly_map{
			if v>max{
				max=v
				maxkey=k
			}
			if v<min{
				min=v
				minkey=k
			}
		}
		gap:=max-min	//如果这里的gap大于等于2，将maxkey中的shard减一，minkey的shard加一就行
		if gap>=2{
			for i:=0;i<10;i++{
				if sh[i]==maxkey{
					sh[i]=minkey
					evenly_map[maxkey]--
					evenly_map[minkey]++
					break
				}
			}
		}else{
			break
		}
	}
	cf_res.Shards=sh
	return cf_res
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	cmd:=Op{Optype:operation_leave,GIDs:args.GIDs,Clerkid:args.Clerkid,Oxnum:args.Oxnum,Opid:args.Opid}
	_,_,isleader:=sm.rf.Start(cmd)

	if isleader==false{
		reply.Err=""
		reply.WrongLeader=true
		return
	}

	fmt.Printf("now sm[%d] leave %d begin\n",sm.me,args.Opid)

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
		fmt.Printf("now sm[%d] leave %d done\n",sm.me,args.Opid)
		fmt.Println(sm.configs[len(sm.configs)-1].Shards)
		reply.WrongLeader=false
	case <-time.After(1000*time.Millisecond):
		reply.WrongLeader=true
		reply.Err="outoftime"
	}
}

func (sm *ShardMaster) hand_leave(cf_now Config,gids []int) Config{
	cf_res:=Config{Num:cf_now.Num+1}
	//添加新的gidmap
	ma:=make(map[int][]string)
	//计数map，为之后均匀分配做准备
	evenly_map:=make(map[int]int)
	for k, v := range cf_now.Groups {
		ma[k] = v
		evenly_map[k] = 0
	}

	for _,v:=range(gids){
		delete(ma,v)
	}

	cf_res.Groups=ma
	//添加新的完成

	//初始化新的shards，并且将evenlymap的值相应的写入，方便之后均匀分配
	var sh [NShards]int
	for i,v:=range cf_now.Shards {
		sh[i] = v
		evenly_map[v]++
	}
	//初始化完成,然后在evenly_map中删除几个删除的键
	for _,v:=range(gids){
		delete(evenly_map,v)
	}
	//删除完成
	//开始交换shards
	for;;{
		min:=10000
		var minkey int
		for k,v:=range evenly_map{
			if v<min{
				min=v
				minkey=k
			}
		}
		var i int
		for i=0;i<len(sh);i++{
			j:=0
			for ;j<len(gids);j++{
				if sh[i]==gids[j]{
					break
				}
			}
			if j==len(gids){//当前gid不是要被删除的gid
				continue
			}
			sh[i]=minkey
			evenly_map[minkey]++
			break		//当前是需要删除的，将指向其的shard重新指向最小的gid
		}
		if i==len(sh){//所有的都清理完毕，退出结束
			break
		}
	}
	cf_res.Shards=sh
	return cf_res
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	cmd:=Op{Optype:operation_move,Shard:args.Shard,Gid:args.GID,Clerkid:args.Clerkid,Oxnum:args.Oxnum,Opid:args.Opid}
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

func (sm *ShardMaster) hand_move(cf_now Config,gid int,shard int) Config{
	cf_res:=Config{Num:cf_now.Num+1}
	//添加新的gidmap
	ma:=make(map[int][]string)
	for k, v := range cf_now.Groups {
		ma[k] = v
	}
	cf_res.Groups=ma
	//添加新的完成

	//初始化新的shards
	var sh [NShards]int
	for i,v:=range cf_now.Shards {
		sh[i] = v
	}
	sh[shard]=gid
	cf_res.Shards=sh
	return cf_res
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	cmd:=Op{Optype:operation_query,Num:args.Num,Clerkid:args.Clerkid,Oxnum:args.Oxnum,Opid:args.Opid}
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
		if args.Num==-1||args.Num>=len(sm.configs) {
			reply.Config = sm.configs[len(sm.configs)-1]
		}else{
			reply.Config = sm.configs[args.Num]
		}
	case <-time.After(1000*time.Millisecond):
		reply.WrongLeader=true
		reply.Err="outoftime"
	}
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
	sm.clerkop=make(map[int64]int)
	sm.clerkchan=make(map[int64]chan string)

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
							config_new:=sm.hand_join(sm.configs[len(sm.configs)-1],op.Servers)
							sm.configs=append(sm.configs,config_new)
						case operation_leave:
							config_new:=sm.hand_leave(sm.configs[len(sm.configs)-1],op.GIDs)
							sm.configs=append(sm.configs,config_new)
						case operation_move:
							config_new:=sm.hand_move(sm.configs[len(sm.configs)-1],op.Gid,op.Shard)
							sm.configs=append(sm.configs,config_new)
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
					sm.clerkop[op.Clerkid]=op.Oxnum
					sm.mu.Unlock()
				}
			}
		}
	}()


	// Your code here.

	return sm
}
