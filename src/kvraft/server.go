package raftkv

import (
	"bytes"
	"labgob"
	"labrpc"
	"log"
	"raft"
	"sync"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Operation_type int

const(
	operation_get Operation_type = iota
	operation_put
	operation_append
)


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Optype	Operation_type		//来标注操作的类型
	Key		string
	Value	string
	Oxnum	int32				//用来标注操作的次数
	Clerkid int64				//操作的客户端
	Opid	int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.


	kvdatebase	map[string]string
	clerkop		map[int64]int32
	clerkchan   map[int64]chan string		//为每一个客户端定一个chan传递已经提交的信息
	isleaderchan map[int64]chan bool		//如果不是leader，则传送消息让RPC重新调用
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {

	cmd:=Op{Optype:operation_get,Key:args.Key,Value:"",Clerkid:args.Clerkid,Oxnum:args.Oxnum,Opid:args.Opid}
	_,_,isleader:=kv.rf.Start(cmd)

	if isleader==false{
		reply.Err=""
		reply.WrongLeader=true
		return
	}


	kv.mu.Lock()
	//if _,ok:=kv.clerkop[args.Clerkid];!ok{
	//	kv.clerkop[args.Clerkid]=0
	//}
	//fmt.Println("在等创建mapchan锁")
	ch , ok:=kv.clerkchan[args.Opid]
	if !ok{
		kv.clerkchan[args.Opid]=make(chan string,1)
		//kv.isleaderchan[args.Clerkid]=make(chan bool)
		ch=kv.clerkchan[args.Opid]
	}
	kv.mu.Unlock()


	select {
		case <-ch:
			//fmt.Printf("Opid为%d 的 get[%s]成功完成,结果是%s\n",args.Opid,args.Key,reply.Value)
			reply.WrongLeader=false
			reply.Err=OK
			kv.mu.Lock()
			reply.Value=kv.kvdatebase[args.Key]
			kv.mu.Unlock()
			//fmt.Println(kv.kvdatebase)
		case <-time.After(1000*time.Millisecond):
			//fmt.Println("失去控制权")
			reply.WrongLeader=true
	}
	//fmt.Println("选择之后")
	//注意这里还有可能最后失去leader控制，还需要一个channel来传递消息
	//fmt.Printf("Opid为%d 的 get[%s]准备return,结果是%s\n",args.Opid,args.Key,reply.Value)
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {

	var cmd Op
	if args.Op=="Put" {
		cmd = Op{Optype:operation_put, Key: args.Key, Value: args.Value, Clerkid: args.Clerkid, Oxnum: args.Oxnum,Opid:args.Opid}
	}else{
		cmd = Op{Optype:operation_append, Key: args.Key, Value: args.Value, Clerkid: args.Clerkid, Oxnum: args.Oxnum,Opid:args.Opid}
	}


	_,_,isleader:=kv.rf.Start(cmd)

	if isleader==false{
		reply.Err=""
		reply.WrongLeader=true
		return
	}

	kv.mu.Lock()
	//if _,ok:=kv.clerkop[args.Clerkid];!ok{
	//	kv.clerkop[args.Clerkid]=0
	//}
	ch , ok:=kv.clerkchan[args.Opid];
	if !ok{
		kv.clerkchan[args.Opid]=make(chan string,1)
		//kv.isleaderchan[args.Clerkid]=make(chan bool)
		ch=kv.clerkchan[args.Opid]
	}
	kv.mu.Unlock()

	select {
	case <-ch:
		reply.WrongLeader=false
		reply.Err=OK

	case <-time.After(1000*time.Millisecond):
		reply.WrongLeader=true
	}
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.kvdatebase=make(map[string]string)
	kv.clerkop=make(map[int64]int32)
	kv.clerkchan=make(map[int64]chan string)
	kv.isleaderchan=make(map[int64]chan bool)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)


	go func() {
		for {
			//fmt.Printf("现在是kvraft[%d]\n",kv.me)
			applymsg := <-kv.applyCh
			//fmt.Printf("现在是kvraft[%d]收到applymsg消息\n",kv.me)
			if applymsg.CommandValid{
				op,ok:=(applymsg.Command).(Op)
				if ok{
					kv.mu.Lock()
					if op.Oxnum>=kv.clerkop[op.Clerkid]{//已经被提交的command
						switch op.Optype {
						case operation_append:
							kv.kvdatebase[op.Key] = kv.kvdatebase[op.Key] + op.Value
						case operation_put:
							kv.kvdatebase[op.Key] = op.Value
						}
						kv.clerkop[op.Clerkid]++
					}
					ch, ok := kv.clerkchan[op.Opid]
					//fmt.Println(ok)
					if ok {
						select {
						case <-ch:
						default:
						}
						//fmt.Println("出来")
						kv.clerkchan[op.Opid] <- string(op.Opid)
						//fmt.Println("出来2")
					} else {
						//fmt.Printf("kv[%d]创建chan\n",kv.me)
						kv.clerkchan[op.Opid] = make(chan string, 1)
					}
					kv.mu.Unlock()
				}

				//fmt.Printf("Raft[%d] statesize: %d\n",kv.me,kv.rf.Statesize())
				if kv.maxraftstate!=-1&&(kv.maxraftstate<=kv.rf.Statesize()) {
					//fmt.Printf("kv[%d]开始snapshot\n",kv.me)
					w:=new(bytes.Buffer)
					e:=labgob.NewEncoder(w)
					e.Encode(kv.kvdatebase)
					e.Encode(kv.clerkop)
					lastIndex:=applymsg.CommandIndex

					data:=w.Bytes()
					//fmt.Printf("kv[%d]'s data size is %d\n",kv.me,len(data))
					go kv.rf.Startsnapshot(lastIndex,data)
				}
			}else{
				//这里是处理快照信息
				var LastIndex int
				var LastTerm  int

				data:=applymsg.Snapshot

				r:=bytes.NewBuffer(data)
				d:=labgob.NewDecoder(r)

				d.Decode(&LastIndex)
				d.Decode(&LastTerm)
				kv.mu.Lock()
				kv.kvdatebase=make(map[string]string)
				kv.clerkop=make(map[int64]int32)
				d.Decode(&kv.kvdatebase)
				d.Decode(&kv.clerkop)
				kv.mu.Unlock()
				//fmt.Println(kv.kvdatebase)
			}
		}
	}()


	// You may need initialization code here.
	return kv
}
