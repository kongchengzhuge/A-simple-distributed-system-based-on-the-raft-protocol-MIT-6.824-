package raftkv

import (
	"labrpc"
	"sync"
	"sync/atomic"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu	sync.Mutex
	clerkname	int64	//客户端id，用于让kvserver辨别
	Ops		int32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkname=nrand()
	ck.Ops=0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	//fmt.Printf("ck %d 准备执行get\n",ck.clerkname)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args:=GetArgs{}
	reply:=GetReply{}
	args.Key=key
	args.Clerkid=ck.clerkname
	args.Opid=nrand()
	args.Oxnum=ck.Ops
	atomic.AddInt32(&ck.Ops,1)
	i:=0

	//fmt.Printf("现在是clerk%d在执行get,opid是%d,Oxnum是%d,key是%s\n",ck.clerkname,args.Opid,args.Oxnum,args.Key)
	for{
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
		if ok&&reply.Err==OK{
			return reply.Value
		}else {
			i=(i+1)%len(ck.servers)
		}
	}
	// You will have to modify this function.

	//fmt.Printf("现在是clerk%d准备返回get,opid是%d,Oxnum是%d,key是%s,value是%s,现在ops是%d\n",ck.clerkname,args.Opid,args.Oxnum,args.Key,reply.Value,ck.Ops)

}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	//fmt.Printf("ck %d 准备执行put\n",ck.clerkname)
	ck.mu.Lock()
	defer ck.mu.Unlock()
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Clerkid = ck.clerkname
	args.Op = op
	args.Opid = nrand()
	args.Oxnum = ck.Ops
	atomic.AddInt32(&ck.Ops, 1)
	i := 0

	//fmt.Printf("现在是clerk%d在执行putappend,Oxnum是%d,key是%s,value是%s\n",ck.clerkname,args.Oxnum,args.Key,args.Value)
	for {
		ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
		//fmt.Println(reply)
		if ok && reply.Err == OK {
			return
		} else {
			i = (i + 1) % len(ck.servers)
			//fmt.Println("wrongleader and try again!")
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
