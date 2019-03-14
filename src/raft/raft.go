package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	//"fmt"
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that Successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	Snapshot	[]byte
}

type State int

// iota 初始化后会自动递增
const (
	idn       State = iota // value --> 0
	leader                 // value --> 1
	candidate              // value --> 2
	follower               // value --> 3
)

//
// A Go object implementing a single Raft peer.
//


type Log struct {
	Command interface{}
	Term    int
	Index   int
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]


	//对于每一种状态，都有几种不同的情况，建立几个channel可以达到传送消息的目的

	leaderchan    chan bool
	votechan      chan bool
	heartbeatchan chan bool //传递心跳
	ApplyMsgchan  chan ApplyMsg
	commitchan    chan bool
	applyCh       chan ApplyMsg

	currentTerm int
	voteFor     int
	logs        []Log
	CommitIndex int   //index of highest log entry known to be commited
	lastApplied int   //index of highest log entry applied to be commited
	nextIndex   []int //index of next log entry
	matchIndex  []int //index of highest log entry known to be replicated

	votes    int //count all of the vote
	rdsource rand.Source

	state State //there are three identity: candidate leader follower

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	term = rf.currentTerm
	isleader = (rf.state == leader)
	// Your code here (2A).
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
 	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w:=new(bytes.Buffer)
	e:=labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	date:=w.Bytes()
	//save raft state


	rf.persister.SaveRaftState(date)
}




//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r:=bytes.NewBuffer(data)
	d:=labgob.NewDecoder(r)

	d.Decode(&rf.currentTerm)
	d.Decode(&rf.voteFor)
	d.Decode(&rf.logs)
}




type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func newlog(candidateTerm int,raftTerm int,candidateIndex int,raftIndex int) bool {//define to determine which one is new
	if(candidateTerm>raftTerm){
		return true
	}else if raftTerm==candidateTerm{
		if candidateIndex>=raftIndex{
			return true
		}
	}
	return false
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	//fmt.Printf("this is %d and I receive %d\n", rf.me, args.CandidateId)
	////fmt.Printf("Raft[%d]Request加锁",rf.me)
	rf.mu.Lock()
	//defer //fmt.Printf("Raft[%d]Request解锁\n",rf.me)
	defer rf.mu.Unlock()
	////fmt.Printf("this is %d and I receive %d", rf.me, args.CandidateId)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false

		return
	}

	//fmt.Printf("I can vote")

	rf.votechan <- true

	if rf.currentTerm < args.Term {
		rf.voteFor = -1
		rf.state = follower
		rf.currentTerm = args.Term
	}

	if (rf.voteFor == -1 || rf.voteFor == args.CandidateId) &&	newlog(args.LastLogTerm,rf.logs[len(rf.logs)-1].Term,args.LastLogIndex,rf.logs[len(rf.logs)-1].Index){
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	}
	//fmt.Printf("this is %d and I vote %t\n", rf.me, reply.VoteGranted)

}
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	//fmt.Printf("Raft[%d]sendRequest,",rf.me)
	//fmt.Println(ok)
	//defer //fmt.Printf("Raft[%d]sendRequest解锁\n",rf.me)
	defer rf.mu.Unlock()
	defer rf.persist()
	if ok {
		if rf.state != candidate {
			return ok
		}

		if rf.currentTerm != args.Term {
			return ok
		}

		if rf.currentTerm < reply.Term {
			rf.state = follower
			rf.voteFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
		}

		if reply.VoteGranted {
			rf.votes++
			if rf.state == candidate && rf.votes > len(rf.peers)/2 {
				rf.leaderchan <- true
				rf.state = leader
			}
		}

	}
	return ok
}

func (rf *Raft) sendRequestVoteAll() {
	//fmt.Printf("this is %d and I send all requestvote\n", rf.me)
	rf.mu.Lock()
	//fmt.Printf("Raft[%d]sendRequestAll加锁,",rf.me)
	args := &RequestVoteArgs{}
	args.Term = rf.currentTerm

	args.LastLogIndex = rf.logs[len(rf.logs) - 1].Index
	args.LastLogTerm = rf.logs[len(rf.logs) - 1].Term

	args.CandidateId = rf.me
	rf.mu.Unlock()
	//fmt.Printf("Raft[%d]sendRequest解锁\n",rf.me)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(i int) {
			reply := &RequestVoteReply{}
			rf.sendRequestVote(i, args, reply)
		}(i)
	}
}


//
// example AppendEntrisArgs RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeadId       int
	PrevLogIndex int
	PrevLogTerm   int
	Entris       []Log
	LeaderCommit int //leader's CommitIndex
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	Term        int
	Success     bool //true if follower contained entry matching PrevLogIndex and prevLogTerm
	CommitIndex int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//fmt.Printf("Raft[%d]AppendEntries加锁,",rf.me)
	//defer fmt.Printf("Raft[%d]AppendEntries解锁\n",rf.me)
	defer rf.mu.Unlock()
	defer rf.persist()

	//fmt.Printf("Raft[%d]AppendEntries from Raft[%d],and It's log's length is %d\n",rf.me,args.LeadId,len(rf.logs))

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.CommitIndex = 0
		return
	}
	////fmt.Println("I am done0")
	rf.heartbeatchan <- true //表示收到心跳信息，可以将时间重置为零
	////fmt.Println("I am done1")
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.state = follower
		rf.voteFor = -1
	}
	////fmt.Println("I am done2")

	//fmt.Printf("Raft[%d] 's logs size is :%d\n",rf.me,len(rf.logs))
	//对于index的处理还是有许多问题的

	if args.PrevLogIndex > rf.logs[len(rf.logs)-1].Index {
		reply.CommitIndex = rf.logs[len(rf.logs)-1].Index
		return
	}

	baseindex:=rf.logs[0].Index

	if args.PrevLogIndex > baseindex {
		term := rf.logs[args.PrevLogIndex-baseindex].Term
		if args.PrevLogTerm != term {
			for i := args.PrevLogIndex - 1 ; i >= baseindex; i-- {
				if rf.logs[i-baseindex].Term != term {
					reply.CommitIndex = i
					break
				}
			}
			return
		}
	}


	if args.PrevLogIndex>=baseindex {
		rf.logs = rf.logs[:args.PrevLogIndex+1-baseindex]
		rf.logs = append(rf.logs, args.Entris...)
		//fmt.Printf("改变Raft[%d]的log到长度为%d，保留自己的从0到%d\n",rf.me,len(rf.logs),args.PrevLogIndex)
		reply.CommitIndex = rf.logs[len(rf.logs)-1].Index
		reply.Success = true
	}

	if args.LeaderCommit>rf.CommitIndex{
		if args.LeaderCommit>rf.logs[len(rf.logs)-1].Index{
			rf.CommitIndex=rf.logs[len(rf.logs)-1].Index
		}else{
			rf.CommitIndex=args.LeaderCommit
		}
		go rf.commitLogs()
	}


}

func (rf *Raft) commitLogs() {
	//fmt.Printf("Raft[%d] start to commit Logs\n",rf.me)
	rf.mu.Lock()
	//fmt.Printf("Raft[%d]commitLogs加锁,",rf.me)
	//defer fmt.Printf("Raft[%d]commitLogs解锁\n",rf.me)
	defer rf.mu.Unlock()

	//fmt.Printf("Raft[%d]commitLogs,and it's CommitIndex is %d and lastapplied is %d\n",rf.me,rf.CommitIndex,rf.lastApplied)
	if rf.CommitIndex > rf.logs[len(rf.logs)-1].Index {
		rf.CommitIndex = rf.logs[len(rf.logs)-1].Index
	}

	baseIndex:=rf.logs[0].Index

	for i := rf.lastApplied+1; i <= rf.CommitIndex; i++ {
		//fmt.Println(i)
		rf.applyCh <- ApplyMsg{Command: rf.logs[i-baseIndex].Command, CommandIndex: i, CommandValid: true}
		//fmt.Println(i)
	}
	rf.lastApplied = rf.CommitIndex
	//fmt.Printf("Raft[%d]commitLogs, done \n",rf.me)
}

func (rf *Raft) Getstate() int {
	if rf.state == leader {
		return 1
	} else if rf.state == candidate {
		return 2
	} else {
		return 3
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//fmt.Printf("Raft[%d] sendAppentri加锁\n",rf.me)
	rf.mu.Lock()
	//fmt.Printf("Raft[%d]sendAppendEntries,\n",rf.me)
	//defer //fmt.Printf("Raft[%d]sendAppendEntries解锁\n",rf.me)
	defer rf.mu.Unlock()
	//defer fmt.Printf("Raft[%d] sendAppentri解锁\n",rf.me)
	if ok{
		if reply.Term > rf.currentTerm {
			//fmt.Println("1")
			//fmt.Printf("leader Raft[%d] is out of date,and it will become follower and reelection\n",rf.me)
			rf.state = follower
			rf.voteFor = -1
			rf.currentTerm = reply.Term
			rf.persist()
			return ok
		}
		baseIndex:=rf.logs[0].Index
		if reply.Success==false {
			//fmt.Println("2")
			rf.nextIndex[server] = reply.CommitIndex + 1
		}else{

			if reply.CommitIndex==0{//heartbeat，没有实际意义
				return ok
			}

			//fmt.Println("3")
			if len(args.Entris) >0 {
				rf.nextIndex[server] = args.Entris[len(args.Entris)-1].Index + 1
				rf.matchIndex[server] = reply.CommitIndex

				count := 1

				for i := 0; i < len(rf.peers); i++ {
					if rf.me == i {
						continue
					}
					if rf.matchIndex[i] >= rf.matchIndex[server] {
						count++
					}
				}
				//fmt.Println("4")
				if rf.matchIndex[server]<baseIndex||(rf.matchIndex[server]-baseIndex)>=len(rf.logs){
					return ok
				}

				if count > len(rf.peers)/2 &&
					rf.CommitIndex < rf.matchIndex[server] &&
					rf.currentTerm == rf.logs[rf.matchIndex[server]-baseIndex].Term {
					rf.CommitIndex = rf.matchIndex[server]

					//fmt.Printf("now leader's CommitIndex is changed to %d\n",rf.CommitIndex)

					go rf.commitLogs()
				}
			}
		}
	}
	//fmt.Print(ok)
	//fmt.Println(reply.Success)
	return ok
}

func (rf *Raft) sendAppendEntriesAll() {
	//fmt.Printf("Raft[%d]sendAppendEntriesAll,and It's log's length is %d\n,",rf.me,len(rf.logs))
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me&&rf.state==leader {

			baseIndex:=rf.logs[0].Index

			//fmt.Printf("now baseIndex is %d , matchIndex is %d\n",baseIndex,rf.matchIndex[i])
			if rf.nextIndex[i]<=baseIndex { //这时候应该发送快照了

				baseIndex := rf.logs[0].Index
				args := &InstallSnapshotArgs{}
				args.Term = rf.currentTerm
				args.LastIncludeIndex = baseIndex
				args.LastIncludeTerm = rf.logs[0].Term
				args.LeaderId = rf.me
				args.Data = rf.persister.ReadSnapshot()
				reply := &InstallSnapshotReply{}

				if rf.state == leader {
					go func(i int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
						rf.sendInstallSnapshot(i, args, reply)
					}(i, args, reply)
				}

			}else {

				//fmt.Printf("Raft[%d]sendAppendEntriesAll加锁,",rf.me)

				baseIndex := rf.logs[0].Index
				args := &AppendEntriesArgs{}
				args.Term = rf.currentTerm
				args.LeaderCommit = rf.CommitIndex
				//fmt.Printf("nextIndex is %d\n", rf.nextIndex[i])
				args.PrevLogIndex = rf.nextIndex[i] - 1
				//fmt.Printf("now In Raft[%d], raft[%d] 's prevlogindex:%d, len(rf.logs):%d, baseIndex:%d\n", rf.me, i, args.PrevLogIndex, len(rf.logs), baseIndex)
				args.PrevLogTerm = rf.logs[args.PrevLogIndex-baseIndex].Term
				args.LeadId = rf.me
				args.Entris = rf.logs[rf.nextIndex[i]-baseIndex:]

				//if len(args.Entris) == 0 {
				//	fmt.Println("Entris is nil")
				//} else {
				//	//fmt.Printf("Entris's size is %d,and args.PrevLogTerm is %d\n",len(args.Entris),args.PrevLogIndex)
				//	fmt.Println("Entris is not nil")
				//}

				//fmt.Printf("Raft[%d]sendAppendEntriesAll解锁\n",rf.me)
				reply := &AppendEntriesReply{}
				if rf.state == leader {
					go func(i int,args *AppendEntriesArgs,reply *AppendEntriesReply) {
						rf.sendAppendEntries(i, args, reply)
					}(i,args,reply)
				}

			}

		}
	}
}
//snapshot
type InstallSnapshotArgs struct {
	// Your data here (2A, 2B).
	Term int	//leader's term
	LeaderId	int
	LastIncludeIndex	int
	LastIncludeTerm	int
	Data	[]byte
}

type InstallSnapshotReply struct {
	Term        int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	//fmt.Printf("now Raft[%d] will InstallSnapshot加锁\n",rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//defer fmt.Printf("now Raft[%d] InstallSnapshot done解锁\n",rf.me)
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}


	rf.logs = truncated(args.LastIncludeIndex,args.LastIncludeTerm, rf.logs)
	rf.persist()

	rf.lastApplied=args.LastIncludeIndex
	rf.CommitIndex=args.LastIncludeIndex

	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.Data) //将当前状态值存进来

	//fmt.Printf("now Raft[%d] will InstallSnapshot sendmessage加锁\n",rf.me)
	applymes := ApplyMsg{CommandValid: false, Snapshot: args.Data}
	rf.applyCh <- applymes
	//fmt.Printf("now Raft[%d] will InstallSnapshot sendmessage解锁\n",rf.me)
}

func truncated(LastIncludeIndex int,LastIncludeTerm int,log []Log) []Log{
	var newLogEntries []Log
	newLogEntries = append(newLogEntries, Log{Index:LastIncludeIndex,Term:LastIncludeTerm})

	for index := len(log) - 1; index >= 0; index-- {
		if log[index].Index == LastIncludeIndex && log[index].Term == LastIncludeTerm {
			newLogEntries = append(newLogEntries, log[index+1:]...)
			break
		}
	}

	return newLogEntries
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool{
	//fmt.Printf("now Raft[%d] will sendInstallSnapshot to Raft[%d] 加锁\n",rf.me,server)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("now Raft[%d]  sendInstallSnapshot to Raft[%d] done解锁\n",rf.me,server)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if ok{
		if reply.Term>rf.currentTerm{
			if reply.Term > rf.currentTerm {
				//fmt.Printf("leader Raft[%d] is out of date,and it will become follower and reelection\n",rf.me)
				rf.state = follower
				rf.voteFor = -1
				rf.currentTerm = reply.Term
				rf.persist()
				return ok
			}
		}

		rf.nextIndex[server]=args.LastIncludeIndex+1
		rf.matchIndex[server]=args.LastIncludeIndex		//成功送出快照之后，需要将当前的nextint和matchint更新一下
		//之后发消息就不能直接使用log的下标值了，需要使用log内存下的logIndex
	}
	return ok
}

func (rf *Raft) Statesize() int{
	return rf.persister.RaftStateSize()
}

func (rf *Raft) Startsnapshot(lastIncludeIndex int,data []byte){
	//fmt.Printf("now Raft[%d] start to snapshot加锁\n",rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//defer fmt.Printf("now Raft[%d] start to snapshot done解锁\n",rf.me)
	//fmt.Printf("now start to snapshot 解锁\n")
	baseIndex:=rf.logs[0].Index
	endIndex:=rf.logs[len(rf.logs)-1].Index

	LastIncludeIndex:=lastIncludeIndex
	//fmt.Printf("lastIndex is:%d,baseIndex is:%d\n",lastIncludeIndex,baseIndex)
	if lastIncludeIndex<=baseIndex||lastIncludeIndex>endIndex{
		return
	}
	LastIncludeTerm:=rf.logs[lastIncludeIndex-baseIndex].Term

	//fmt.Printf("now Raft[%d] start to truncated,and now logsize is %d\n",rf.me,len(rf.logs))
	rf.logs=truncated(LastIncludeIndex,LastIncludeTerm,rf.logs)
	//fmt.Printf("now Raft[%d] done truncated,and now logsize is %d\n",rf.me,len(rf.logs))


	w:=new(bytes.Buffer)
	e:=labgob.NewEncoder(w)
	e.Encode(LastIncludeIndex)
	e.Encode(LastIncludeTerm)
	d:=w.Bytes()
	d=append(d,data...)
	//fmt.Printf("开始snapshot LastIndex:%d,LastTerm:%d\n",LastIncludeIndex,LastIncludeTerm)
	rf.persist()
	pre:=rf.persister.ReadRaftState()

	rf.persister.SaveStateAndSnapshot(pre,d)
	//fmt.Printf("Raft[%d] truncated done\n",rf.me)
}

func (rf *Raft) readSnapshot(data []byte){
	//fmt.Printf("Raft[%d] 冲突之后恢复\n",rf.me)
	rf.readPersist(rf.persister.ReadRaftState())
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	var LastIndex int
	var LastTerm int

	r:=bytes.NewBuffer(data)
	d:=labgob.NewDecoder(r)

	d.Decode(&LastIndex)
	d.Decode(&LastTerm)

	//fmt.Printf("冲突恢复之后 LastIndex:%d,LastTerm:%d\n",LastIndex,LastTerm)

	rf.CommitIndex=LastIndex
	rf.lastApplied=LastIndex

	//log.Printf("rf[%d] send message\n",rf.me	)
	applymsg:=ApplyMsg{CommandValid:false,Snapshot:data}
	go func() {
		rf.applyCh<-applymsg
	}()
	//log.Printf("rf[%d] done message\n",rf.me	)
}


//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	if rf.state != leader {
		return index, term, false
	}

	//fmt.Printf("now rf.[%d]start command",rf.me)
	//fmt.Println(command)

	index = rf.logs[len(rf.logs)-1].Index+1

	//fmt.Printf("now index is %d\n",index)
	term = rf.currentTerm
	log := Log{}
	log.Index = index
	log.Command = command
	log.Term = term
	rf.logs = append(rf.logs, log)
	rf.persist()

	return index, term, isLeader
}



//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = follower
	rf.voteFor = -1
	rf.heartbeatchan=make(chan bool,100)
	//rf.commitchan=make(chan bool,100)
	rf.leaderchan = make(chan bool, 100)
	rf.votechan = make(chan bool, 100)
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.logs = append(rf.logs, Log{Index: 0, Term: 0,Command:0})
	rf.lastApplied=0
	rf.CommitIndex=0

	rf.readPersist(persister.ReadRaftState())
	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash

	//start a election time out check
	//log.Printf("Raft[%d] is restart and will read snapshot\n",rf.me)
	rf.readSnapshot(persister.ReadSnapshot())

	go func() {
		for {
			//fmt.Printf("now raft[%d]'s state is %d\n", rf.me, rf.state)
			switch rf.state {
			case follower:
				select {
				case <-rf.heartbeatchan:
					//fmt.Printf("Raft[%d]收到了心跳信息\n",rf.me)
				case <-rf.votechan:
				case <-time.After(time.Duration(rand.Int63()%500+300) * time.Millisecond):
					rf.state = candidate
				}
			case candidate:
				/*在等待投票期间，candidate 可能会收到另一个声称自己是 leader 的服务器节点发来的 AppendEntries RPC 。
				如果这个 leader 的任期号（包含在RPC中）不小于 candidate 当前的任期号，
				那么 candidate 会承认该 leader 的合法地位并回到 follower 状态。
				如果 RPC 中的任期号比自己的小，那么 candidate 就会拒绝这次的 RPC 并且继续保持 candidate 状态。
				*/
				//fmt.Printf("this is %d and I become a candidate\n", rf.me)
				rf.mu.Lock()
				//fmt.Printf("Raft[%d]变成candidate",rf.me)
				rf.currentTerm++
				rf.voteFor = me
				rf.votes = 1
				rf.persist()
				rf.mu.Unlock()
				//fmt.Printf("Raft[%d]变成candidate解锁\n",rf.me)
				//fmt.Printf("this is %d and I will sendRequestVote\n", rf.me)
				go rf.sendRequestVoteAll()
				select {
				case <-time.After(time.Duration(rand.Int63()%500+300) * time.Millisecond):
				case <-rf.heartbeatchan:
					rf.state = follower
				case <-rf.leaderchan:
					rf.mu.Lock()
					//fmt.Printf("Raft[%d]变成leader,\n",rf.me)
					rf.state = leader
					rf.nextIndex = make([]int, len(rf.peers))
					rf.matchIndex = make([]int, len(rf.peers))
					for i := 0; i < len(rf.peers); i++ {
						rf.nextIndex[i] = rf.logs[len(rf.logs)-1].Index+1
						rf.matchIndex[i] = 0
					}
					rf.mu.Unlock()
					//fmt.Printf("Raft[%d] become leader\n",rf.me)
				}

			case leader:
				rf.sendAppendEntriesAll()
				time.Sleep(50 * time.Millisecond)
			}
		}
	}()
	return rf
}
