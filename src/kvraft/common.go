package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
	Alreaddid = "Alreaddid"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Clerkid	int64
	Opid	int64
	Oxnum	int32
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Clerkid	int64
	Oxnum	int32
	Opid	int64
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}
