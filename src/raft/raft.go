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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

// 角色常量
const (
	Follower  int = 1
	Candidate int = 2
	Leader    int = 3
)

// 心跳超时，要求1秒10次，所以是100ms一次
const HEART_BEAT_TIMEOUT = 100

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int        //latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int        //candidateId that received vote in current term (or null if none)
	log         []LogEntry //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	//Volatile state on all servers:
	commitIndex int //index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int //index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	//Volatile state on leaders:(Reinitialized after election)
	nextIndex  []int //for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	// 辅助数据
	heartbeatTimer *time.Timer   // 心跳定时器
	role           int           // 角色
	applyCh        chan ApplyMsg // 提交通道
	cond           *sync.Cond    //条件变量，用于通知applier

	// (2D)
	// 发送SnapShot后需要截断日志，但raft的commitIndex和lastApplied等还是全局索引
	//Even when the log is trimmed,
	//your implemention still needs to properly send the term
	//and index of the entry prior to new entries in AppendEntries RPCs;
	//this may require saving and referencing
	//the latest snapshot's lastIncludedTerm/lastIncludedIndex
	//(consider whether this should be persisted).
	// 总之需要数据来存储截断日志的信息
	snapShot          []byte //存储的快照
	lastIncludedIndex int    //日志中最新条目的索引
	lastIncludedTerm  int    //日志中最新条目的Term
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.role == Leader
	return term, isleader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	// 2D,lastIncluedIndex和lastIncluedTerm也要持久化
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, rf.snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapShot []byte) {
	//不需要锁，因为只在Make中调用
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncluedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncluedTerm) != nil {
		DPrintf("server %v readPersist 失败\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		//2D
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncluedTerm
		//获得lastIncluedIndex后，更新commitIndex和lastApplied
		//都被快照了，那肯定被commit和apply了
		//这步不是必须的，但这样做，后面就不用判断索引越界了
		rf.commitIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		DPrintf("server %v readPersist 成功\n", rf.me)
	}
	if len(snapShot) == 0 {
		DPrintf("server %v readSnapshot 失败，因为无快照\n", rf.me)
		return
	}
	rf.snapShot = snapShot
	DPrintf("server %v 读取快照成功\n", rf.me)
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// Snapshot很简单, 接收service层的快照请求, 并截断自己的log数组, 但还是有几个点需要说明:
//
//	1.判断是否接受Snapshot
//	  创建Snapshot时, 必须保证其index小于等于commitIndex, 如果index大于commitIndex, 则会有包括未提交日志项的风险。快照中不应包含未被提交的日志项
//	  创建Snapshot时, 必须保证其index大于lastIncludedIndex, 因为这可能是一个重复的或者更旧的快照请求RPC, 应当被忽略
//	2.将snapshot保存,因为后续Follower可能需要snapshot, 以及持久化时需要找到snapshot进行保存, 因此此时要保存以便后续发送给Follower
//	3.除了更新lastIncludedTerm和lastIncludedIndex外, 还需要检查lastApplied是否位于Snapshot之前, 如果是, 需要调整到与index一致
//	4.调用persist持久化
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.commitIndex < index || index <= rf.lastIncludedIndex {
		//快照点落后于index或者落后于上一次快照点
		DPrintf("server %v 拒绝了 Snapshot 请求,其index=%v,自身commitIndex=%v,lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex)
		return
	}

	DPrintf("server %v 同意了 Snapshot 请求,其index=%v,自身commitIndex=%v,原来的lastIncludedIndex=%v,快照后的lastIncludedIndex=%v\n", rf.me, index, rf.commitIndex, rf.lastIncludedIndex, index)

	//截断log,这要小心截取的起点，为什么是rf.RealLogIdx(index)呢
	//index是上层service通知我们删除的日志索引，所以要把它删掉
	//因为log的0号位置不算，所以从0开始，把index开始放进去，就跟把index对应条目删掉一样
	rf.snapShot = snapshot //保存snapshot
	rIdx := rf.RealLogIdx(index)
	rf.lastIncludedTerm = rf.log[rIdx].Term
	rf.lastIncludedIndex = index //要记住截断的索引,注意不能放到前两行前
	rf.log = append([]LogEntry{{Term: rf.log[rIdx].Term}}, rf.log[rIdx+1:]...)
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //candidate’s term
	CandidateId  int //candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Candidate[raft%v][term:%v] request vote: raft%v[%v] 's term%v\n", args.CandidateId, args.Term, rf.me, rf.role, rf.currentTerm)
	// reset timer after grant vote 投票要重置，防止多余选举
	rf.heartbeatTimer.Reset(randTimeDuration())
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchRoleTo(Follower)
	}
	//初始reply，默认失败
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1.候选人落伍了，不投票
	if args.Term < rf.currentTerm {
		return
	}

	// 2.已经投给别人了，不投票
	if args.Term == rf.currentTerm && rf.votedFor != -1 {
		return
	}

	// 3.解决脑裂问题撒手锏,在这个任期里，我已经是Leader了，不能给别人投票
	if args.Term == rf.currentTerm && rf.role == Leader {
		return
	}

	lastLogIndex := rf.getLastLogIndex()                    //本日志的最后一条的索引
	lastLogTerm := rf.log[rf.RealLogIdx(lastLogIndex)].Term ////本日志的最后一条的Term
	// 4.投票限制，必须日志够新的节点才有资格当选，防止一颗老鼠屎污染一锅汤
	// 2B: candidate's vote should be at least up-to-date as receiver's log "up-to-date" is defined in thesis 5.4.1
	// Receiver is more up-to-date, does not grant vote 候选人日志落后了，不投票
	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex < lastLogIndex) {
		return
	}
	//-----------满足各种限制，可以投票--------------
	DPrintf("[选举日志][term %v] server %v 投票 %v\n", rf.currentTerm, rf.me, args.CandidateId)
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.VoteGranted = true
}

type AppendEntriesArgs struct {
	Term         int        //leader’s term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm

	//Figure 8: A time sequence showing why a leader cannot determine commitment using log entries from older terms. In
	// (a) S1 is leader and partially replicates the log entry at index
	// 2. In (b) S1 crashes; S5 is elected leader for term 3 with votes
	// from S3, S4, and itself, and accepts a different entry at log
	// index 2. In (c) S5 crashes; S1 restarts, is elected leader, and
	// continues replication. At this point, the log entry from term 2
	// has been replicated on a majority of the servers, but it is not
	// committed. If S1 crashes as in (d), S5 could be elected leader
	// (with votes from S2, S3, and S4) and overwrite the entry with
	// its own entry from term 3. However, if S1 replicates an entry from its current term on a majority of the servers before
	// crashing, as in (e), then this entry is committed (S5 cannot
	// win an election). At this point all preceding entries in the log
	// are committed as well.
	ConflictTerm  int // 2C
	ConflictIndex int // 2C
}

// 接收到Leader的日志推送
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[同步日志][term:%v] %v %v收到来自term %v leader%v的日志推送\n", rf.currentTerm, rf.role, rf.me, args.Term, args.LeaderId)
	// reset election timer even log does not match args.LeaderId is the current term's Leader 就算没成功，只要收到AE，就重置计时器
	rf.heartbeatTimer.Reset(randTimeDuration())
	//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
	//我落伍了，无论我原来是什么，现在都成为Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchRoleTo(Follower)
	}
	//初始reply，默认失败
	reply.Term = rf.currentTerm
	reply.Success = false

	// 本节点对于的快照点已经超过本次日志复制的点，没必要接受此日志复制
	if rf.lastIncludedIndex > args.PrevLogIndex {
		return
	}

	// Reply false if term < currentTerm (§5.1)如果Leader的term还没我高，拒绝
	if args.Term < rf.currentTerm {
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 我的日志偏少了,那肯定不一致
	if rf.getLastLogIndex() < args.PrevLogIndex {
		// (quick fallback)
		// optimistically thinks receiver's log matches with Leader's as a subset
		// 往好的想，Follower 有可能是Leader的一个子集
		reply.ConflictIndex = rf.VirtualLogIdx(len(rf.log))
		// no conflict term  没冲突的Term
		reply.ConflictTerm = -1
		return
	}
	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	// 上一个日志条目，索引匹配，但Term不匹配，所以本次追加不能通过，因为上一个都不一致，后面追加新的也没用
	if rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {
		// (quick fallback)
		// Term冲突了，把自己最后一个节点的Term发给Leader
		reply.ConflictTerm = rf.log[rf.RealLogIdx(args.PrevLogIndex)].Term
		// expecting Leader to check the former term,so set ConflictIndex to the first one of entries in ConflictTerm
		// 初始冲突的索引就是args.PrevLogIndex，但不能排除前面的日志也冲突
		conflictIndex := args.PrevLogIndex
		// apparently, since rf.log[0] are ensured to match among all servers;ConflictIndex must be > 0, safe to minus 1
		// 上一个日志的term不符合，那么前面一样Term的日志大概率也是不对的，把conflictIndex往后退,但也要保证把conflictIndex的实际索引最低为1
		for rf.RealLogIdx(conflictIndex-1) >= 0 && rf.log[rf.RealLogIdx(conflictIndex-1)].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}
	DPrintf("[同步日志][term:%v] %v %v应用来自term %v leader%v的日志推送\n", rf.currentTerm, rf.role, rf.me, args.Term, args.LeaderId)
	// 能通过之前的判断，说明我和Leader在PrevLogIndex以及之前的日志都是一致的，那么就可以在后面添加新日志了
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	// 4. Append any new entries not already in the log compare from rf.log[args.PrevLogIndex + 1]
	rf.log = rf.log[:rf.RealLogIdx(args.PrevLogIndex+1)] //截取一致的日志，因为分片左闭右开，要保留PrevLogIndex，所以+1
	rf.log = append(rf.log, args.Entries...)             //然后把Entries追加
	rf.persist()                                         //改动了rf.log，要持久化
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 一致性判断，Leader的CommitIndex和我的commitIndex中最小值，是我的新commitIndex
	if rf.lastIncludedIndex < args.LeaderCommit && rf.commitIndex < args.LeaderCommit {
		// 如果直接等于LeaderCommit，会通不过2C实验
		rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogIndex()))
	}
	reply.Success = true
}

type InstallSnapshotArgs struct {
	Term              int         //leader’s term
	LeaderId          int         //so follower can redirect clients
	LastIncludedIndex int         //index of log entry immediately preceding new ones
	LastIncludedTerm  int         //term of prevLogIndex entry
	SnapShot          []byte      //[] raw bytes of the snapshot chunk
	LastIncluedCmd    interface{} //自己新加的字段，用于在0处占位
}

type InstallSnapshotReply struct {
	Term    int //currentTerm, for leader to update itself
	Success bool
}

// Follower收到来自Leader的InstallSnapshot后如何做
// InstallSnapshot响应需要考虑更多的边界情况:
//
//	1.如果是旧leader, 拒绝
//	2.如果Term更大, 证明这是新的Leader, 需要更改自身状态, 但不影响继续接收快照
//	3.如果LastIncludedIndex位置的日志项存在, 即尽管需要创建快照, 但并不导致自己措施日志项, 只需要截断日志数组即可
//	4.如果LastIncludedIndex位置的日志项不存在, 需要清空切片, 并将0位置构造LastIncludedIndex位置的日志项进行占位
//	5.需要检查lastApplied和commitIndex 是否小于LastIncludedIndex, 如果是, 更新为LastIncludedIndex
//	6.完成上述操作后, 需要将快照发送到service层
//	7.由于InstallSnapshot可能是替代了一次心跳函数, 因此需要重设定时器
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[Term %v]server %v 接收来自 leader %v 的InstallSnapshot", rf.currentTerm, rf.me, args.LeaderId)

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
	}

	reply.Term = rf.currentTerm
	reply.Success = false

	//1.Reply immediately if term < currentTerm 这个Leader太老了,返回false
	if args.Term < rf.currentTerm {
		DPrintf("server %v 拒绝来自 leader %v 的InstallSnapshot，原因:更小的Term, Leader的Term%v 本节点的Term %v", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		return
	}

	// 2.leader快照点小于当前提交点,返回false
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// 3.重复命令，返回true
	if args.LastIncludedIndex <= rf.lastIncludedIndex /*raft快照点要先于leader时，无需快照*/ {
		DPrintf("server %v 拒绝来自 leader %v 的InstallSnapshot，原因:落后的lastIncludedIndex，Leader发的LastIncludedIndex%v 本节点的lastIncludedIndex%v", rf.me, args.LeaderId, args.LastIncludedIndex, rf.lastIncludedIndex)
		// 2D 应该返回成功，虽然没安装快照，但我已经有了，这是幂等性
		// 这行代码看似简单，背后却是 Raft 协议的精髓之一：安全、幂等、自愈。
		// 网络会丢包 → 要能重发请求 -> 请求必须幂等，发现已经实现时，就算什么都不用执行也要返回确认，这样Leader才能正确推进
		// 节点会崩溃 → 状态必须可恢复
		reply.Success = true
		return
	}

	//----------通过限制，接收快照------------
	//强转为Follower，顺便重置计时器，注意这和AE不一样，AE不会强转
	//然后投票给发送者
	rf.switchRoleTo(Follower)
	rf.votedFor = args.LeaderId

	//6.If existing Log entry has same index and term as snapshot`s last included entry,retain log entries following it and reply
	//如果找到了匹配last included的entry，保留后续的条目，然后返回
	hasEntry := false
	rIdx := 0
	for ; rIdx < len(rf.log); rIdx++ {
		if rf.VirtualLogIdx(rIdx) == args.LastIncludedIndex && rf.log[rIdx].Term == args.LastIncludedTerm {
			//在日志里找到了lastIncluded的条目
			hasEntry = true
			break
		}
	}

	if hasEntry {
		//有，说明后续有可能有其他日志，所以要截断
		//只把已经存入snapshot的日志截走，其余部分留下
		DPrintf("server %v InstallSnapshot:args.LastIncludedIndex= %v 位置存在", rf.me, args.LastIncludedIndex)
		rf.log = rf.log[rIdx:]
	} else {
		// 7.Discard the entire log
		//没有后续日志，直接清空 把lastIncluded的日志条目存入0号占位位置
		DPrintf("server %v InstallSnapshot:清空log\n", rf.me)
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: args.LastIncluedCmd}}
	}

	// 8.Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
	rf.snapShot = args.SnapShot
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	//更新commitIndex和lastApplied，同理LastIncluedIndex肯定被commit和apply了
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.persist()

	reply.Success = true
	DPrintf("server %v 安装快照成功，快照索引为 %v 任期为%v \n", rf.me, args.LastIncludedIndex, args.LastIncludedTerm)
	//通知上层服务
	go func() {
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.SnapShot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()

}

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
// RPC 发送函数（如 sendRequestVote）应只负责“发送”和“底层通信”，不应包含业务逻辑。
// 所有 Raft 一致性协议相关的逻辑必须在调用者中处理。
// 因为职责分离原则（Separation of Concerns）
// RPC发送者是通信层，负责发起RPC ,返回是否成功（网络层面）
// 调用者如startEleciton与heartbeats是协议层，负责逻辑处理，如统计票数，转换角色等
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.role == Leader

	if isLeader {
		//如果是Leader，就把此命令转为条目，然后加入日志
		logEntry := LogEntry{Command: command, Term: term}
		rf.log = append(rf.log, logEntry)
		rf.persist()
		rf.matchIndex[rf.me] = rf.getLastLogIndex()
		rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
		index = rf.matchIndex[rf.me] //这个条目存储的索引
	}

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Ticker辅助线程，控制各节点
// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			switch rf.role {
			case Follower:
				rf.switchRoleTo(Candidate)
			case Candidate:
				rf.startElection()
				rf.heartbeatTimer.Reset(randTimeDuration()) //发起投票，重置计时器
			case Leader:
				//Leader到时了，群发心跳，并重置计时器
				rf.heartbeats()
				rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

// 辅助线程，用来apply日志条目
// 因为apply是两用的，不仅可以放日志，也可以放快照
// 先检查有没有快照，然后在检查日志
// several setters, should be called with a lock
// 一条日志如何被commit了，说明过半节点都写入了这条日志
// 而一旦一条日志被 commit，Raft 协议就保证这条日志永远不会被覆盖或删除
// commit的日志，就可以被apply了，setCommitIndex的作用就是把还没apply的命令apply掉，告诉上层应用可以执行返回结果了
func (rf *Raft) applier() {
	for rf.killed() == false {
		rf.mu.Lock()
		//防止虚假唤醒，当被唤醒时，再检查 rf.commitIndex与rf.lastApplied，如果是左大于右，那就可以出循环，然后执行
		for rf.commitIndex <= rf.lastApplied {
			rf.cond.Wait()
		}
		DPrintf("[server %d] applier: lastApplied=%d, commitIndex=%d, lastIncludedIndex=%d \n", rf.me, rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex)
		//第二个参数是长度，第三个参数是容量，注意区分
		msgs := make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex && i > rf.lastIncludedIndex; i++ {
			msgs = append(msgs, ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.RealLogIdx(i)].Command,
				CommandIndex: i,
			})
		}
		rf.mu.Unlock()
		lastApplied := rf.lastApplied
		DPrintf("[server %d] applying logs from %d to %d \n", rf.me, lastApplied+1, rf.commitIndex)
		for _, msg := range msgs {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 /*下一个apply的log一定是lastApplied+1*/ {
				rf.mu.Unlock()
				continue
			}
			rf.mu.Unlock()
			DPrintf("[server %d] apply msg: index=%d, lastApplied %v,log`s size %v   \n", rf.me, msg.CommandIndex, lastApplied, len(rf.log))
			//在发送时，先解锁再加锁，避免阻塞
			rf.applyCh <- msg
			rf.mu.Lock()

			lastApplied++
			rf.lastApplied = max(lastApplied, rf.lastApplied)
			rf.mu.Unlock()
		}
	}
}

// 切换状态，调用者需要加锁
func (rf *Raft) switchRoleTo(role int) {
	DPrintf("Term %d: server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.role, role)
	rf.role = role
	switch role {
	case Follower:
		rf.votedFor = -1
		rf.heartbeatTimer.Reset(randTimeDuration())
	case Candidate:
		// 成为候选人后立马进行选举,并重置计时器
		rf.startElection()
		rf.heartbeatTimer.Reset(randTimeDuration())
	case Leader:
		// 当选后，要重置nextIndex和matchIndex,并重置计时器
		DPrintf("[选举日志][Term %v] server %d 成功当选 \n", rf.currentTerm, rf.me)
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1 // initialized to leader last log index + 1
			rf.matchIndex[i] = rf.lastIncludedIndex    //知道lastIncluedIndex，就能快速知道matchIndex
		}
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

// 发起选举，调用者需要加锁
func (rf *Raft) startElection() {
	DPrintf("[选举日志][Term %v] server %d 发起选举\n", rf.currentTerm, rf.me)
	rf.currentTerm++    //term自增1
	rf.votedFor = rf.me //自己给自己投票
	rf.persist()        //修改currentTerm和voteFor后，需要持久化
	//注意选票信息放到外面，不要放到go routine里，不然它创建的args的信息可能不是发起选举的
	//把我的最后一条日志的索引和term发过去，选民通过这个知道我是否有资格
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.log[rf.RealLogIdx(rf.getLastLogIndex())].Term,
	}
	voteCount := 1 //投票数，初始为1，因为自己给自己投了一票
	for server := range rf.peers {
		//自己不用给自己发
		if server == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//对方term比我高，我变成Follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchRoleTo(Follower)
					rf.persist()
				}
				// 2D的脑裂问题，加了个条件rf.currentTerm == args.Term，保证验票时的Term和竞选时的Term一样
				if reply.VoteGranted && rf.role == Candidate && rf.currentTerm == args.Term {
					voteCount++
					if voteCount > len(rf.peers)/2 && rf.role != Leader {
						rf.switchRoleTo(Leader)
					}
				}
			}
		}(server)
	}
}

// 群发心跳，调用者需要加锁
func (rf *Raft) heartbeats() {
	for server := range rf.peers {
		// 不用发给自己
		if server == rf.me {
			continue
		}
		// 向指定server推送日志
		go func(server int) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.role != Leader {
				// 只有Leader才能推日志
				return
			}
			// 根据账本记录，找到server的最后一条日志条目，那就是nextIndex减一
			prevLogIndex := rf.nextIndex[server] - 1
			//(2D)，判断是要推送日志还是推送快照
			DPrintf("PrevLogIndex是 %v ;lastIncludedIndex是%v \n", prevLogIndex, rf.lastIncludedIndex)
			if prevLogIndex < rf.lastIncludedIndex {
				// 发现Follower已经落后于快照点，那直接推送快照
				DPrintf("leader %v 向 server %v 发送sendInstallSnapshot, lastIncludedIndex=%v, nextIndex[%v]=%v \n", rf.me, server, rf.lastIncludedIndex, server, rf.nextIndex[server])
				args := InstallSnapshotArgs{
					Term:              rf.currentTerm,
					LeaderId:          rf.me,
					LastIncludedIndex: rf.lastIncludedIndex,
					LastIncludedTerm:  rf.lastIncludedTerm,
					SnapShot:          rf.snapShot,
					LastIncluedCmd:    rf.log[0].Command,
				}
				go rf.handleInstallSnapshot(server, args)
			} else {
				DPrintf("leader %v 开始向 server %v 广播新的AppendEntries, lastIncludedIndex=%v, nextIndex[%v]=%v\n", rf.me, server, rf.lastIncludedIndex, server, rf.nextIndex[server])
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					PrevLogTerm:  rf.log[rf.RealLogIdx(prevLogIndex)].Term,
					LeaderCommit: rf.commitIndex,
					Entries:      nil,
				}
				if rf.getLastLogIndex() > args.PrevLogIndex {
					//对方Follower没有落后快照点，但日志没我多，所以向它推送日志
					startIdx := rf.RealLogIdx(prevLogIndex + 1) //遇到要在log里操作，就要用RealLogIdx
					// use deep copy to avoid race condition when override log in AppendEntries()
					// 直接分片其实也能通过测试，但这种做法不安全，因为log可能会被修改，而测试无法模拟这种边界情况
					// entries := rf.log[(prevLogIndex + 1):]
					// Raft论文意思是，AppendEntries RPC 传递的是“那一刻的日志条目快照”，应与 leader 后续操作解耦
					// 所以用make分配空间，再copy，这样更加通用与安全
					// Raft 的日志是共享状态，任何传出的数据都应该解耦，避免竞态和副作用。
					entries := make([]LogEntry, len(rf.log[startIdx:]))
					copy(entries, rf.log[startIdx:])
					args.Entries = entries
				} else {
					//Follower和我一致，没有可发的，所以发个空的
					args.Entries = make([]LogEntry, 0)
				}
				go rf.handleAppendEntries(server, args)
			}
		}(server)
	}
}

// 处理一个具体发送AE的交互
// 发送AE后，发现不一致时，也会根据情况发送快照，有三处
// 这里会有3个情况触发发送InstallSnapshot RPC:
// Follower的日志过短(PrevLogIndex这个位置在Follower中不存在), 甚至短于lastIncludedIndex
// Follower的日志在PrevLogIndex这个位置发生了冲突, 回退时发现即使到了lastIncludedIndex也找不到匹配项(大于或小于这个Xterm)
// nextIndex中记录的索引本身就小于lastIncludedIndex
func (rf *Raft) handleAppendEntries(server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		// If last log index ≥ nextIndex for a follower: send
		// AppendEntries RPC with log entries starting at nextIndex
		// • If successful: update nextIndex and matchIndex for
		// follower (§5.3)
		// • If AppendEntries fails because of log inconsistency:
		// decrement nextIndex and retry (§5.3)
		if reply.Success {
			// successfully replicated args.Entries
			// 如果目标follower成功写入推送的日志
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries) //已有的+添加的
			rf.nextIndex[server] = rf.matchIndex[server] + 1              //下一条就是matchIndex+1

			// If there exists an N such that N > commitIndex, a majority
			// of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			// set commitIndex = N (§5.3, §5.4).
			// 再看日志的commit情况，现在看Leader的还没commit的日志,也就是最后一条日志到commitIndex这部分
			for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
				//试试这个N
				count := 0
				for _, matchIndex := range rf.matchIndex {
					if matchIndex >= N {
						count += 1
					}
				}
				//统计一下，看看过半没
				if count > len(rf.peers)/2 {
					// most of nodes agreed on rf.log[i]
					// 过半节点的日志都记录了N之前的条目，那就可以推动commit了
					if rf.lastIncludedIndex < N {
						rf.setCommitIndex(N)
					}
					break
				}
			}
		} else {
			//推送失败了，什么原因
			if reply.Term > rf.currentTerm {
				//对方term比我高，那我这个Leader过期了，所以我要转变为Follower
				rf.currentTerm = reply.Term
				rf.switchRoleTo(Follower)
				rf.persist()
			} else {
				// 它之前的日志跟我的对应的不一致，所以要降低它的nextIndex，直到一致，然后就能把后面不一致的一口气推过去
				// 这里也是性能优化点，如果有1000条日志不一致，那就得呼叫1000次RPC
				//rf.nextIndex[server]--
				// (quick fallback)
				rf.nextIndex[server] = reply.ConflictIndex
				// if term found, override it to
				// the first entry after entries in ConflictTerm
				//如果reply.ConflictTerm == -1，说明是PrevLogIndex不匹配
				//返回错误后，Leader会修改对应server的nextIndex，变成Follow返回的ConflictIndex
				//ConflictIndex是Follower发现和Leader不一致后，推测的最早不一致的条目索引，虽然有可能前面还有不一致的
				//等下次心跳再试试
				//如果reply.ConflictTerm != -1，那说明PrevLogIndex对应但Term不一致，说明日志不一致，有冲突
				//所以要往前遍历找相同Term的最开始一条
				//要结合2D的逻辑索引和lastIncludedIndex
				if reply.ConflictTerm != -1 {
					i := args.PrevLogIndex
					if i < rf.lastIncludedIndex {
						i = rf.lastIncludedIndex
					}
					for ; i > rf.lastIncludedIndex; i-- {
						if rf.log[rf.RealLogIdx(i)].Term == reply.ConflictTerm {
							// in next trial, check if log entries in ConflictTerm matches
							//从后往前遍历log，找到第一条匹配的
							rf.nextIndex[server] = i
							break
						}
					}
				}
			}
		}
		rf.mu.Unlock()
	}
}

// Leader向Follow发送InstallSnapshot指令
func (rf *Raft) handleInstallSnapshot(server int, args InstallSnapshotArgs) {
	rf.mu.Lock()
	//检查一下还是不是Leader
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock() //最好在发送RPC时解锁

	DPrintf("server %v 发送快照给%v 快照索引 %v\n", rf.me, server, args.LastIncludedIndex)
	reply := InstallSnapshotReply{}
	if rf.sendInstallSnapshot(server, &args, &reply) {
		//发送RPC成功了
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if reply.Success {
			//如果对方顺利接收，那我就可以更新nextIndex了，下一位是1，因为之前的部分都因快照而截断了
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		} else {
			if reply.Term > rf.currentTerm {
				//发现自己是旧Leader
				rf.currentTerm = reply.Term
				rf.switchRoleTo(Follower)
				rf.persist()
			}
		}
	}
}

// 设置commitIndex，并通知applier
func (rf *Raft) setCommitIndex(commitIndex int) {
	if commitIndex > rf.commitIndex {
		rf.commitIndex = commitIndex
		rf.cond.Broadcast()
	}
}

// 索引是2B实验的一个难点，不清晰的索引会带来很多bug
// lastLogIndex指最后一条日志的索引
// 因为本Raft实现中，log初始用一个空条目占位了，有很多好处，首先日志的index就对应具体存储的索引
// 其次，空条目类似链表的头节点，让我们不用判断一些导致数组越界的临界条件
// 但是len(rf.log)计算log长度，会带上这条空条目，所以要-1
// 所以lastLogIndex就等于len(rf.log) - 1
func (rf *Raft) getLastLogIndex() int {
	//return len(rf.log) - 1
	return rf.VirtualLogIdx(len(rf.log)) - 1
}

// (2D)
// 加上快照机制后，存在两种索引，代码将遵循以下规则:
//	1.真实索引RealLogIdx，真正存储在log里的索引。访问rf.log一律使用真实的切片索引, 即Real Index
//	2.逻辑索引VirtualLogIdx，全局的索引。在其余情况, 一律使用全局真实递增的索引Virtual Index
// 有效的日志项索引从1开始，和初始化的log一致
// 为了让它们方便转换，可以写两个方法，从而映射所有代码中对索引的操作,
// 调用RealLogIdx将Virtual Index转化为Real Index,
// 调用VirtualLogIdx将Real Index转化为Virtual Index

// 将传过来的逻辑索引转换为真实索引
func (rf *Raft) RealLogIdx(vIdx int) int {
	return vIdx - rf.lastIncludedIndex
}

// 将传过来的真实索引转换为逻辑索引
func (rf *Raft) VirtualLogIdx(rIdx int) int {
	return rIdx + rf.lastIncludedIndex
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers //peer就是client节点
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.role = Follower //初始都是Follower
	rf.votedFor = -1   //初始谁都不投
	rf.dead = 0        //初始都存活
	rf.heartbeatTimer = time.NewTimer(randTimeDuration())
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1) //Raft`s log start from index 1，Raft的日志索引从1开始，但go的数组从0开始，所以占个位

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.cond = sync.NewCond(&rf.mu)
	// initialize from state persisted before a crash
	// 2C 持久化操作，如果发现之前有备份，可以直接读取
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())
	fmt.Printf("")
	// 注意要先读取持久化数据，然后在执行两个go routine
	go rf.ticker()
	go rf.applier()

	return rf
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}
