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

const (
	Follower           int = 1
	Candidate          int = 2
	Leader             int = 3
	HEART_BEAT_TIMEOUT     = 100 //心跳超时，要求1秒10次，所以是100ms一次
)

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

	//辅助数据
	heartbeatTimer *time.Timer   // 心跳定时器
	state          int           // 角色
	applyCh        chan ApplyMsg // 提交通道
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.state == Leader
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
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
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
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil {
		panic("fail to decode state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
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
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

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
	DPrintf("Candidate[raft%v][term:%v] request vote: raft%v[%v] 's term%v\n", args.CandidateId, args.Term, rf.me, rf.state, rf.currentTerm)
	if args.Term < rf.currentTerm ||
		(args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}

	// 2B: candidate's vote should be at least up-to-date as receiver's log "up-to-date" is defined in thesis 5.4.1
	// 投票限制，必须日志够新的节点才有资格当选，防止一颗老鼠屎污染一锅汤
	lastLogIndex := rf.getLastLogIndex() //本日志的最后一条的索引
	if args.LastLogTerm < rf.log[lastLogIndex].Term ||
		(args.LastLogTerm == rf.log[lastLogIndex].Term && args.LastLogIndex < (lastLogIndex)) {
		// Receiver is more up-to-date, does not grant vote
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	// 满足限制，可以投票
	rf.votedFor = args.CandidateId
	rf.persist()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	// reset timer after grant vote
	// 投票后要重置，防止多余选举
	rf.heartbeatTimer.Reset(randTimeDuration())
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
	DPrintf("leader[raft%v][term:%v] beat term:%v [raft%v][%v]\n", args.LeaderId, args.Term, rf.currentTerm, rf.me, rf.state)
	reply.Success = true
	//------我收到了Leader的日志推送------

	// 1. Reply false if term < currentTerm (§5.1)
	// 如果Leader的term还没我高，拒绝
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm //把我的Term返回
		return
	}
	//If RPC request or response contains term T > currentTerm:set currentTerm = T, convert to follower (§5.1)
	// 我没Leader的高，那无论我原来是什么角色，现在都变成Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.switchStateTo(Follower)
	}
	// reset election timer even log does not match args.LeaderId is the current term's Leader
	// 重置计时器
	rf.heartbeatTimer.Reset(randTimeDuration())

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	lastLogIndex := rf.getLastLogIndex()
	// 我的日志偏少了,不一致
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false
		reply.Term = rf.currentTerm
		// (quick fallback)
		// optimistically thinks receiver's log matches with Leader's as a subset
		// 往好的想，Follower 有可能是Leader的一个子集
		reply.ConflictIndex = len(rf.log)
		// no conflict term  没冲突的Term
		reply.ConflictTerm = -1
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	// 上一个日志条目，索引匹配，但Term不匹配，所以本次追加不能通过，因为上一个都不一致，后面追加新的也没用
	if rf.log[(args.PrevLogIndex)].Term != args.PrevLogTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// (quick fallback)
		// receiver's log in certain term unmatches Leader's log
		// Term冲突了，把自己最后一个节点的Term发给Leader
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term

		// expecting Leader to check the former term
		// so set ConflictIndex to the first one of entries in ConflictTerm
		// 初始冲突的索引就是args.PrevLogIndex，但不能排除前面的日志也冲突
		conflictIndex := args.PrevLogIndex
		// apparently, since rf.log[0] are ensured to match among all servers
		// ConflictIndex must be > 0, safe to minus 1
		// 上一个日志的term不符合，那么前面一样Term的日志大概率也是不对的，把conflictIndex往后退
		for rf.log[conflictIndex-1].Term == reply.ConflictTerm {
			conflictIndex--
		}
		reply.ConflictIndex = conflictIndex
		return
	}

	// 能通过之前的判断，说明我和Leader在PrevLogIndex上的日志以及之前的日志都是一致的，那么就可以在后面添加新日志了
	// 4. Append any new entries not already in the log compare from rf.log[args.PrevLogIndex + 1]
	rf.log = rf.log[:(args.PrevLogIndex + 1)] //截取一致的日志，因为分片左闭右开，要保留PrevLogIndex，所以+1
	rf.log = append(rf.log, args.Entries...)  //然后把Entries追加
	rf.persist()
	//5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	// 一致性判断，Leader的CommitIndex和我的commitIndex中最小值，是我的新commitIndex
	if args.LeaderCommit > rf.commitIndex {
		min := func(x, y int) int {
			if x < y {
				return x
			} else {
				return y
			}
		}
		rf.setCommitIndex(min(args.LeaderCommit, rf.getLastLogIndex()))
	}

	reply.Success = true
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	isLeader := rf.state == Leader

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
			switch rf.state {
			case Follower:
				rf.switchStateTo(Candidate)
			case Candidate:
				rf.startElection()
			case Leader:
				//Leader到时了，群发心跳，并重置计时器
				rf.heartbeats()
				rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
			}
			rf.mu.Unlock()
		}
	}
}

func randTimeDuration() time.Duration {
	return time.Duration(HEART_BEAT_TIMEOUT*3+rand.Intn(HEART_BEAT_TIMEOUT)) * time.Millisecond
}

// 切换状态，调用者需要加锁
func (rf *Raft) switchStateTo(state int) {
	// 如果已经是想转变的状态了，不用重复操作
	if state == rf.state {
		return
	}
	DPrintf("Term %d: server %d convert from %v to %v\n", rf.currentTerm, rf.me, rf.state, state)
	rf.state = state
	switch state {
	case Follower:
		rf.votedFor = -1
		rf.heartbeatTimer.Reset(randTimeDuration())
	case Candidate:
		// 成为候选人后立马进行选举
		rf.startElection()
	case Leader:
		// initialized to leader last log index + 1
		// 当选后，要重置nextIndex和matchIndex
		for i := range rf.peers {
			rf.nextIndex[i] = rf.getLastLogIndex() + 1
			rf.matchIndex[i] = 0
		}
		rf.votedFor = -1
		rf.heartbeats()
		rf.heartbeatTimer.Reset(HEART_BEAT_TIMEOUT * time.Millisecond)
	}
}

// 发送心跳包，调用者需要加锁
func (rf *Raft) heartbeats() {
	for i := range rf.peers {
		//不用发给自己
		if i == rf.me {
			continue
		}
		go rf.heartbeat(i)
	}
}

// 向指定server推送日志
func (rf *Raft) heartbeat(server int) {
	rf.mu.Lock()
	if rf.state != Leader {
		// 只有Leader才能推日志
		rf.mu.Unlock()
		return
	}
	// 根据账本记录，找到server的最后一条日志条目，那就是nextIndex减一
	prevLogIndex := rf.nextIndex[server] - 1

	// use deep copy to avoid race condition when override log in AppendEntries()
	// 直接分片其实也能通过测试，但这种做法不安全，因为log可能会被修改，而测试无法模拟这种边界情况
	// entries := rf.log[(prevLogIndex + 1):]
	// Raft论文意思是，AppendEntries RPC 传递的是“那一刻的日志条目快照”，应与 leader 后续操作解耦
	// 所以用make分配空间，再copy，这样更加通用与安全
	// Raft 的日志是共享状态，任何传出的数据都应该解耦，避免竞态和副作用。
	entries := make([]LogEntry, len(rf.log[(prevLogIndex+1):]))
	copy(entries, rf.log[(prevLogIndex+1):])

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  rf.log[(prevLogIndex)].Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	var reply AppendEntriesReply
	if rf.sendAppendEntries(server, &args, &reply) {
		rf.mu.Lock()
		if rf.state != Leader {
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
					rf.setCommitIndex(N)
					break
				}
			}
		} else {
			//推送失败了，什么原因
			if reply.Term > rf.currentTerm {
				//对方term比我高，那我这个Leader过期了，所以我要转变为Follower
				rf.currentTerm = reply.Term
				rf.switchStateTo(Follower)
				rf.persist()
			} else {
				// 它之前的日志跟我的对应的不一致，所以要降低它的nextIndex，直到一致，然后就能把后面不一致的一口气推过去
				// 这里也是性能优化点，如果有1000条日志不一致，那就得呼叫1000次RPC
				//rf.nextIndex[server]--
				// (quick fallback)
				rf.nextIndex[server] = reply.ConflictIndex
				// if term found, override it to
				// the first entry after entries in ConflictTerm
				if reply.ConflictTerm != -1 {
					for i := args.PrevLogIndex; i >= 1; i-- {
						if rf.log[i-1].Term == reply.ConflictTerm {
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

// 开始选举，调用者需要加锁
func (rf *Raft) startElection() {
	DPrintf("raft%v is starting election\n", rf.me)
	rf.currentTerm += 1 //term自增1
	rf.votedFor = rf.me //自己给你投票
	rf.persist()
	voteCount := 1                              //投票数，初始为1，因为自己给自己投了一票
	rf.heartbeatTimer.Reset(randTimeDuration()) //发起投票，重置计时器

	for i := range rf.peers {
		//自己不用给自己发
		if i == rf.me {
			continue
		}
		go func(server int) {
			rf.mu.Lock()
			lastLogIndex := rf.getLastLogIndex()
			//把我的最后一条日志的索引和term发过去，选民通过这个知道我是否有资格
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  rf.log[lastLogIndex].Term,
			}
			DPrintf("raft%v[%v] is sending RequestVote RPC to raft%v\n", rf.me, rf.state, server)
			rf.mu.Unlock()
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//对方term比我高，我变成Follower
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.switchStateTo(Follower)
					rf.persist()
				}
				if reply.VoteGranted && rf.state == Candidate {
					voteCount++
					if voteCount > len(rf.peers)/2 {
						rf.switchStateTo(Leader)
					}
				}
			}
		}(i)
	}
}

// 索引是2B实验的一个难点，不清晰的索引会带来很多bug
// lastLogIndex指最后一条日志的索引
// 因为本Raft实现中，log初始用一个空条目占位了，有很多好处，首先日志的index就对应具体存储的索引
// 其次，空条目类似链表的头节点，让我们不用判断一些导致数组越界的临界条件
// 但是len(rf.log)计算log长度，会带上这条空条目，所以要-1
// 所以lastLogIndex就等于len(rf.log) - 1
func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

// several setters, should be called with a lock
// 一条日志如何被commit了，说明过半节点都写入了这条日志
// 而一旦一条日志被 commit，Raft 协议就保证这条日志永远不会被覆盖或删除
// commit的日志，就可以被apply了，setCommitIndex的作用就是把还没apply的命令apply掉，告诉上层应用可以执行返回结果了
func (rf *Raft) setCommitIndex(commitIndex int) {
	rf.commitIndex = commitIndex
	// apply all entries between lastApplied and committed
	// should be called after commitIndex updated
	// 把lastApplied到committed中的日志都apply掉
	// 当commitIndex被更新时，执行此方法
	if rf.commitIndex > rf.lastApplied {
		// startIdx是第一个要apply的日志索引，endIdx是最后一个要apply的日志索引+1
		// 这样endIdx-startIdx就是apply的次数，就像6-4=2，而7-4=3
		// 可以直接分片，但那样就是共享底层的日志数组，很危险，以后想并行apply等扩展操作，都会因为这个bug耽误
		startIdx, endIdx := rf.lastApplied+1, rf.commitIndex+1
		DPrintf("%v apply from index %d to %d", rf, startIdx, rf.commitIndex)
		entriesToApply := make([]LogEntry, endIdx-startIdx)
		copy(entriesToApply, rf.log[startIdx:endIdx])

		//这必须异步，因为rf.applyCh <- msg是个阻塞操作，channel必须被读取了才能往下执行
		//如果不异步，那setCommitIndex方法就会阻塞，进而breakHeart操作也会阻塞，导致Leader无法进行心跳
		//所以这里必须异步，也就是go routine
		go func(startIdx int, entries []LogEntry) {
			for idx, entry := range entries {
				msg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIdx + idx,
				}
				rf.applyCh <- msg
				// do not forget to update lastApplied index
				// this is another goroutine, so protect it with lock
				// 被应用后，也要更新lastApplied
				rf.mu.Lock()
				if rf.lastApplied < msg.CommandIndex {
					rf.lastApplied = msg.CommandIndex
				}
				rf.mu.Unlock()
			}
		}(startIdx, entriesToApply)
	}
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
	rf.state = Follower //初始都是Follower
	rf.votedFor = -1    //初始谁都不投
	rf.dead = 0         //初始都存活
	rf.heartbeatTimer = time.NewTimer(randTimeDuration())
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 1) //Raft`s log start from index 1，Raft的日志索引从1开始，但go的数组从0开始，所以占个位

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	go rf.ticker()

	// initialize from state persisted before a crash
	//持久化操作，如果发现之前有备份，可以直接读取
	rf.readPersist(persister.ReadRaftState())

	return rf
}
