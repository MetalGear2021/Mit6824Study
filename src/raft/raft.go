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
	"6.824/labrpc"
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
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

// HeartBeatTimeout 定义一个全局心跳超时时间
var HeartBeatTimeout = 120 * time.Millisecond

type Role string

const (
	Leader    Role = "leader"
	Candidate Role = "candidate"
	Follower  Role = "follower"
)

// 日志条目，由term和command组成
type LogEntry struct {
	Term    int         //本条目生成的任期
	Command interface{} //本条目记录的命令
}
type AppendEntriesState int

const (
	AppNormal    AppendEntriesState = iota //追加正常
	AppOutOfDate                           //追加过时
	AppKilled                              //Raft程序终止
	AppCommitted                           //追加的日志已经提交
	Mismatch                               //追加不匹配
)

type VoteState int

const (
	Normal VoteState = iota //投票过程正常
	Killed                  //Raft节点已终止
	Expire                  //投票(消息\竞选者)过期
	Voted                   //本Term内已经投过票
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
	currentTerm int        // 最新的任期，初始为0，然后单调递增
	votedFor    int        // 在最新的任期里，给哪个候选人投票
	role        Role       // 本server的角色，是leader还是follower还是candidates
	logEntries  []LogEntry //每个Raft节点记录的日志条目表
	commitIndex int        //本节点已提交日志最新的index,初始为0,单调递增
	lastApplied int        //本节点被应用日志最新的index,初始为0,单调递增
	applyCh     chan ApplyMsg
	nextIndex   []int //了解各个Follower的日志条目最新索引的吓一条，会随着AE的回复修正(如果冲突的话)，在Leader当选时初始化，初始都是Leader自己Log的最新index+1
	matchIndex  []int //了解Follower日志条目的正确匹配的最新索引，初始为0，单调递增

	overtime time.Duration // 设置超时时间，200-400ms
	timer    *time.Ticker  // 每个节点中的计时器
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
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

type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int //任期
	CandidateId  int //候选人的ID
	LastLogIndex int //Follower汇报我的上一条日志条目的索引
	LastLogTerm  int //Follower汇报我的上一条日志条目的任期
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int       //候选人的任期
	VoteGranted bool      //是否给发送者投票
	VoteState   VoteState //投票的结果
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = -1
		reply.VoteState = Killed
		reply.VoteGranted = false
		return
	}

	if args.Term < rf.currentTerm {
		// 1.任期没有我的新，直接拒绝
		reply.Term = rf.currentTerm
		reply.VoteState = Expire
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		// 2.如果对方term比我大，我就转为follower，然后直接投票
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
	}

	if rf.currentTerm == args.Term {
		// 3.在同一任期前提下，判断是否可以投票，之间没投过或者原本投给自己
		//论文原文是If votedFor is null or CandidateId,
		//and
		//candidate`s log is at least as up-to-date as receiver`s log,
		//grant vote($5.2,$5.4)
		//up-to-date的意思是，Raft 通过比较两份日志中的最后一条日志条目的索引和任期号来定义谁的日志更新。
		//如果两份日志最后条目的任期号不同，那么任期号大的日志更新
		//如果两份日志最后条目的任期号相同，那么谁的日志更长，谁就更新
		if rf.votedFor == -1 {
			currentLogIndex := len(rf.logEntries) - 1
			currentLogTerm := 0
			if currentLogIndex >= 0 {
				currentLogTerm = rf.logEntries[currentLogIndex].Term
			}
			//候选人的日志太旧，这是5.4.1规则，不能让日志落后的候选人担任Leader
			if args.LastLogIndex < currentLogIndex || args.LastLogTerm < currentLogTerm {
				reply.VoteState = Expire
				reply.VoteGranted = false
				reply.Term = rf.currentTerm
				return
			}

			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = rf.currentTerm
			reply.VoteState = Normal

			rf.timer.Reset(rf.overtime)
		} else {
			//已经投票了
			reply.VoteState = Voted
			reply.VoteGranted = false
			if rf.votedFor != args.CandidateId {
				//是投给别人吗
				return
			} else {
				//投的就是args的候选人，但因为网络问题，重发了
				rf.role = Follower
			}
			rf.timer.Reset(rf.overtime)
		}
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, votes *int) bool {

	if rf.killed() {
		return false
	}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	for !ok {
		//失败重传
		if rf.killed() {
			return false
		}
		ok = rf.peers[server].Call("Raft.RequestVote", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return false
	}

	//根据此Follower的投票结果
	switch reply.VoteState {
	case Expire:
		{
			//过期了
			//1.候选者的Term落后了
			//2.候选者的日志落后了
			rf.role = Follower
			rf.timer.Reset(rf.overtime)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
			}
		}
	case Normal, Voted:
		{
			if reply.VoteGranted && reply.Term == rf.currentTerm && *votes <= (len(rf.peers)/2) {
				*votes++
			}
			if *votes >= (len(rf.peers)/2)+1 {
				*votes = 0
				if rf.role == Leader {
					return ok
				}

				rf.role = Leader
				rf.matchIndex = make([]int, len(rf.peers))
				rf.nextIndex = make([]int, len(rf.peers))
				for i, _ := range rf.nextIndex {
					rf.nextIndex[i] = len(rf.logEntries) + 1
				}
				rf.timer.Reset(HeartBeatTimeout)
			}
		}
	case Killed:
		return false
	}

	return ok
}

// Leader向Folloers发送的信息的参数
type AppendEntriesArgs struct {
	// Your data here (2A, 2B)
	Term         int        //任期
	LeaderId     int        //Leader的ID,这样follower就能重定向到clients
	PrevLogIndex int        //Leader上一条日志的Index
	PrevLogTerm  int        //Leader上一条日志的Term
	Entries      []LogEntry //要被存储的日志条目，对心跳来说是空
	LeaderCommit int        //Leader的commitIndex
}

// Leader向Folloers发送的信息的回复
type AppendEntriesReply struct {
	// Your data here (2A).
	Term        int                //leader的currentTerm
	Success     bool               //Args中的PreLogIndex/PreLogTerm是否匹配follower对应的的logIndex/LogTerm，不匹配直接返回false
	AppState    AppendEntriesState //追加状态
	UpNextIndex int                //告诉Leader，本Follower更新日志的nextIndex
}

//func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//
//	// 检查节点状态
//	if rf.killed() {
//		reply.AppState = AppKilled
//		reply.Term = -1
//		reply.Success = false
//		return
//	}
//
//	// 1. 任期检查
//	if args.Term < rf.currentTerm {
//		reply.AppState = AppOutOfDate
//		reply.Term = rf.currentTerm
//		reply.Success = false
//		return
//	}
//
//	// 更新任期并转换为 follower
//	if args.Term > rf.currentTerm {
//		rf.currentTerm = args.Term
//		rf.votedFor = -1
//		rf.role = Follower
//	}
//
//	// 重置心跳计时器
//	rf.timer.Reset(rf.overtime)
//
//	// 2. 日志一致性检查
//	if args.PrevLogIndex > 0 {
//		// 检查 PrevLogIndex 是否超出范围
//		if args.PrevLogIndex > len(rf.logEntries) {
//			reply.AppState = Mismatch
//			reply.Term = rf.currentTerm
//			reply.Success = false
//			reply.UpNextIndex = len(rf.logEntries) + 1
//			return
//		}
//
//		// 检查任期是否匹配
//		if args.PrevLogIndex-1 < len(rf.logEntries) &&
//			rf.logEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
//			reply.AppState = Mismatch
//			reply.Term = rf.currentTerm
//			reply.Success = false
//
//			// 找到冲突任期的第一个索引
//			conflictTerm := rf.logEntries[args.PrevLogIndex-1].Term
//			conflictIndex := args.PrevLogIndex
//			for i := args.PrevLogIndex - 1; i >= 0; i-- {
//				if rf.logEntries[i].Term != conflictTerm {
//					break
//				}
//				conflictIndex = i + 1
//			}
//			reply.UpNextIndex = conflictIndex
//			return
//		}
//	}
//
//	// 3. 追加日志条目
//	if len(args.Entries) > 0 {
//		// 删除冲突的日志条目并追加新条目
//		insertIndex := args.PrevLogIndex
//		for i, entry := range args.Entries {
//			logIndex := insertIndex + i
//			if logIndex < len(rf.logEntries) {
//				if rf.logEntries[logIndex].Term != entry.Term {
//					// 发现冲突，截断日志
//					rf.logEntries = rf.logEntries[:logIndex]
//					rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
//					break
//				}
//			} else {
//				// 追加新条目
//				rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
//				break
//			}
//		}
//	}
//
//	// 4. 更新提交索引
//	if args.LeaderCommit > rf.commitIndex {
//		min := func(a, b int) int {
//			if a < b {
//				return a
//			}
//			return b
//		}
//		newCommit := min(args.LeaderCommit, len(rf.logEntries))
//		if newCommit > rf.commitIndex {
//			// 应用新提交的日志
//			for i := rf.commitIndex + 1; i <= newCommit; i++ {
//				applyMsg := ApplyMsg{
//					CommandValid: true,
//					CommandIndex: i,
//					Command:      rf.logEntries[i-1].Command,
//				}
//				rf.applyCh <- applyMsg
//			}
//			rf.commitIndex = newCommit
//			rf.lastApplied = newCommit
//		}
//	}
//
//	reply.AppState = AppNormal
//	reply.Term = rf.currentTerm
//	reply.Success = true
//	reply.UpNextIndex = len(rf.logEntries) + 1
//}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.AppState = AppKilled
		reply.Term = -1
		reply.Success = false
		return
	}

	// 任期检查
	if args.Term < rf.currentTerm {
		reply.AppState = AppOutOfDate
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// 如果对方term更大，更新自身状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = Follower
		rf.persist()
	}

	// 重置计时器
	rf.timer.Reset(rf.overtime)

	// 日志一致性检查
	if args.PrevLogIndex > 0 {
		// 检查PrevLogIndex是否超出范围
		if args.PrevLogIndex > len(rf.logEntries) {
			reply.AppState = Mismatch
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.UpNextIndex = len(rf.logEntries) + 1
			return
		}

		// 检查任期是否匹配
		if rf.logEntries[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			reply.AppState = Mismatch
			reply.Term = rf.currentTerm
			reply.Success = false

			// 优化冲突处理，找到冲突任期的第一个索引
			conflictTerm := rf.logEntries[args.PrevLogIndex-1].Term
			conflictIndex := args.PrevLogIndex
			for i := args.PrevLogIndex - 1; i >= 0; i-- {
				if rf.logEntries[i].Term != conflictTerm {
					break
				}
				conflictIndex = i + 1
			}
			reply.UpNextIndex = conflictIndex
			return
		}
	}

	// 追加日志条目
	newEntriesAdded := false
	if len(args.Entries) > 0 {
		insertIndex := args.PrevLogIndex
		// 检查是否有冲突
		for i, entry := range args.Entries {
			logIndex := insertIndex + i
			if logIndex < len(rf.logEntries) {
				if rf.logEntries[logIndex].Term != entry.Term {
					// 发现冲突，截断日志
					rf.logEntries = rf.logEntries[:logIndex]
					rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
					newEntriesAdded = true
					break
				}
			} else {
				// 追加新条目
				rf.logEntries = append(rf.logEntries, args.Entries[i:]...)
				newEntriesAdded = true
				break
			}
		}
		if newEntriesAdded {
			rf.persist()
		}
	}

	// 更新提交索引
	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit > len(rf.logEntries) {
			newCommit = len(rf.logEntries)
		}
		if newCommit > rf.commitIndex {
			// 应用新提交的日志
			for i := rf.commitIndex + 1; i <= newCommit; i++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: i,
					Command:      rf.logEntries[i-1].Command,
				}
				rf.applyCh <- applyMsg
			}
			rf.commitIndex = newCommit
			rf.lastApplied = newCommit
		}
	}

	reply.AppState = AppNormal
	reply.Term = rf.currentTerm
	reply.Success = true
	reply.UpNextIndex = len(rf.logEntries) + 1
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, appendNums *int) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != args.Term { // ← 新增保护
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	// paper中5.3节第一段末尾提到，如果append失败应该不断的retries，知道成功被提交
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	for !ok {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.role != Leader || rf.currentTerm != args.Term { // ← 新增保护
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term != rf.currentTerm {
		return // 我已经不是这个 term 的 Leader 了
	}
	switch reply.AppState {
	case AppKilled:
		{
			return
		}
	case AppOutOfDate:
		//推送日志条目，结果对面的term比我高，那我只能转变为Follower，然后term和它一致
		rf.role = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.timer.Reset(rf.overtime)
	case AppCommitted:
		//已经提交了
		if args.Term != rf.currentTerm {
			return
		}
		rf.nextIndex[server] = reply.UpNextIndex
	case Mismatch:
		//不匹配
		if args.Term != rf.currentTerm {
			return
		}
		rf.nextIndex[server] = reply.UpNextIndex
	case AppNormal:
		{
			//正常情况，Follower接收AE并同步日志
			//2A的test目的是让Leader能不能连续任期，所以2A只需要对节点初始化然后返回就好
			//2B需要判断返回的节点是否超过半数commit，才能将自身commit
			if reply.Success && reply.Term == rf.currentTerm && *appendNums <= len(rf.peers)/2 {
				*appendNums++
			}
			//此Follower的长度比Leader的大
			if rf.nextIndex[server] > len(rf.logEntries)+1 {
				return
			}
			if len(args.Entries) > 0 {
				rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[server] = rf.matchIndex[server] + 1
			} else {
				// 心跳包，如果 nextIndex 太大，应该修正（可选，但推荐）
				if rf.nextIndex[server] > len(rf.logEntries)+1 {
					rf.nextIndex[server] = len(rf.logEntries) + 1
				}
			}
			if rf.role != Leader {
				return
			}

			rf.updateCommitIndex()
		}
	}
	return
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
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	if rf.killed() {
		return index, term, isLeader
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return index, term, isLeader
	}

	isLeader = true

	//初始化日志条目，并进行追加
	appendLog := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logEntries = append(rf.logEntries, appendLog)
	index = len(rf.logEntries)
	term = rf.currentTerm

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
	// 如果把一个节点kill了，那它的计时器也该关闭
	rf.mu.Lock()
	rf.timer.Stop()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
//func (rf *Raft) ticker() {
//	for rf.killed() == false {
//		// Follow随机等待的下限必须高于 Leader 定时心跳的间隔
//		// 在此基础上，如果定时心跳与随机等待差距不大，有可能误判没有Leader，从而发起新选举，如果随机等待高定时心跳太多，又会拖慢程序恢复速度
//		// Leader 心跳间隔：通常 50~100ms
//		// Follower 选举超时：通常 150~300ms
//		// 这样设计是为了保证：
//		// 	Leader 能在 follower 超时前发送至少 1~2 次心跳
//		// 	避免网络抖动或短暂延迟导致不必要的选举
//		// 	保证系统稳定，减少 term 频繁增长
//		rf.mu.Lock()
//		if rf.role == Leader {
//			//如果我是Leader,要定时发送心跳
//			//最好要用go启动，这是非阻塞的，发起一个心跳后，就直接等待100ms了，无需等待心跳完成
//			//如果不加go，那就是阻塞式，会导致必须先发送心跳，然后等待接收回复，接收完，再等100ms，整个时间太长了，会导致有些Follower误判没有Leader，从而发起新的选举
//			//会警告 warning: term changed even though there were no failures  ... Passed
//			//go rf.broadcastHeartbeats()
//			appendNums := 1
//			for i := range rf.peers {
//				//自己不用给自己发
//				if i == rf.me {
//					continue
//				}
//				args := &AppendEntriesArgs{
//					Term:         rf.currentTerm,
//					LeaderId:     rf.me,
//					PrevLogIndex: 0,
//					PrevLogTerm:  0,
//					Entries:      nil,
//					LeaderCommit: rf.commitIndex, // commitIndex为大多数log所认可的commitIndex
//				}
//				reply := AppendEntriesReply{}
//				// 如果nextIndex[i]长度不等于rf.logEntries,代表与Leader的LogEntries不一致
//				args.Entries = rf.logEntries[rf.nextIndex[i]-1:]
//
//				//如果不是初始的0，针对不同Follower，Leader让它们参考的PrevLogIndex和PrevLogTerm也不一样
//				//这是一致化协议的要求，如果Leader发现PrevLogIndex和PrevLogTerm符合，那说明之前的日志也同步，就只用考虑后面的
//				if rf.nextIndex[i] > 0 {
//					args.PrevLogIndex = rf.nextIndex[i] - 1
//				}
//
//				if args.PrevLogIndex > 0 {
//					args.PrevLogTerm = rf.logEntries[args.PrevLogIndex-1].Term
//				}
//				go rf.sendAppendEntries(i, args, &reply, &appendNums)
//			}
//			rf.mu.Unlock()
//			time.Sleep(50 * time.Millisecond)
//		} else {
//			//如果我不是Leader,那就随机等待，等待后看超不超时，最低限度150ms，最高300ms
//			rf.mu.Unlock()
//			//生成一个固定的选举超时时间（150~300ms）
//			electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
//			time.Sleep(electionTimeout)
//			//检查是否超时,超时了就发起选举
//			if time.Since(rf.lastHeartbeatTime) >= electionTimeout {
//				rf.startElection()
//			}
//		}
//	}
//}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using

		// 当定时器结束进行超时选举
		select {

		case <-rf.timer.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			// 根据自身的status进行一次ticker
			switch rf.role {

			// follower变成竞选者
			case Follower:
				rf.role = Candidate
				fallthrough
			case Candidate:

				// 初始化自身的任期、并把票投给自己
				rf.currentTerm += 1
				rf.votedFor = rf.me
				votedNums := 1 // 统计自身的票数

				// 每轮选举开始时，重新设置选举超时
				rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生200-400ms
				rf.timer.Reset(rf.overtime)

				// 对自身以外的节点进行选举
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					voteArgs := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.logEntries),
						LastLogTerm:  0,
					}
					if len(rf.logEntries) > 0 {
						voteArgs.LastLogTerm = rf.logEntries[len(rf.logEntries)-1].Term
					}

					voteReply := RequestVoteReply{}

					go rf.sendRequestVote(i, &voteArgs, &voteReply, &votedNums)
				}
			case Leader:
				if rf.role != Leader {
					continue // 不再是 Leader，不要发送心跳
				}
				// 进行心跳/日志同步
				appendNums := 1 // 对于正确返回的节点数量
				rf.timer.Reset(HeartBeatTimeout)
				currentTerm := rf.currentTerm // 保存当前任期，避免在循环中被修改
				// 构造msg
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.role != Leader || rf.currentTerm != currentTerm {
						break // 不再是 Leader 或任期已变，停止发送
					}
					args := AppendEntriesArgs{
						Term:         currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: 0,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex, // commitIndex为大多数log所认可的commitIndex
					}

					reply := AppendEntriesReply{}

					// 如果nextIndex[i]长度不等于rf.logs,代表与leader的log entries不一致，需要附带过去

					args.Entries = rf.logEntries[rf.nextIndex[i]-1:]

					// 代表已经不是初始值0
					if rf.nextIndex[i] > 0 {
						args.PrevLogIndex = rf.nextIndex[i] - 1
					}

					if args.PrevLogIndex > 0 {
						//fmt.Println("len(rf.log):", len(rf.logs), "PrevLogIndex):", args.PrevLogIndex, "rf.nextIndex[i]", rf.nextIndex[i])
						args.PrevLogTerm = rf.logEntries[args.PrevLogIndex-1].Term
					}

					//fmt.Printf("[	ticker(%v) ] : send a election to %v\n", rf.me, i)
					go rf.sendAppendEntries(i, &args, &reply, &appendNums)
				}
			}

			rf.mu.Unlock()
		}

	}
}

func (rf *Raft) updateCommitIndex() {
	// 从当前 commitIndex+1 开始，向后找最大的 N，使得：
	// 1. log[N].term == currentTerm
	// 2. 大多数节点的 matchIndex[i] >= N
	for N := rf.commitIndex + 1; N <= len(rf.logEntries); N++ {
		if rf.logEntries[N-1].Term != rf.currentTerm {
			continue // 只考虑当前任期的日志
		}

		count := 1 // 自己
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}

		if count > len(rf.peers)/2 {
			// 可以提交到 N
			oldCommit := rf.commitIndex
			rf.commitIndex = N

			// 应用从 oldCommit+1 到 N 的所有日志
			for idx := oldCommit + 1; idx <= rf.commitIndex; idx++ {
				applyMsg := ApplyMsg{
					CommandValid: true,
					CommandIndex: idx,
					Command:      rf.logEntries[idx-1].Command,
				}
				rf.applyCh <- applyMsg
			}
			rf.lastApplied = rf.commitIndex // 保持一致
		} else {
			// 一旦某个 N 不满足，后面的也不用检查了（因为 matchIndex 是递增的）
			break
		}
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
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1 //初始谁也不投，所以为-1
	rf.currentTerm = 0
	rf.dead = 0
	rf.role = Follower //最开始都是Follower

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.applyCh = applyCh
	rf.logEntries = make([]LogEntry, 0)

	rf.overtime = time.Duration(150+rand.Intn(200)) * time.Millisecond // 随机产生150-350ms
	rf.timer = time.NewTicker(rf.overtime)
	//rf.logEntries = append(rf.logEntries, &LogEntry{Term: 0, Command: nil}) //第0条占位，因为logEntries的索引从1开始
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
