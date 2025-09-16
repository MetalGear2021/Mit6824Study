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
	"bytes"
	"math"
	"math/rand"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
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

type Role string

const (
	Leader    Role = "leader"
	Candidate Role = "candidate"
	Follower  Role = "follower"
)

// 日志条目，由term和command组成
type LogEntry struct {
	Term    int    //本条目生成的任期
	Command []byte //本条目记录的命令
}

func init() {
	labgob.Register(LogEntry{})
}

// Leader作为管理者，要有个账本管理，只有当选为Leader节点的才能用
type LeaderMenu struct {
	mu         sync.Mutex // 互斥锁，因为会随着AE修改，如果不保护，会出错
	nextIndex  []int      //了解各个Follower的日志条目最新索引的吓一条，会随着AE的回复修正(如果冲突的话)，在Leader当选时初始化，初始都是Leader自己Log的最新index+1
	matchIndex []int      //了解Follower日志条目的正确匹配的最新索引，初始为0，单调递增
}

// 声明一个指针类型
var leaderMenu = &LeaderMenu{}

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
	currentTerm       int         // 最新的任期，初始为0，然后单调递增
	votedFor          int         // 在最新的任期里，给哪个候选人投票
	role              Role        // 本server的角色，是leader还是follower还是candidates
	lastHeartbeatTime time.Time   // 上次接收到心跳的时间
	logEntries        []*LogEntry //每个Raft节点记录的日志条目表
	commitIndex       int         //本节点已提交日志最新的index,初始为0,单调递增
	lastApplied       int         //本节点被应用日志最新的index,初始为0,单调递增
	applyCh           chan ApplyMsg
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int //任期
	CandidateId  int //候选人的ID
	LastLogIndex int //Follower汇报我的上一条日志条目的索引
	LastLogTerm  int //Follower汇报我的上一条日志条目的任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //候选人的任期
	VoteGranted bool //是否给发送者投票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// todo 完善投票逻辑，比如候选人的term比选民的高，或者一样时，候选人的日志不少于选民
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		// 1.任期没有我的新，直接拒绝
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		// 2.如果对方term比我大，我就转为follower，然后直接投票
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		rf.role = Follower
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
	} else if rf.currentTerm == args.Term {
		// 3.在同一任期前提下，判断是否可以投票，之间没投过或者原本投给自己
		//论文原文是If votedFor is null or CandidateId,
		//and
		//candidate`s log is at least as up-to-date as receiver`s log,
		//grant vote($5.2,$5.4)
		//up-to-date的意思是，Raft 通过比较两份日志中的最后一条日志条目的索引和任期号来定义谁的日志更新。
		//如果两份日志最后条目的任期号不同，那么任期号大的日志更新
		//如果两份日志最后条目的任期号相同，那么谁的日志更长，谁就更新
		lastIndex := len(rf.logEntries) - 1
		lastTerm := rf.logEntries[lastIndex].Term
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
			(args.LastLogTerm > lastTerm || (args.LastLogTerm == lastTerm && args.LastLogIndex >= lastIndex)) {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			rf.lastHeartbeatTime = time.Now() //更新心跳时间，避免重复选举
		} else {
			reply.VoteGranted = false
		}
		reply.Term = rf.currentTerm
	}
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

// Leader向Folloers发送的信息的参数
type AppendEntriesArgs struct {
	// Your data here (2A, 2B)
	Term         int         //任期
	LeaderId     int         //Leader的ID,这样follower就能重定向到clients
	PrevLogIndex int         //Leader上一条日志的Index
	PrevLogTerm  int         //Leader上一条日志的Term
	Entries      []*LogEntry //要被存储的日志条目，对心跳来说是空
	LeaderCommit int         //Leader的commitIndex
}

// Leader向Folloers发送的信息的回复
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int  //leader的currentTerm
	Success bool //是否收到并成功处理(成功处理是后续内容，follower需要看自己是否有对应logIndex和LogTerm)
}

// 节点收到了来自leader的AppendEntries，是心跳或者日志增加，根据是不是空的判断
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// 1.任期没有我的新，直接拒绝
		reply.Term = rf.currentTerm
		reply.Success = false
	} else {
		// 2.如果对方term比我大，我就转为follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1 //只知道有新的任期了，但不知道给谁投票，先设为-1
			rf.role = Follower
		}
		// 现在term都一样
		// 3.重置心跳
		rf.lastHeartbeatTime = time.Now()
		if args.PrevLogIndex >= len(rf.logEntries) || rf.logEntries[args.PrevLogIndex].Term != args.PrevLogTerm {
			//一致性校验，看上一个日志条目一不一样
			//对应位置是空的或者存在，但是term不一样，这说明冲突了
			reply.Term = rf.currentTerm
			reply.Success = false
		} else {
			//如果上一个条目的index和term一样，那就可以更新了
			rf.logEntries = rf.logEntries[:args.PrevLogIndex+1] //左闭右开，将包括args.PrevLogIndex+1的后续内容删掉
			//也就是把前面对的部分留着，后面替换
			rf.logEntries = append(rf.logEntries, args.Entries...)

			// 4.报告正确
			if args.LeaderCommit > rf.commitIndex {
				//提交到min(leader的commitIndex，本地最新日志索引)
				lastIndex := len(rf.logEntries) - 1
				newCommit := int(math.Min(float64(args.LeaderCommit), float64(lastIndex)))
				if newCommit > rf.commitIndex {
					rf.commitIndex = newCommit
					//非常重要，启动apply线程，提交日志到状态机
					go rf.applyLogs()
				}
			}
			reply.Term = rf.currentTerm
			reply.Success = true
		}
	}
}

// Raft 协议要求：一旦 commitIndex 前进，就要把新提交的日志通过 applyCh 发送给上层服务（kvserver）。
// 在以下地方使用applyLogs
// AppendEntries 中更新 commitIndex 后
// Start() 成功 append 日志后（如果是本地 Leader，可能立即提交）
// Leader 在收到多数派 AE 回复后更新 commitIndex 时
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//将还没apply应用的日志，不断发送到上层服务，告诉它你这个命令集群已经接收了，你可以执行了
	//一直到commitIndex，commitIndex就是集群已经取得一致性的日志最大索引，根据规则，取得一致性的日志，它之前的日志也一定取得一致性
	for rf.lastApplied < rf.commitIndex {
		rf.lastApplied++
		logEntry := rf.logEntries[rf.lastApplied]
		// 解码
		var command interface{}
		if logEntry.Command != nil {
			buf := bytes.NewBuffer(logEntry.Command)
			dec := labgob.NewDecoder(buf)
			dec.Decode(&command)
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: rf.lastApplied,
		}
		go func() {
			rf.applyCh <- msg
		}()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//扩展sendAppendEntries的结果意思
	//不仅没发送成功算失败，发送过去了，被拒绝，也算失败，所以要加代码
	if !ok {
		return false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	//推送日志条目，结果对面的term比我高，那我只能转变为Follower，然后term和它一致
	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.currentTerm = reply.Term
		return false
	}
	//我不是Leader了，或者它的term和我不一样，也不行
	if rf.role != Leader || reply.Term != rf.currentTerm {
		return false
	}

	if reply.Success {
		//如果推送日志，对面成功接收了
		//那就可以更新leaderMenu了
		leaderMenu.mu.Lock()
		leaderMenu.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		leaderMenu.nextIndex[server] = leaderMenu.matchIndex[server] + 1
		leaderMenu.mu.Unlock()
		//尝试推进commitIndex
		rf.tryAdvanceCommitIndex()
	} else {
		//如果推送日志，对面拒绝了，那说明有冲突
		//要修改leaderMenu
		leaderMenu.mu.Lock()
		if leaderMenu.nextIndex[server] > 1 {
			leaderMenu.nextIndex[server]--
		}
		leaderMenu.mu.Unlock()
	}
	return true
}

func (rf *Raft) tryAdvanceCommitIndex() {
	//从当前commitIndex+1开始，向后找可以提交的最高index
	// *****************----
	// *是已提交的，-是还没提交的，总长度是len(rf.logEntries)
	// 本方法就是通过matchIndex来看，leader的commitIndex
	for N := rf.commitIndex + 1; N < len(rf.logEntries); N++ {
		if rf.logEntries[N].Term != rf.currentTerm {
			continue //只提交当前任期的日志，论文要求
		}

		count := 1
		leaderMenu.mu.Lock()
		for i := range rf.peers {
			//自己不算，已经+1了
			if i == rf.me {
				continue
			}
			//排除自己的前提下，如果有人的匹配索引比本N高
			if leaderMenu.matchIndex[i] >= N {
				count++
			}
		}
		leaderMenu.mu.Unlock()
		//取得多数了，可以把commitIndex推到这个N
		if count*2 > len(rf.peers) {
			rf.commitIndex = N
			go rf.applyLogs()
		} else {
			break
		}
	}
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
	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}
	//本节点是Leader,现在来了个条命令，要把它存到日志里
	//先编码为字节数组
	var buf bytes.Buffer
	encoder := labgob.NewEncoder(&buf)
	err := encoder.Encode(command)
	if err != nil {
		return -1, rf.currentTerm, false
	}
	//然后创建日志条目
	entry := &LogEntry{
		Term:    rf.currentTerm,
		Command: buf.Bytes(),
	}
	index := len(rf.logEntries)
	//先添加到自己的日志上
	rf.logEntries = append(rf.logEntries, entry)
	//然后把这条日志条目复制到Followers的日志上
	go rf.replicateLog(index)
	return index, rf.currentTerm, true
}

// Leader向Followers推送日志
func (rf *Raft) replicateLog(startIndex int) {
	for {
		//只有Leader有资格复制日志，有可能刚执行replicateLog时，就触发选举，换人了，所以在这判断一下
		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		leaderMenu.mu.Lock()
		nextIndex := make([]int, len(rf.peers))
		copy(nextIndex, leaderMenu.nextIndex)
		leaderMenu.mu.Unlock()

		var wg sync.WaitGroup
		for i := range rf.peers {
			//自己不用给自己发
			if i == rf.me {
				continue
			}
			prevIndex := nextIndex[i] - 1
			//要对prevIndex判断一下
			if prevIndex < 0 || prevIndex >= len(rf.logEntries) {
				//这个Follower的prevIndex不对，跳过
				rf.mu.Unlock()
				continue
			}
			args := &AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: prevIndex,
				PrevLogTerm:  rf.logEntries[prevIndex].Term,
				Entries:      rf.logEntries[nextIndex[i]:],
				LeaderCommit: rf.commitIndex,
			}
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				var reply AppendEntriesReply
				if rf.sendAppendEntries(server, args, &reply) {

				}
			}(i)
		}
		//等都执行完了再说
		rf.mu.Unlock()
		wg.Wait()

		rf.mu.Lock()
		if rf.commitIndex >= startIndex {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
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

// The ticker go routine starts a new election if this peer hasn't received heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Follow随机等待的下限必须高于 Leader 定时心跳的间隔
		// 在此基础上，如果定时心跳与随机等待差距不大，有可能误判没有Leader，从而发起新选举，如果随机等待高定时心跳太多，又会拖慢程序恢复速度
		// Leader 心跳间隔：通常 50~100ms
		// Follower 选举超时：通常 150~300ms
		// 这样设计是为了保证：
		// 	Leader 能在 follower 超时前发送至少 1~2 次心跳
		// 	避免网络抖动或短暂延迟导致不必要的选举
		// 	保证系统稳定，减少 term 频繁增长
		rf.mu.Lock()
		if rf.role == Leader {
			//如果我是Leader,要定时发送心跳
			rf.mu.Unlock()
			//最好要用go启动，这是非阻塞的，发起一个心跳后，就直接等待100ms了，无需等待心跳完成
			//如果不加go，那就是阻塞式，会导致必须先发送心跳，然后等待接收回复，接收完，再等100ms，整个时间太长了，会导致有些Follower误判没有Leader，从而发起新的选举
			//会警告 warning: term changed even though there were no failures  ... Passed
			go rf.broadcastHeartbeats()
			time.Sleep(50 * time.Millisecond)
		} else {
			//如果我不是Leader,那就随机等待，等待后看超不超时，最低限度150ms，最高300ms
			rf.mu.Unlock()
			//生成一个固定的选举超时时间（150~300ms）
			electionTimeout := time.Duration(150+rand.Intn(150)) * time.Millisecond
			time.Sleep(electionTimeout)
			//检查是否超时,超时了就发起选举
			if time.Since(rf.lastHeartbeatTime) >= electionTimeout {
				go rf.startElection()
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = Candidate
	rf.lastHeartbeatTime = time.Now()

	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	//发起选举时，要先投自己一票
	votes := 1
	for i := range rf.peers {
		//自己不用给自己发
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//处理回复
				if reply.VoteGranted && rf.role == Candidate && rf.currentTerm == args.Term {
					//当本次有人投我一票，且我还是Candidate，且我们的term一样
					votes++
					if votes*2 > len(rf.peers) {
						//票数过半，成功当选，发送广播通知各节点
						rf.role = Leader
						go rf.broadcastHeartbeats()
						//初始化leaderMenu
						leaderMenu.mu.Lock()
						leaderMenu.nextIndex = make([]int, len(rf.peers))
						leaderMenu.matchIndex = make([]int, len(rf.peers))
						for i := range leaderMenu.nextIndex {
							leaderMenu.nextIndex[i] = len(rf.logEntries)
							leaderMenu.matchIndex[i] = 0
						}
						leaderMenu.mu.Unlock()
					}
				} else if reply.Term > rf.currentTerm {
					//没同时满足上述，而且对方的Term比我大，本着Term大的优先，所以我变成Follower
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
				}
			}
		}(i)
	}
}

func (rf *Raft) broadcastHeartbeats() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logEntries) - 1,
		PrevLogTerm:  rf.logEntries[len(rf.logEntries)-1].Term,
		Entries:      []*LogEntry{},
		LeaderCommit: rf.commitIndex,
	}
	for i := range rf.peers {
		//自己不用给自己发
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply AppendEntriesReply
			if rf.sendAppendEntries(server, args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				//处理回复
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
				}
			}
		}(i)
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
	rf.lastHeartbeatTime = time.Now()
	rf.role = Follower //最开始都是Follower
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.logEntries = make([]*LogEntry, 0)
	rf.logEntries = append(rf.logEntries, &LogEntry{Term: 0, Command: nil}) //第0条占位，因为logEntries的索引从1开始
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//fmt.Println("a server has made!")
	return rf
}
