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
	currentTerm       int       // 最新的任期，初始为0，然后单调递增
	votedFor          int       // 在最新的任期里，给哪个候选人投票
	role              Role      // 本server的角色，是leader还是follower还是candidates
	lastHeartbeatTime time.Time // 上次接收到心跳的时间
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
	Term        int //任期
	CandidateId int //候选人的ID
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
	// Your code here (2A, 2B).
	if args.Term < rf.currentTerm {
		// 1.任期没有我的新，直接拒绝
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else {
		if args.Term > rf.currentTerm {
			// 2.如果对方term比我大，我就转为follower(因为我可能是老Leader，现在是新任期，不可用了)
			rf.currentTerm = args.Term
			rf.votedFor = -1 //只知道有新的任期了，但不知道给谁投票，先设为-1
			rf.role = Follower
			reply.VoteGranted = false
		}
		if rf.currentTerm == args.Term {
			// 3.在同一任期前提下，判断是否可以投票，之间没投过或者原本投给自己
			if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				rf.lastHeartbeatTime = time.Now() //更新心跳时间，避免重复选举
			} else {
				reply.VoteGranted = false
			}
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
	Term     int //任期
	LeaderId int //Leader的ID,这样follower就能重定向到clients
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
		// 4.报告正确
		reply.Term = rf.currentTerm
		reply.Success = true
	}
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
		Term:     rf.currentTerm,
		LeaderId: rf.me,
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
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	//fmt.Println("a server has made!")
	return rf
}
