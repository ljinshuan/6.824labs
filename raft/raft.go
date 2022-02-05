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
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824labs/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// TODO jinshuan.li 2022/2/5 8:47 AM  数据结构
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	nodeMap           map[int]*NodeInfo
	currentTermId     int       //当前termId
	voteFor           *VoteInfo //当前term的投票信息
	role              Role      //当前状态
	lastHeartbeatTime time.Time
	heartbeatTimeout  time.Duration
	leaderId          int //leaderId
	nodeInfo          *NodeInfo
	meu               sync.Mutex
}

type NodeInfo struct {
	NodeId   int
	NodeName interface{}
	Enable   bool
}

type Role int

const (
	FOLLOWER Role = iota
	CANDIDATE
	LEADER
)

//投票信息
type VoteInfo struct {
	candidateId int
	termId      int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	//  获取当前raft节点状态
	return rf.currentTermId, rf.role == LEADER
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
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
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

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// TODO jinshuan.li 2022/2/5 8:47 AM  选举请求数据结构
	CurrentTermId int
	NodeId        int
	NodeName      interface{}
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Accept  bool
	Message string
	TermId  int
}

type RequestHeartbeat struct {
	CurrentTermId int
	NodeId        int
}

type RequestHeartbeatReply struct {
	CurrentTermId int
	NodeId        int
	Valide        bool
}

func (rf *Raft) RequestHeartbeat(args *RequestHeartbeat, reply *RequestHeartbeatReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if (args.CurrentTermId > rf.currentTermId && rf.role == CANDIDATE) || (rf.role == FOLLOWER && args.CurrentTermId >= rf.currentTermId) {
		Println("node:%v accept RequestHeartbeat:%v\n", rf.nodeInfo, args)
		oldId := rf.currentTermId
		rf.currentTermId = args.CurrentTermId
		rf.lastHeartbeatTime = time.Now()
		rf.voteFor = &VoteInfo{
			candidateId: args.NodeId,
			termId:      args.CurrentTermId,
		}
		rf.leaderId = args.NodeId
		rf.role = FOLLOWER

		reply.Valide = true
		reply.NodeId = rf.nodeInfo.NodeId
		reply.CurrentTermId = oldId

	} else if args.CurrentTermId < rf.currentTermId {
		//拒绝 这个心跳
		Println("reject RequestHeartbeat:%v\n", args)
		reply.Valide = false
		reply.NodeId = rf.nodeInfo.NodeId
		reply.CurrentTermId = rf.currentTermId
	} else {
		Println("error condition RequestHeartbeat %v %v \n", rf.nodeInfo, args)
		reply.Valide = false
		reply.NodeId = rf.nodeInfo.NodeId
		reply.CurrentTermId = rf.currentTermId
	}

}

//
// 收到投票信息时处理
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//  处理选举请求
	rf.mu.Lock()
	defer rf.mu.Unlock()

	Println("clientId:%v  receive RequestVote:%v\n", rf.me, args)
	if rf.currentTermId < args.CurrentTermId {
		//当前任期 比期望投票的任期小 同意投票
		reply.Accept = true
		reply.Message = "accept vote"
		rf.voteFor = &VoteInfo{
			candidateId: args.NodeId,
			termId:      args.CurrentTermId,
		}
		rf.currentTermId = args.CurrentTermId
		rf.leaderId = args.NodeId
		rf.role = FOLLOWER
	} else {
		//拒绝
		reply.Accept = false
		reply.Message = "less termId"
		reply.TermId = rf.currentTermId
	}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendRequestHeartbeat(server int, args *RequestHeartbeat, reply *RequestHeartbeat) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}

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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

const (
	kMinHeartbeatMs = 150
	kMaxHeartbeatMs = 300
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(rf.heartbeatTimeout)
		if time.Now().Sub(rf.lastHeartbeatTime) >= rf.heartbeatTimeout {
			//当前时间-上一次心跳时间 > 心跳超时时间 发起投票
			rf.sendRequestVote2Others()
		}
	}
}

func (rf *Raft) init() {
	//初始化心跳超时时间
	rf.role = FOLLOWER
	rf.heartbeatTimeout = time.Duration(rand.Intn(kMaxHeartbeatMs-kMinHeartbeatMs)+kMaxHeartbeatMs) * time.Millisecond
	//上一次心跳时间 为当前时间
	rf.lastHeartbeatTime = time.Now()
	rf.currentTermId = 0
	rf.nodeInfo = &NodeInfo{
		NodeId:   rf.me,
		NodeName: rf.peers[rf.me].GetEndName(),
		Enable:   true,
	}
	rf.nodeMap = map[int]*NodeInfo{}
	for i, peer := range rf.peers {
		rf.nodeMap[i] = &NodeInfo{
			NodeId:   i,
			NodeName: peer.GetEndName(),
			Enable:   true,
		}
	}
}

func (rf *Raft) sendRequestVote2Others() {
	rf.meu.Lock()
	defer rf.meu.Unlock()

	if rf.role == LEADER {
		//如果是leader直接跳过
		return
	}
	Println("node %v start to election leader \n", rf.nodeInfo)
	//状态转变
	rf.role = CANDIDATE
	rf.voteFor = &VoteInfo{
		candidateId: rf.nodeInfo.NodeId,
		termId:      rf.currentTermId,
	}
	rf.lastHeartbeatTime = time.Now()
	//增加任期
	rf.currentTermId += 1
	//自己有一票投自己
	voteAcceptNum := 1
	voteFailNum := 0

	for i, _ := range rf.nodeMap {
		if i == rf.me {
			continue
		}
		voteArgs := &RequestVoteArgs{
			CurrentTermId: rf.currentTermId,
			NodeId:        rf.nodeInfo.NodeId,
			NodeName:      rf.nodeInfo.NodeName,
		}
		requestVoteReply := &RequestVoteReply{}
		startTime := time.Now()
		vote := rf.sendRequestVote(i, voteArgs, requestVoteReply)
		if rf.role == FOLLOWER {
			//如果角色转变 直接break  发送投票请求期间可能收到心跳 从而导致角色变化 待处理
			Println("node %v change to FOLLOWER when vote.\n", rf.nodeInfo)
			break
		}
		endTime := time.Now()
		if vote && requestVoteReply.Accept {
			voteAcceptNum += 1
			Println("node %v ask for vote %v success cost:%v\n", rf.nodeInfo, i, endTime.Sub(startTime).Milliseconds())
		} else {
			if !vote {
				requestVoteReply.Message = "call fail"
				voteFailNum += 1
			}
			Println("node %v ask for vote %v fail %v cost:%v\n", rf.nodeInfo, i, requestVoteReply, endTime.Sub(startTime).Milliseconds())
		}
	}
	Println("node %v vote result. voteAcceptNum:%v voteFailNum:%v\n", rf.nodeInfo, voteAcceptNum, voteFailNum)
	if voteAcceptNum > (len(rf.peers)-voteFailNum)/2 {
		//超过一半投自己 开始转变角色
		rf.role = LEADER
		Println("node:%v become leader\n", rf.nodeInfo)
		rf.sendHeartbeat2Others()
	} else {
		//回退任期 回退角色
		rf.currentTermId -= 1
		rf.role = FOLLOWER
	}
}

func (rf *Raft) sendHeartbeat2Others() {

	go func() {
		for rf.role == LEADER {
			for i, _ := range rf.peers {
				if i == rf.me {
					rf.lastHeartbeatTime = time.Now()
					continue
				}
				//如果断开链接 同步调用会导致更新lastHeartbeatTime超时 从而触发重新选举
				go func(index int) {
					rf.sendRequestHeartbeat(index, &RequestHeartbeat{
						CurrentTermId: rf.currentTermId,
						NodeId:        rf.nodeInfo.NodeId,
					}, &RequestHeartbeat{})
				}(i)
			}
			//发完之后sleep一段时间
			time.Sleep(kMinHeartbeatMs / 2 * time.Millisecond)
		}
	}()
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

	// Your initialization code here (2A, 2B, 2C).
	// TODO jinshuan.li 2022/2/5 8:48 AM  初始化
	rf.init()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
