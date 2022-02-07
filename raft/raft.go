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
	"context"
	"errors"
	"fmt"
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
	//选举相关
	nodeMap           map[int]*NodeInfo
	currentTermId     int       //当前termId
	voteFor           *VoteInfo //当前term的投票信息
	role              Role      //当前状态
	lastHeartbeatTime time.Time
	heartbeatTimeout  time.Duration
	leaderId          int //leaderId
	nodeInfo          *NodeInfo
	meu               sync.Mutex

	//日志相关数据
	LogInfo
	//主节点相关数据
	LeaderInfo

	//存储的日志
	logEntries []LogRecord
	//apply队列 apply之后把数据写到该chan中
	applyCh chan ApplyMsg

	commitCh chan LogRecord
}

type LogRecord struct {
	TermId   int
	Cmd      interface{}
	LogIndex int
}

//日志信息
type LogInfo struct {
	commitIndex      int
	lastAppliedIndex int
}

//leader需要维护的 针对每个follower 当前日志索引 以及下一个日志索引
type LeaderInfo struct {
	nextIndexArr  []int
	matchIndexArr []int
}

//节点信息
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
	// 选举请求数据结构
	CurrentTermId int
	NodeId        int
	NodeName      interface{}

	//最后一个日志的信息
	LastLogIndex int
	LastLogTerm  int
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
	LeaderCommit  int
}

type RequestHeartbeatReply struct {
	CurrentTermId int
	NodeId        int
	Valide        bool
}

type RequestAppendLog struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	LogRecord
	LeaderCommit int
}

type RequestAppendLogReply struct {
	CurrentTermId int
	Success       bool
}

func (rf *Raft) RequestHeartbeat(args *RequestHeartbeat, reply *RequestHeartbeatReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.CurrentTermId < rf.currentTermId {
		//拒绝 这个心跳
		Println("reject RequestHeartbeat nodeInfo:%v  currentTermId:%v role:%v args:%v \n", rf.nodeInfo, rf.currentTermId, rf.role, args)
		reply.Valide = false
		reply.NodeId = rf.nodeInfo.NodeId
		reply.CurrentTermId = rf.currentTermId

		return
	}

	switch rf.role {

	case LEADER:
		if args.CurrentTermId > rf.currentTermId {
			rf.followLeaderState(args, reply)
		} else {
			//TODO jinshuan.li 2022/2/5 19:58 一般情况下不会到这个状态 打日志排查错误
			Println("error condition RequestHeartbeat nodeInfo:%v  currentTermId:%v role:%v args:%v \n", rf.nodeInfo, rf.currentTermId, rf.role, args)
			reply.Valide = false
			reply.NodeId = rf.nodeInfo.NodeId
			reply.CurrentTermId = rf.currentTermId
			//出现这种情况 重新选举
			go rf.startLeaderElection()
		}
	case CANDIDATE:
		rf.followLeaderState(args, reply)
	case FOLLOWER:
		rf.followLeaderState(args, reply)
	}
}
func (rf *Raft) followLeaderState(args *RequestHeartbeat, reply *RequestHeartbeatReply) {
	//Println("node:%v accept RequestHeartbeat:%v\n", rf.nodeInfo, args)
	oldId := rf.currentTermId
	rf.currentTermId = args.CurrentTermId
	rf.lastHeartbeatTime = time.Now()
	rf.voteFor = &VoteInfo{
		candidateId: args.NodeId,
		termId:      args.CurrentTermId,
	}
	rf.leaderId = args.NodeId
	rf.role = FOLLOWER
	rf.commitIndex = args.LeaderCommit

	reply.Valide = true
	reply.NodeId = rf.nodeInfo.NodeId
	reply.CurrentTermId = oldId
}

//
// 收到投票信息时处理
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//  处理选举请求
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//Println("clientId:%v  currentTermId:%v  receive RequestVote:%v  \n", rf.me, rf.currentTermId, args)

	accept := false
	if rf.currentTermId < args.CurrentTermId {
		record := rf.getLastLog()
		term := args.LastLogTerm
		index := args.LastLogIndex
		accept = true
		if record.TermId > term {
			//比较最后一条日志的任期号 如果比候选人大 直接拒绝
			accept = false
			reply.Message = "record.TermId > term "
		} else if record.TermId == term && record.LogIndex > index {
			//如果相等 继续比较index
			accept = false
			reply.Message = "record.TermId == term && record.LogIndex > index"
		}
	} else {
		accept = false
		reply.Message = "less termId"
	}

	if accept {
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
		reply.TermId = rf.currentTermId
	}

}

func (rf *Raft) RequestAppendLog(args *RequestAppendLog, reply *RequestAppendLogReply) {

	Println("node %v get append request %v", rf.nodeInfo, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != FOLLOWER {
		reply.Success = false
		reply.CurrentTermId = rf.currentTermId
		return
	}
	accept := true
	if args.Term < rf.currentTermId {
		accept = false
	}
	errorMessage := ""
	if args.PrevLogIndex != 0 && args.PrevLogTerm != 0 {
		if len(rf.logEntries) < args.PrevLogIndex {
			//长度小于这个值 不可能有数据
			accept = false
			errorMessage = fmt.Sprintf("error 111 logEntries len %v prevLogIndex %v", len(rf.logEntries), args.PrevLogIndex)
		} else {
			record := rf.logEntries[args.PrevLogIndex-1]
			if record.TermId != args.PrevLogTerm {
				accept = false
				errorMessage = fmt.Sprintf("error 222 term logEntries len %v prevLogIndex %v", len(rf.logEntries), args.PrevLogIndex)
			}
		}

		if len(rf.logEntries) >= args.LogRecord.LogIndex {
			record := rf.logEntries[args.LogRecord.LogIndex-1]
			//这个index已经有log了 检查term
			if record.TermId != args.Term {
				//删除该数据
				Println("ndoe %v delete pre logs", rf.nodeInfo)
				rf.logEntries = rf.logEntries[0 : args.LogRecord.LogIndex-1]
			}
		}
	}

	if accept {
		Println("node %v accept the append log %v", rf.nodeInfo, args.LogRecord)
		rf.logEntries = append(rf.logEntries, LogRecord{
			TermId:   args.Term,
			Cmd:      args.Cmd,
			LogIndex: args.LogIndex,
		})
		Println("node %v append log in request %v len:%v", rf.nodeInfo, args.LogRecord, len(rf.logEntries))

		reply.Success = true
		reply.CurrentTermId = rf.currentTermId
	}

	if !accept {
		Println("node %v reject the append log %v %v", rf.nodeInfo, args.LogRecord, errorMessage)
		reply.Success = false
		reply.CurrentTermId = rf.currentTermId
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, args.LogRecord.LogIndex)
	}
	//通知应用
	//rf.applyNotifyCh <- struct{}{}
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
func (rf *Raft) sendRequestHeartbeat(server int, args *RequestHeartbeat, reply *RequestHeartbeatReply) bool {
	ok := rf.peers[server].Call("Raft.RequestHeartbeat", args, reply)
	return ok
}
func (rf *Raft) sendRequestApplyMessage(server int, args *RequestAppendLog, reply *RequestAppendLogReply) bool {
	ok := rf.peers[server].Call("Raft.RequestAppendLog", args, reply)
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
/**
Star 开始准备commit  返回值 1 当前命令在该节点应该提交的index  2 如果已经提交了 第二个参数返回当前term
3 是否是leader
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	term := rf.currentTermId

	if !rf.isLeader() {
		return -1, -1, false
	}
	// Your code here (2B).
	commandIdnex := len(rf.logEntries) + 1
	record := LogRecord{
		TermId:   rf.currentTermId,
		Cmd:      command,
		LogIndex: commandIdnex,
	}
	rf.logEntries = append(rf.logEntries, record)
	Println("node %v append log in start %v len:%v", rf.nodeInfo, record, len(rf.logEntries))

	return commandIdnex, term, rf.role == LEADER
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
			for true {
				rf.startLeaderElection()
				if rf.role != CANDIDATE {
					//只要没有选出来 一直选
					break
				}
			}

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

	// init for  log
	rf.logEntries = make([]LogRecord, 0)
	rf.LogInfo = LogInfo{
		commitIndex:      0,
		lastAppliedIndex: 0,
	}
	rf.LeaderInfo = LeaderInfo{
		nextIndexArr:  make([]int, len(rf.peers)),
		matchIndexArr: make([]int, len(rf.peers)),
	}
	rf.commitCh = make(chan LogRecord)
	rf.startApplyLogLoop()
}

func (rf *Raft) startLeaderElection() bool {
	rf.meu.Lock()
	defer rf.meu.Unlock()

	if rf.role == LEADER {
		//如果是leader直接跳过
		return true
	}
	electionTimeout := time.Duration(rand.Intn(kMaxHeartbeatMs-kMinHeartbeatMs)+kMaxHeartbeatMs) / 2 * time.Millisecond

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
	Println("node %v start to election leader use term %v\n", rf.nodeInfo, rf.currentTermId)
	for i, _ := range rf.nodeMap {
		if i == rf.me {
			continue
		}
		record := rf.getLastLog()
		voteArgs := &RequestVoteArgs{
			CurrentTermId: rf.currentTermId,
			NodeId:        rf.nodeInfo.NodeId,
			NodeName:      rf.nodeInfo.NodeName,
			LastLogIndex:  record.LogIndex,
			LastLogTerm:   record.TermId,
		}
		requestVoteReply := &RequestVoteReply{}
		//startTime := time.Now()
		vote, err := rf.sendRequestVoteWithTimeout(i, voteArgs, requestVoteReply, kMinHeartbeatMs/len(rf.peers))
		if err != nil {
			//超时
			vote = false
		}
		if time.Now().Sub(rf.lastHeartbeatTime) > electionTimeout {
			//选举超时 直接不选了
			if voteAcceptNum == 1 {
				rf.currentTermId -= 1
			}
			return false
		}
		if rf.role == FOLLOWER {
			//如果角色转变 直接break  发送投票请求期间可能收到心跳 从而导致角色变化 待处理
			Println("node %v change to FOLLOWER when vote.\n", rf.nodeInfo)
			return false
		}
		//endTime := time.Now()
		if vote && requestVoteReply.Accept {
			voteAcceptNum += 1
			//Println("node %v ask for vote %v success cost:%v\n", rf.nodeInfo, i, endTime.Sub(startTime).Milliseconds())
		} else {
			if !vote {
				requestVoteReply.Message = "call fail"
				voteFailNum += 1
			}
			//Println("node %v ask for vote %v fail %v cost:%v\n", rf.nodeInfo, i, requestVoteReply, endTime.Sub(startTime).Milliseconds())
			if requestVoteReply.TermId > rf.currentTermId {
				//如果返回值的termId比当前大  直接转换为follower
				rf.currentTermId = requestVoteReply.TermId
				rf.role = FOLLOWER
				Println("node %v change to FOLLOWER when vote.\n", rf.nodeInfo)
				return false
			}
		}
	}
	Println("node %v vote result. voteAcceptNum:%v voteFailNum:%v\n", rf.nodeInfo, voteAcceptNum, voteFailNum)
	if voteAcceptNum > (len(rf.peers)-voteFailNum)/2 && voteAcceptNum >= 2 {
		//超过一半投自己 开始转变角色
		rf.role = LEADER
		Println("node:%v become leader as term:%v %v  commitIndex:%v lastAppliedIndex:%v\n", rf.nodeInfo, rf.currentTermId, rf.logEntries, rf.commitIndex, rf.lastAppliedIndex)
		rf.initLeaderLogInfo()
		rf.sendHeartbeat2Others()
		rf.startAppendRequest()
		return true
	}
	if voteAcceptNum == 1 {
		rf.currentTermId -= 1
	}
	return false
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
					r := &RequestHeartbeatReply{}
					rf.sendRequestHeartbeat(index, &RequestHeartbeat{
						CurrentTermId: rf.currentTermId,
						NodeId:        rf.nodeInfo.NodeId,
						LeaderCommit:  rf.commitIndex,
					}, r)
					if r.CurrentTermId > rf.currentTermId {
						rf.currentTermId = r.CurrentTermId
						rf.role = FOLLOWER
					}
				}(i)
			}
			//发完之后sleep一段时间
			time.Sleep(kMinHeartbeatMs / 2 * time.Millisecond)
		}
	}()
}

func (rf *Raft) sendRequestVoteWithTimeout(i int, args *RequestVoteArgs, reply *RequestVoteReply, timeoutMs int) (bool, error) {
	timeout, _ := context.WithTimeout(
		context.Background(),
		time.Millisecond*time.Duration(timeoutMs),
	)
	chRet := make(chan bool)
	go func() {
		vote := rf.sendRequestVote(i, args, reply)
		chRet <- vote
	}()
	select {
	case ans := <-chRet:
		return ans, nil
	case <-timeout.Done():
		return false, errors.New("time out")
	}

}

func (rf *Raft) sendRequestApplyMessageTimeout(i int, args *RequestAppendLog, reply *RequestAppendLogReply, timeoutMs int) (bool, error) {
	timeout, _ := context.WithTimeout(
		context.Background(),
		time.Millisecond*time.Duration(timeoutMs),
	)
	chRet := make(chan bool)
	go func() {
		vote := rf.sendRequestApplyMessage(i, args, reply)
		chRet <- vote
	}()
	select {
	case ans := <-chRet:
		return ans, nil
	case <-timeout.Done():
		return false, errors.New("time out")
	}

}

func (rf *Raft) isLeader() bool {
	return rf.role == LEADER
}

func (rf *Raft) startAppendRequest() {

	for i, _ := range rf.nextIndexArr {
		go func(i int) {
			//只要是leader 一直执行
			hasOk := false
			for rf.isLeader() {

				shouldTryCommitIndex := rf.nextIndexArr[i]
				for len(rf.logEntries) > 0 && shouldTryCommitIndex >= 1 && shouldTryCommitIndex <= len(rf.logEntries)+1 {
					ok, outRange := rf.tryCommit(i, shouldTryCommitIndex)
					if !ok {
						if outRange && hasOk {
							break
						}
						shouldTryCommitIndex -= 1
						Println("try_commit fail node:%v index:%v len:%v", i, shouldTryCommitIndex, len(rf.logEntries))

					} else {
						Println("try_commit success node:%v index:%v len:%v", i, shouldTryCommitIndex, len(rf.logEntries))
						rf.matchIndexArr[i] = shouldTryCommitIndex
						logRecord := rf.logEntries[shouldTryCommitIndex-1]
						shouldTryCommitIndex += 1
						rf.nextIndexArr[i] = shouldTryCommitIndex
						//通知通道
						hasOk = true
						rf.checkCommit(logRecord)
					}
				}
				if shouldTryCommitIndex <= 1 {
					shouldTryCommitIndex = 1
				}
				rf.nextIndexArr[i] = shouldTryCommitIndex
				time.Sleep(50 * time.Millisecond)
			}
		}(i)
	}

}

func (rf *Raft) tryCommit(serverNode int, shouldTryCommitIndex int) (bool, bool) {

	if !rf.isLeader() {
		return false, false
	}
	if len(rf.logEntries) < shouldTryCommitIndex || shouldTryCommitIndex < 1 {
		return false, true
	}
	if serverNode == rf.me {
		//如果是自己 直接成功
		return true, false
	}

	//取上一条日志
	preRecord := LogRecord{
		TermId:   0,
		Cmd:      nil,
		LogIndex: 0,
	}
	logRecord := rf.logEntries[shouldTryCommitIndex-1]
	if logRecord.LogIndex-2 >= 0 && len(rf.logEntries) > 1 {
		preRecord = rf.logEntries[logRecord.LogIndex-2]
	}

	request := &RequestAppendLog{
		Term:         rf.currentTermId,
		LeaderId:     rf.nodeInfo.NodeId,
		PrevLogIndex: preRecord.LogIndex,
		PrevLogTerm:  preRecord.TermId,
		LogRecord:    logRecord,
		LeaderCommit: rf.commitIndex,
	}
	Println("%v sendRequestApplyMessageTimeout %v %v", rf.nodeInfo, rf.logEntries, request)
	reply := &RequestAppendLogReply{}
	ok, err := rf.sendRequestApplyMessageTimeout(serverNode, request, reply, kMinHeartbeatMs)
	if err != nil {
		//超时了 不处理
	}
	if reply.CurrentTermId > rf.currentTermId {
		rf.currentTermId = reply.CurrentTermId
		rf.role = FOLLOWER
		return false, false
	}
	if ok && reply.Success {
		return true, false
	}
	return false, false

}

func (rf *Raft) startApplyLogLoop() {

	go func() {
		for true {
			//等待通知
			for rf.commitIndex > rf.lastAppliedIndex && len(rf.logEntries) > rf.lastAppliedIndex {
				record := rf.logEntries[rf.lastAppliedIndex]
				Println("node %v apply log %v", rf.nodeInfo, record)
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      record.Cmd,
					CommandIndex: record.LogIndex,
				}
				rf.lastAppliedIndex += 1
			}
			time.Sleep(100 * time.Millisecond)
		}

	}()
}

func (rf *Raft) getLastLog() LogRecord {

	record := LogRecord{
		TermId:   0,
		Cmd:      nil,
		LogIndex: 0,
	}
	if len(rf.logEntries) > 0 {
		record = rf.logEntries[len(rf.logEntries)-1]
	}
	return record
}

func (rf *Raft) initLeaderLogInfo() {

	for i, _ := range rf.nextIndexArr {
		rf.nextIndexArr[i] = len(rf.logEntries) + 1
	}
	for i, _ := range rf.matchIndexArr {
		rf.matchIndexArr[i] = 0
	}
}

func (rf *Raft) checkCommit(logRecord LogRecord) {

	if logRecord.TermId != rf.currentTermId {
		return
	}
	if logRecord.LogIndex <= rf.commitIndex {
		return
	}
	count := 0
	for _, v := range rf.matchIndexArr {
		if logRecord.LogIndex <= v {
			count += 1
		}
	}
	if count > len(rf.matchIndexArr)/2 {
		rf.commitIndex = logRecord.LogIndex
	}
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
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	// 初始化
	rf.init()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
