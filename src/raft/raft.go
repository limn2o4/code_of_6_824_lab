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
	//	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
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

const (
	FOLLOWER  int = 0
	CANDIDATE     = 1
	LEADER        = 2
)

type Log struct {
	logId int32
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
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentStatus int //server status
	currentTerm   int

	lastHeartBeatTime int32 //time stamp when recive heart beat from leader
	electionTimeout   int32 //time elapse define how long a server will start election without reciving heart beat

	votedFor int
	log      []Log

	commitIndex    int32
	lastAppliedIdx int32

	nextIndex  []int32
	matchIndex []int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()

	term = rf.currentTerm
	isleader = rf.currentStatus == LEADER
	rf.mu.Unlock()
	return term, isleader
}

func (rf *Raft) SetTerm(term int) {
	rf.mu.Lock()
	rf.currentTerm = term
	rf.mu.Unlock()
}

func (rf *Raft) SetState(State int) {
	rf.mu.Lock()
	rf.currentStatus = State
	rf.mu.Unlock()
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
	Term         int
	CandidateId  int
	LastLogIndex int32
	LastLogTerm  int32
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

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	nowTerm, _ := rf.GetState()
	if args.Term < nowTerm {
		//reject
		reply.Term = nowTerm
		reply.VoteGranted = false
		return
	}
	rf.mu.Lock()
	nowVoteFor := rf.votedFor

	if nowVoteFor == -1 || nowVoteFor == args.CandidateId {
		reply.Term = nowTerm
		reply.VoteGranted = true
		// granted vote
		rf.votedFor = args.CandidateId
	}
	rf.mu.Unlock()
	return
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

type AppendEntriesReq struct {
	Term         int
	LeaderId     int32
	PrevLogIndex int32
	PrevLogTerm  int32
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//request only send by leader

func (rf *Raft) sendAppendEntries(server int, req *AppendEntriesReq, resp *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", req, resp)
	return ok
}

func (rf *Raft) sendHeatBeat() bool {
	curTerm, isleader := rf.GetState()
	if !isleader {
		return false
	}
	for _, cli := range rf.peers {
		req := &AppendEntriesReq{}
		req.Term = curTerm
		req.Entries = make([]Log, 0)

		resp := &AppendEntriesReply{}
		go func(curCli *labrpc.ClientEnd) {
			curCli.Call("Raft.AppendEntries", req, resp)
			if !resp.Success {
				log.Println("call heat beat failed")
				//TODO handle haerted failure
			}
		}(cli)
	}
	return true
}

//handel hb in lab 2a
func (rf *Raft) AppendEntries(req *AppendEntriesReq, resp *AppendEntriesReply) bool {
	curTerm, _ := rf.GetState()
	resp.Success = true
	if req.Term < curTerm {
		//reject from outdate request
		resp.Term = curTerm
		resp.Success = false
		return true
	}
	if len(req.Entries) == 0 {
		// heart beats request
		rf.mu.Lock()
		rf.currentTerm = req.Term                       //update term
		rf.lastHeartBeatTime = int32(time.Now().Unix()) // update heart beat ts

		//reset
		rf.currentStatus = FOLLOWER
		rf.electionTimeout = GetTimeOut()
		rf.votedFor = -1
		rf.mu.Unlock()
	}
	return true
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

func GetTimeOut() int32 {
	return int32((rand.Intn(2000)+1000)%3000) * 1000 * 1000
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		var timeout int32 = GetTimeOut()
		rf.mu.Lock()
		timeout = rf.electionTimeout
		rf.mu.Unlock()
		//do sleep

		time.Sleep(time.Duration(timeout))

		curTime := int32(time.Now().Unix())
		rf.mu.Lock()
		lastTime := rf.lastHeartBeatTime
		timeout = rf.electionTimeout
		curStat := rf.currentStatus
		rf.mu.Unlock()
		log.Printf("%d - %d < %d\n", curTime, lastTime, timeout)
		if curStat != LEADER && curTime-lastTime >= timeout {
			rf.handelElection()
		}

	}
}
func (rf *Raft) handleFollower() {

}
func (rf *Raft) handelLeader() {
	for rf.killed() == false {
		rf.sendHeatBeat()
		time.Sleep(time.Millisecond * 100)
	}

}

func (rf *Raft) handelElection() bool {
	// 1. state transition
	rf.mu.Lock()
	rf.currentStatus = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	newTerm := rf.currentTerm
	rf.lastHeartBeatTime = int32(time.Now().Unix())
	rf.mu.Unlock()
	// 2. send vote request
	var voteCnt int32 = 0
	for _, cli := range rf.peers {
		req := &RequestVoteArgs{}
		resp := &RequestVoteReply{}
		req.Term = newTerm

		go func(curCli *labrpc.ClientEnd) {
			curCli.Call("Raft.RequestVote", req, resp)
			if !resp.VoteGranted {
				log.Println("vote not granted!")
				//TODO handle haerted failure
			} else {
				atomic.AddInt32(&voteCnt, int32(1))

			}
		}(cli)
	}
	total_peers := len(rf.peers)
	//3. cnt vote
	for rf.killed() == false {
		//vote cnt process
		time.Sleep(time.Millisecond * 50)
		//check current state
		rf.mu.Lock()
		now_state := rf.currentStatus
		rf.mu.Unlock()
		if now_state != CANDIDATE {
			// some other raft win election
			return true
		}
		vote_now := int(atomic.LoadInt32(&voteCnt))
		nowTime := int32(time.Now().Unix())
		log.Printf("candidate %d cnt vote : %d/%d\n using %d before %d", rf.me, vote_now, total_peers, (nowTime - rf.lastHeartBeatTime), rf.electionTimeout)
		if vote_now > total_peers/2+(total_peers%2) {
			// win election
			rf.mu.Lock()
			rf.currentStatus = LEADER
			rf.mu.Unlock()
			return true
		} else if nowTime-rf.lastHeartBeatTime >= rf.electionTimeout {
			return false
		}
	}
	return false

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

	rf.currentStatus = FOLLOWER
	rf.electionTimeout = GetTimeOut()
	rf.lastHeartBeatTime = 0
	rf.votedFor = -1
	rf.currentTerm = 0

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.handelLeader()
	log.Printf("raft inst created: id : %d, init timeout : %d\n", rf.me, rf.electionTimeout)
	return rf
}
