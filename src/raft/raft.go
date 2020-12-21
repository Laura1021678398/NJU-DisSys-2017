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
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

var FOLLWER = 1
var CANDIDATE = 2
var LEADER = 3

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Term    int // 日志条目的term
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[] ？我的节点编号？

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status            int // 用来记录当前节点的状态（一定要记录状态吗？代码有其他的写法吗）
	follower          chan bool
	candidate         chan bool
	leader            chan bool
	timer             *time.Timer
	startElectionChan chan bool
	beLeader          chan bool

	electionTimeout  int // election timeout的时间长度（注意初始化）The election timeout is the amount of time a follower waits until becoming a candidate.
	heartBeatTimeout int // leader存在的时候，每个节点在heartBeatTimeout的时间内需要收到leader的消息

	currentTerm int // 服务器已知最新的任期（服务器首次启动时初始化为0）
	votedFor    int // 当前任期内收到选票的候选者ID，如果没有投给任何候选者则为空
	log         []Entry

	commitedIndex int // 已知已提交的最高的日志条目的索引（初始值为0，单调递增）
	lastApplied   int // 已经被应用到状态机的最高的日志条目的索引（初始值为0，单调递增）

	nextIndex  []int // 对于每一台服务器，发送到该服务器的下一个日志条目的索引（初始值为领导者最后的日志条目的索引+1？？)
	matchIndex []int // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引（初始值为0，单调递增）
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int // 候选人的任期号
	CandidateID  int // 请求选票的候选人的ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

type AppendEntriesArgs struct {
	Term          int
	LeaderID      int // 领导者ID
	PrevLogIndex  int
	PrevLogTerm   int
	Entries       []Entry
	LeaderCommmit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
// 接收请求投票RPC
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	log.Printf("raft%v RequestVote()", rf.electionTimeout)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// TODO 这里还要判断候选人日志是否和自己一样新
		if args.Term > rf.currentTerm {
			reply.VoteGranted = true
			rf.timer.Stop()
			rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
			return
		} else if args.LastLogIndex <= len(rf.log) {
			reply.VoteGranted = true
			// 节点投票以后就要重新开始election timeout计时器
			rf.timer.Stop()
			rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
			return
		}
	}
	reply.VoteGranted = false
}

// leader发送的添加条目/heartbeat RPC
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {

}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
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
// server's port is peers[me]. 所有server的peer数组的顺序相同
// all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make()应该很快返回，因此对任何需要长时间运行的工作都应该使用goroutine。
// Make() must return quickly, so it should start goroutines
// for any long-running work.
// 修改Make()来创建一个background goroutine，当节点在一段时间没有
// 收到任何一个节点发来的消息时就开始一轮选举。
// Modify Make() to create a background goroutine that
// starts an election by sending out RequestVote RPC
// when it hasn’t heard from another peer for a while.

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// The election timeout is randomized to be between 150ms and 300ms.
	rf.status = FOLLWER
	rf.electionTimeout = rand.Intn(150) + 150
	rf.startElectionChan = make(chan bool)
	rf.beLeader = make(chan bool)

	rf.votedFor = -1

	rf.currentTerm = 0
	rf.log = make([]Entry, 0)

	rf.commitedIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startServer()
	return rf
}

// startServer 表示开启raft服务器，在这里控制raft状态的切换，当处于不同的状态的时候，raft server的处理模式应该不同
func (rf *Raft) startServer() {
	log.Printf("raft%v: start startServer()", rf.electionTimeout)
	rf.timer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond) // 开启定时器
	for {                                                                          // for循环不能阻塞，因为要一直判断是否超时
		select {
		case <-rf.timer.C: // 定时器超时或选举超时
			log.Printf("raft%v: startServer(): rf.timer.C 定时器超时", rf.electionTimeout)
			// 开始一轮选举
			rf.mu.Lock()
			rf.status = CANDIDATE
			rf.mu.Unlock()
			go rf.startElection()
		case <-rf.startElectionChan:
			// 开始一轮选举
			log.Printf("raft%v: startServer(): rf.startElectionChan 选举超时", rf.electionTimeout)
			go rf.startElection()
		case <-rf.beLeader:
			// 当前节点成为leader
			go rf.startAppend()
		}
	}
}

// Leader发送appendEntry消息给其他节点
func (rf *Raft) startAppend() {
	log.Printf("raft%v: startAppend()", rf.electionTimeout)
}

// 服务器在follower和candidate下都要判断是否发生超时（当接收到指定的消息以后就重置计时器）
// 需要监听timer的消息，一旦超时，就开始一轮选举（状态转变为candidate）

// 开始一次选举
func (rf *Raft) startElection() {
	log.Printf("raft%v : startElection()", rf.electionTimeout)
	t := time.NewTimer(time.Duration(rf.electionTimeout) * time.Second) // 开始选举以后新开一个计时器
	// 向所有peer发送RPC请求（不包括自己）
	args := RequestVoteArgs{
		Term:         rf.currentTerm + 1,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.log), // TODO 候选人最后日志条目的索引值暂时不知道是什么。。
	}
	if len(rf.log) == 0 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
	}
	var nVote = 1             // 候选人获得的选票数量
	end := make(chan bool, 1) // 使用end限制只发送一次rpc
	end <- true
	for {
		select {
		case <-t.C: // 选举超时，开始下一轮选举
			log.Printf("raft%v: startElection(): t.C 选举超时", rf.electionTimeout)
			rf.startElectionChan <- true
			return
		case <-end:
			for index, _ := range rf.peers {
				if index == rf.me {
					continue
				}
				// peer的类型*labrpc.ClientEnd
				go func(server int, args RequestVoteArgs) {
					// 对每台服务器请求选票都是使用一个goroutine
					// 发送RPC之后处理reply，计算获得的选票数量，从而确定是否赢得选举
					// 先默认只请求一次吧。这里如何改成请求多次？
					reply := &RequestVoteReply{}
					ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
					if ok {
						rf.mu.Lock()
						if reply.VoteGranted {
							nVote += 1
							log.Printf("raft%v: startElection(): nVote=%v", rf.electionTimeout, nVote)
							if nVote == len(rf.peers)/2+1 {
								// TODO 节点转变为leader
								log.Printf("raft%v: beLeader", rf.electionTimeout)
								rf.status = LEADER
								rf.beLeader <- true
							}
						} else if reply.Term > rf.currentTerm {
							// 当候选人发现有节点比自己的term高
							rf.currentTerm = reply.Term
							rf.timer.Stop()
							rf.status = FOLLWER
							rf.timer.Reset(time.Duration(rf.electionTimeout) * time.Second)
						}
						rf.mu.Unlock()
					}
				}(index, args)
			}
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// 将rpc的返回值赋给reply
// fills in *reply with RPC reply, so caller should
// pass &reply.
// args和reply的类型必须和handler function中的一样（包括是否使用指针）
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
// 如果RPC发送成功返回true
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
// 发送请求投票RPC
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
