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
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

const FOLLWER = 1
const CANDIDATE = 2
const LEADER = 3

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
	Index   int
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
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	status int // 用来记录当前节点的状态
	ftimer *time.Timer
	ctimer *time.Timer
	start  chan bool
	f2c    chan bool
	c2c    chan bool
	c2l    chan bool
	c2f    chan bool
	l2f    chan bool

	applyChan        chan ApplyMsg
	electionTimeout  int // election timeout的时间长度（注意初始化）The election timeout is the amount of time a follower waits until becoming a candidate.
	heartBeatTimeout int // leader存在的时候，每个节点在heartBeatTimeout的时间内需要收到leader的消息

	currentTerm int // 服务器已知最新的任期（服务器首次启动时初始化为0）
	votedFor    int // 当前任期内收到选票的候选者ID，如果没有投给任何候选者则为空
	Log         []Entry

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
	term = rf.currentTerm
	if rf.status == LEADER {
		isleader = true
	}
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

type RequestVoteArgs struct {
	// Your data here.
	Term         int // 候选人的任期号
	CandidateID  int // 请求选票的候选人的ID
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
}

type RequestVoteReply struct {
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
	// log.Printf("raft%v RequestVote() from %v have votedFor %v ", rf.me, args.CandidateID, rf.votedFor)

	reply.Term = rf.currentTerm
	// TODO 会不会出现这样一种情况？
	// 有一个节点很久没有收到消息，因此变成候选者，但是此时leader也没发生变化，因此这个候选者的term变大了，然后leader发现一个比自己大的term的节点
	// 因此变为follower，然后重新选举？这样的话岂不是很容易发生选举现象
	if args.Term > rf.currentTerm { // 接收到更大的term，变为跟随者
		// log.Printf("raft%v RequestVote() 收到更大的term %v", rf.me, args.CandidateID)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		if rf.status == LEADER { // 所有服务器需遵守的规则：如果当前节点是leader，却收到了其他后候选人发来的消息，如果候选人的term大于当前节点，则当前节点变为follower，但是一般不会发生这种情况
			rf.status = FOLLWER
			rf.l2f <- true
		} else if rf.status == CANDIDATE {
			rf.status = FOLLWER
			rf.c2f <- true
		}
	}
	if args.Term < rf.currentTerm {
		// log.Printf("raft%v RequestVote() 收到更小的term %v", rf.me, args.CandidateID)
		reply.VoteGranted = false
	} else if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		// log.Printf("raft%v RequestVote() from %v have votedFor %v %v %v ", rf.me, args.CandidateID, rf.votedFor, args.LastLogIndex, len(rf.log))
		// 如果两份日志最后的条目的任期号不同，那么任期号大的日志更加新；
		// 如果两份日志最后的条目任期号相同，那么日志比较长的那个就更加新。
		if len(rf.Log) == 0 { // 如果当前服务器没有日志条目的话，肯定没有其他服务器新或者一样新
			rf.ftimer.Stop()
			rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateID
		} else {
			if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term { // 发送过来的日志的最后一个条目任期号大
				rf.ftimer.Stop()
				rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
				reply.VoteGranted = true
				rf.votedFor = args.CandidateID
			} else if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term { // 任期号相同
				if args.LastLogIndex >= len(rf.Log) {
					rf.ftimer.Stop()
					rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
					reply.VoteGranted = true
					rf.votedFor = args.CandidateID
				} else {
					reply.VoteGranted = false
				}
			}
		}
		// if args.LastLogIndex == 0 { // 当候选者没有日志的时候
		// 	if len(rf.Log) == 0 { // 当前服务器也没有日志，则一样新
		// 		rf.ftimer.Stop()
		// 		rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
		// 		reply.VoteGranted = true
		// 		rf.votedFor = args.CandidateID
		// 	} else {
		// 		reply.VoteGranted = false
		// 	}
		// } else if args.LastLogTerm > rf.Log[len(rf.Log)-1].Term { // 候选者的最后一个日志条目的term更加新
		// 	rf.ftimer.Stop()
		// 	rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
		// 	reply.VoteGranted = true
		// 	rf.votedFor = args.CandidateID
		// } else if args.LastLogTerm == rf.Log[len(rf.Log)-1].Term {
		// 	if args.LastLogIndex >= len(rf.Log) {
		// 		rf.ftimer.Stop()
		// 		rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
		// 		reply.VoteGranted = true
		// 		rf.votedFor = args.CandidateID
		// 	} else {
		// 		reply.VoteGranted = false
		// 	}
		// }
	}
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
	rf.mu.Lock()

	index := -1
	term := -1
	isLeader := (rf.status == LEADER)

	if isLeader {
		term = rf.currentTerm
		index = len(rf.Log) + 1 // 新的logEntry的index是当前log的长度+1
		rf.Log = append(rf.Log, Entry{Index: index, Term: term, Command: command})
		log.Printf("raft%v Start() %v", rf.me, rf.Log)
	}

	defer rf.mu.Unlock()
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

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	// The election timeout is randomized to be between 150ms and 300ms.
	rf.electionTimeout = rand.Intn(150) + 150
	rf.heartBeatTimeout = rand.Intn(20)
	rf.start = make(chan bool, 1)
	rf.f2c = make(chan bool)
	rf.c2c = make(chan bool)
	rf.c2l = make(chan bool)
	rf.c2f = make(chan bool)
	rf.l2f = make(chan bool)

	rf.votedFor = -1

	rf.currentTerm = 0
	rf.Log = make([]Entry, 0)

	rf.commitedIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.applyChan = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startServer()
	return rf
}

func (rf *Raft) startServer() {
	// log.Printf("raft%v : startServer()", rf.me)
	rf.status = FOLLWER
	rf.start <- true
	for {
		select {
		case <-rf.start: // 使用channel来启动状态转变
			go rf.beFollower()
		case <-rf.f2c:
			go rf.beCandidate()
		case <-rf.c2l:
			go rf.beLeader()
		case <-rf.c2f:
			go rf.beFollower()
		case <-rf.l2f:
			go rf.beFollower()
		}
	}
}

// 处于Follower阶段时需要开启election timeout
// 由于follower是一个状态，因此要用for循环使自己一直在这个状态中
func (rf *Raft) beFollower() {
	// log.Printf("raft%v : beFollower 成为跟随者", rf.me)
	rf.ftimer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Millisecond)
	for {
		select {
		case <-rf.ftimer.C: // 当计时器超时，变为candidate
			// log.Printf("raft%v : heartbeat计时器超时", rf.me)
			rf.status = CANDIDATE
			rf.f2c <- true
			return
		}
	}
}

// candidate也是一个状态，因此要用for循环
func (rf *Raft) beCandidate() {
	log.Printf("raft%v : beCandidate 成为候选人", rf.me)
	// 成为candidate以后先开始一轮选举，如果选举超时，则向C2C中发送消息，重开一轮选举
	rf.ctimer = time.NewTimer(time.Duration(rf.electionTimeout) * time.Second)
	go rf.startElection()
	for {
		if rf.status == CANDIDATE {
			select {
			case <-rf.ctimer.C:
				if rf.status == CANDIDATE { // 这里用rf.status保证一下，防止出现已经选举结束了但是cancel()没有及时执行导致的第二次选举
					rf.electionTimeout = rand.Intn(150) + 150
					rf.ctimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
					go rf.startElection() // 开始一次选举（向所有的机器发送一次消息）
				}
				// default: // 这里如果不要default的话会不会被阻塞住？
			}
		} else {
			// log.Printf("raft%v : beCandidate 候选人结束 ", rf.me)
			return
		}
	}
}

func (rf *Raft) startElection() {

	rf.currentTerm += 1 // 重新开始一次选举以后，currentTerm值要加一
	rf.votedFor = rf.me

	rf.mu.Lock()
	args := RequestVoteArgs{ // TODO args发送的消息以后check一下有没有理解错
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: len(rf.Log),
	}
	if len(rf.Log) == 0 {
		args.LastLogTerm = 0
	} else {
		args.LastLogTerm = rf.Log[len(rf.Log)-1].Term
	}
	rf.mu.Unlock()

	nVote := 1
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int, args RequestVoteArgs) {
				reply := &RequestVoteReply{}
				ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
				if !ok {
					// log.Printf("raft%v 无法连接", server)
					return
				}
				if reply.VoteGranted { // 如果服务器给当前节点投票了
					rf.mu.Lock()
					nVote += 1
					if rf.status == CANDIDATE && nVote == len(rf.peers)/2+1 { // 当前节点收到的选票超过一半以后就可以变成leader了
						rf.status = LEADER
						rf.c2l <- true
					}
					rf.mu.Unlock()

				}
				if reply.Term > rf.currentTerm { // 如果当前节点发现有比自己更大的任期，结束选举变为follower
					rf.mu.Lock()
					if rf.status == CANDIDATE { // 由于可能在多个回复中发现自己的term很小，因此这里要阻止多次变成follower
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.status = FOLLWER
						rf.c2f <- true // 每次状态转换的时候都要注意修改status
					}
					rf.mu.Unlock()
				}
			}(i, args)
		}
	}
}

// func (rf *Raft) applyLog() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()

// 	baseIndex := rf.Log[0].Index

// 	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
// 		msg := ApplyMsg{}
// 		msg.Index = i
// 		msg.Valid = true
// 		msg.Command = rf.Log[i-baseIndex].Command
// 		rf.applyChan <- msg
// 	}
// 	rf.lastApplied = rf.commitIndex
// }

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	for i := rf.lastApplied + 1; i <= rf.commitedIndex; i++ {
		log.Printf("applylog")
		msg := ApplyMsg{}
		msg.Index = i - 1
		msg.Command = rf.Log[i-1].Command
		rf.applyChan <- msg
	}
	rf.lastApplied = rf.commitedIndex
	rf.mu.Unlock()
}

// 当一个服务器节点成为leader
// 向所有节点发送appendEntry的消息
// 成为leader以后首先初始化nextIndex和matchIndex，然后发送一个心跳
// commitIndex应该等于领导者当前最后一个条目的索引
// 接着再与其他节点对其log
func (rf *Raft) beLeader() {
	log.Printf("raft%v : beLeader 成为领导者", rf.me)
	for index, _ := range rf.nextIndex { // nextIndex的初始值为领导者最后的日志条目的索引
		rf.nextIndex[index] = len(rf.Log) + 1
	}
	for index, _ := range rf.matchIndex { // 对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引
		rf.matchIndex[index] = 0
	}
	rf.commitedIndex = len(rf.Log)
	for index, _ := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.startAppendEntries(index) // 开始向其他节点传递logEntry
	}
}

// leader发送的添加条目/heartbeat RPC
func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	// log.Printf("raft%v AppendEntries()", rf.me)
	// log.Printf("raft%v AppendEntries() currentLeader %v currentState %v args.PrevLogIndex %v", rf.me, args.LeaderID, rf.status, args.PrevLogIndex)
	// log.Printf("raft%v AppendEntries() rf.Log %v args.PrevLogIndex %v args.Entries %v ", rf.me, rf.Log, args.PrevLogIndex, args.Entries)
	reply.Term = rf.currentTerm
	if rf.status == FOLLWER && args.Term == rf.currentTerm {
		rf.ftimer.Stop()
		rf.ftimer.Reset(time.Duration(rf.electionTimeout) * time.Millisecond)
	}
	if args.Term < rf.currentTerm { // 领导者的任期小于接收者的任期
		reply.Success = false
	} else {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = -1
		}
		if rf.status == CANDIDATE { // 接收到当前任期的leader发来的添加条目RPC，如果此时是candidate，马上变成follower。有没有可能是leader？
			rf.status = FOLLWER
			rf.c2f <- true
		} else if rf.status == LEADER {
			rf.status = FOLLWER
			rf.l2f <- true
		}
		// 如果leader没有条目，且服务器也没有条目，追加条目并返回真
		if args.PrevLogIndex == 0 && len(rf.Log) == 0 {
			reply.Success = true
			if len(args.Entries) > 0 {
				// log.Printf("raft%v AppendEntries() %v %v ", rf.me, args.Entries, rf.Log)
			}
			rf.Log = append(rf.Log, args.Entries...)
		} else if args.PrevLogIndex > len(rf.Log) { // 接收者日志中没有包含这样一个条目
			reply.Success = false
		} else if rf.Log[args.PrevLogIndex-1].Term != args.PrevLogTerm { // 接收者日志中包含了，但是不对
			reply.Success = false
			rf.Log = rf.Log[:args.PrevLogIndex]
		} else {
			reply.Success = true
			if len(args.Entries) > 0 {
				log.Printf("raft%v AppendEntries() %v %v", rf.me, args.Entries, rf.Log)
			}
			rf.Log = append(rf.Log, args.Entries...)
		}
	}
	// 如果领导者的已知已提交的最高的日志索引条目的索引 大于 接收者已知已提交的最高的日志条目的索引，
	// 则把接收者的已知已提交的最高的日志条目的索引 重置为 领导者的已知已提交的最高的日志条目的索引 或者是 上一个新条目的索引 取两者的最小值
	if args.LeaderCommmit > rf.commitedIndex {
		rf.commitedIndex = Min(args.LeaderCommmit, len(rf.Log)-1)
		go rf.applyLog()
	}
	// log.Printf("raft%v currentLeader %v currentState %v args.PrevLogIndex %v reply.Success %v len(rf.log) %v", rf.me, args.LeaderID, rf.status, args.PrevLogIndex, reply.Success, len(rf.log))

}

// 向server发送appendEntriesRPC，不管是否发送heartbeat
func (rf *Raft) startAppendEntries(server int) {
	for {
		if rf.status == LEADER {
			rf.mu.Lock()
			args := AppendEntriesArgs{
				Term:          rf.currentTerm,
				LeaderID:      rf.me,
				PrevLogIndex:  rf.nextIndex[server] - 1,
				LeaderCommmit: rf.commitedIndex,
			}
			if args.PrevLogIndex <= len(rf.Log) && args.PrevLogIndex > 0 {
				args.PrevLogTerm = rf.Log[args.PrevLogIndex-1].Term
			} else {
				args.PrevLogTerm = 0
			}
			rf.mu.Unlock()
			if rf.nextIndex[server] <= len(rf.Log) {
				// TODO 这里修改以后可以过concurrentStarts的test，这里的index还是有问题，第一个entry没有传过去
				args.Entries = rf.Log[rf.nextIndex[server]-1:]
			}
			reply := &AppendEntriesReply{}
			ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
			if !ok {
				log.Printf("raft%v heartbeat发送失败 %v", rf.me, rf.status)
			} else {
				if !reply.Success { // 接收者返回假有两种情况：1.领导者任期小于接收者任期 2.接收者日志中没有包含这样一个条目
					if rf.status == LEADER && reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.status = FOLLWER
						rf.l2f <- true
					} else {
						rf.nextIndex[server]--
					}
					// log.Printf("raft%v false startAppendEntries() tmp:%v args.Entries:%v args.Entries:%v len(rf.log):%v", rf.me, rf.nextIndex, args.Entries, len(args.Entries), len(rf.Log))
				} else { // 成功以后更新nextIndex和matchIndex
					rf.nextIndex[server] += len(args.Entries)
					rf.matchIndex[server] = rf.nextIndex[server] - 1
					// 遍历所有的matchIndex，找到大多数的matchIndex[i]>=N成立的N
					temp := rf.matchIndex
					sort.Ints(temp)
					N := temp[len(rf.peers)/2-1]
					// log.Printf("raft%v true startAppendEntries() tmp:%v args.Entries:%v args.Entries:%v len(rf.log):%v", rf.me, rf.matchIndex, args.Entries, len(args.Entries), len(rf.Log))
					if N > rf.commitedIndex {
						if rf.Log[N-1].Term == rf.currentTerm {
							rf.commitedIndex = N
							go rf.applyLog()
						}
					}
				}
			}
		}
		t := time.NewTicker(8 * time.Millisecond) // 每隔一段时间发送心跳
		<-t.C
	}
}

func Max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func Min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
