package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   初始化新的服务器
// rf.Start(command interface{}) (index, term, isleader)
//   在新日志条目上开始协议
// rf.GetState() (term, isLeader)
//   询问当前任期，以及是否为leader
// ApplyMsg
//	 每当一个新条目被提交到日志时，每个Raft peer应该向部署在同一台服务器上的服务(或测试人员)发送一个ApplyMsg

import (
	//	"bytes"
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

// 1. 测试人员要求领导者发送心跳rpc每秒不超过10次。
// 2. 确保不同peer的选举超时不总是同时触发，否则所有的同伴都只会投票给自己，没有人会成为领导者。
// 3. 因为测试者限制你每秒发送10次心跳，选举超时时间比论文的150到300毫秒要大，但也不是太大，因为那样你就可能在五秒钟内选不到领导人。
// 4. 如果出现分裂投票，领导人选举可能需要多轮
// (如果数据包丢失或候选人不幸地选择了相同的随机回退时间，就会发生这种情况)。
// 您必须选择足够短的选举超时(以及心跳间隔)
// 即使需要多轮选举，选举也很可能在5秒内完成。

const (
	heartbeatsPerSecond = 10
	baseTimeOut         = 250
	baseRange           = 150
)

const (
	heartbeatTO     = time.Second / heartbeatsPerSecond
	electionTO      = time.Second / heartbeatsPerSecond // 250 - 400
	appendEntriesTO = time.Second / 15
	snapShotTO      = appendEntriesTO
)

// 随机选举超时 250-400ms
func RandomElectionTimeOut() time.Duration {
	return time.Duration(rand.Int()%baseRange+baseTimeOut) * time.Millisecond
}

// 定义三种状态
type State byte

const (
	Follower State = iota
	Candidate
	Leader
)

func (s State) String() string {
	var str string
	switch s {
	case Follower:
		str = "Follower"
	case Candidate:
		str = "Candidate"
	case Leader:
		str = "Leader"
	}
	return str
}

// 因为每个Raft peer都意识到连续的日志条目提交后，peer应该发送一个ApplyMsg到在同一服务器上的服务(或测试者)
// 通过applyCh传递给Make()。设置CommandValid为true，表示ApplyMsg包含一个新提交的日志条目。

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

// 由领导调用以复制日志条目;也可用作心跳。
type AppendEntriesArg struct {
	Term         int     // leader 任期
	LeaderID     int     // 以便于客户端发送给follower请求后重定向到leader
	PrevLogIndex int     // 紧跟在新日志条目之前的日志条目索引
	PrevLogTerm  int     // prevLogIndex 的任期
	Entries      []Entry // 需要存储的日志条目 (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int     // leader 已提交索引
}

// 输出AppendEntries的条目
func (e AppendEntriesArg) String() string {
	if len(e.Entries) == 0 {
		return fmt.Sprintf("HEAT[PI%d,PT%d,LC%d]", e.PrevLogIndex, e.PrevLogTerm, e.LeaderCommit)
	}
	// return fmt.Sprintf("ARG[%d,%d,%d] %v", a.PrevLogIndex, a.PrevLogTerm, a.LeaderCommit, a.Entries)
	return fmt.Sprintf("AE[PI%d,PT%d,LC%d,%v] L%d", e.PrevLogIndex, e.PrevLogTerm, e.LeaderCommit, EntriesToString(e.Entries), len(e.Entries))
}

// 检查appendentriesargs
// 本机为leader且本机的任期等于args且前一日志索引等于该服务器前一日志
func (rf *Raft) checkAppendEntriesArg(server int, args *AppendEntriesArg) (ok bool) {
	ok = rf.state == Leader && args.Term == rf.CurrentTerm && args.PrevLogIndex == rf.prevLog(server).Index
	if !ok {
		rf.debug(dError, "AP old [%v,%v]<=[%v,%v]", rf.state, rf.CurrentTerm, Candidate, args.Term)
	}
	return
}

/*
接收者实现:
 1. 如果term < currentTerm则返回false
 2. 如果日志中不包含与prevLogTerm匹配的条目，则返回false
 3. 如果现有条目与新条目冲突(索引相同，但任期不同)，删除现有条目以及其后的一切
 4. 添加日志中没有的任何新条目
 5. 如果leaderCommit > commitIndex，则设置commitIndex = min(leaderCommit，最后一个新条目的索引)
*/
type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // follower的前一条日志是否匹配
	XIndex  int  // 该任期第一条目索引
	XTerm   int  // 冲突条目所在任期
	XLen    int  // 日志长度
}

func (e AppendEntriesReply) String() string {
	if !e.Success {
		return fmt.Sprintf("NAPP[T%v,XI%v,XT%v,XL%v]", e.Term, e.XIndex, e.XTerm, e.XLen)
	}
	return fmt.Sprintf("APP[T%v]", e.Term)
}

// 请求投票
type RequestVoteArgs struct {
	Term         int // 竞选者任期
	CandidatedId int // 竞选者要求投票Id
	LastLogIndex int // 竞选者最新条目索引
	LastLogTerm  int // 竞选者最新条目任期
}

func (e RequestVoteArgs) String() string {
	return fmt.Sprintf("[T%d,LI%d,LT%d]", e.Term, e.LastLogIndex, e.LastLogTerm)
}

// 如果该peer为竞选者并且该信息为当前任期所发出的，则返回成功(避免超时重传)
func (rf *Raft) checkRequestVoteArgs(server int, args *RequestVoteArgs) (ok bool) {
	ok = rf.state == Candidate && args.Term == rf.CurrentTerm
	if !ok {
		rf.debug(dError, "RV old [%v,%v,%v]<=[%v,%v,%v]", rf.state, rf.CurrentTerm, rf.lastLog().Index, Candidate, args.Term, args.LastLogIndex)
	}
	return
}

/*
1.如果 term < currentTerm false，即follower更新
2.如果votedFor为null或candidateId，并且candidate的日志至少和follower的日志一样新，投票
*/
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (e RequestVoteReply) String() string {
	if e.VoteGranted {
		return fmt.Sprintf("[T%d,G]", e.Term)
	}
	return fmt.Sprintf("[T%d,X-G]", e.Term)
}

type Entry struct {
	Index   int // for debug
	Term    int
	Command interface{}
}

func (e Entry) String() string {
	return fmt.Sprintf("[%v,%v]", e.Index, e.Term)
}

var logVerbose int

func init() {
	if os.Getenv("VERBOSE") == "2" {
		logVerbose = 2
	}
}

func EntriesToString(entries []Entry) string {
	if logVerbose == 2 {
		return fmt.Sprintf("E{%v}", entries)
	}
	if len(entries) > 1 {
		return fmt.Sprintf("E{%v->%v}", entries[0], entries[len(entries)-1])
	}
	return fmt.Sprintf("E{%v}", entries[0])
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // 其他peer的通信地址
	persister  *Persister          // 保存 peer 的持久化状态
	me         int                 // 该peerid
	dead       int32               // set by Kill()
	disconnect int32

	// state a Raft server must maintain.
	// 所有服务器上的持久状态:(在响应rpc之前在稳定存储上更新)

	CurrentTerm int     // 服务器见过的最新任期(在第一次引导时初始化为0，单调增加)
	VotedFor    int     // 投票给谁 (or null if none)
	Logs        []Entry // 日志条目;每个条目包含状态机命令，以及leader接收到条目时的任期(第一个索引为1)

	// Volatile state on all servers:

	state       State
	commitIndex int // 已知提交的最高日志条目的索引(初始化为0，单调增加)
	lastApplied int // 应用于状态机的最高日志项的索引(初始化为0，单调增加)

	//领导人的不稳定状态:(选举后重新初始化)
	//leader为每个follower维护一个nextIndex，这是leader将发送给该follower的下一个日志条目的索引。
	//在拒绝之后，leader递减nextIndex并重试AppendEntries RPC。最终，nextIndex将达到leader和follower日志匹配的点
	nextIndex      []int        // 对于每个服务器，发送到该服务器的下一个日志条目的索引(初始化为上一个日志索引+ 1)
	nextMatchIndex []int        // 对于每个服务器，已知在服务器上复制的最高日志条目的索引(初始化为0，单调增加)
	isInAppending  map[int]bool //标记是否正在追加日志中

	applyCh   chan ApplyMsg // 在每个peer上的applyCh上发送每个新提交的条目。
	applyCond *sync.Cond
	//计时器
	electionTO       time.Duration
	lastHeatbeatTime time.Time
	tick             *time.Ticker
	//快照所包括的条目信息
	lastIncludeIndex int
	lastIncludeTerm  int

	date []byte

	heatCnt int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	term = rf.CurrentTerm
	if rf.state == Leader {
		isleader = true
	}
	rf.mu.Unlock()
	return term, isleader
}

// 将Raft的持久状态保存到稳定的存储中，在那里它可以在崩溃和重新启动后被检索。关于什么应该是持久的，请参见论文的图2。
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.State())
}

func (rf *Raft) State() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Logs)
	data := w.Bytes()
	rf.debug(dPersist, "P T%v VF%v  %v", rf.CurrentTerm, rf.VotedFor, EntriesToString(rf.Logs))
	return data
}

// 从先前的持久状态恢复
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.debug(dPersist, "bootstrap without any state?")
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Entry
	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		rf.debug(dPersist, "Decode file")
	} else {
		rf.CurrentTerm = term
		rf.VotedFor = votedFor
		rf.Logs = logs
		rf.lastIncludeIndex = rf.Logs[0].Index
		rf.lastIncludeTerm = rf.Logs[0].Term
		rf.lastApplied = rf.lastIncludeIndex
		rf.debug(dPersist, "RP T%v VF%v [LI%d,LT%d] %v", rf.CurrentTerm, rf.VotedFor, rf.lastIncludeIndex, rf.lastIncludeTerm, EntriesToString(rf.Logs))
	}
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	return true
}

type InstallSnapShotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

func (e InstallSnapShotArgs) String() string {
	return fmt.Sprintf("T%d [%d,%d]", e.Term, e.LastIncludedIndex, e.LastIncludedTerm)
}

// 检查快照rpc返回信息
func (rf *Raft) checkInstallSnapShotArgs(args *InstallSnapShotArgs) (ok bool) {
	//发送者此时仍是leader，并且不是过时消息，且此时的提交范围未改变
	ok = rf.state == Leader && rf.CurrentTerm == args.Term && rf.lastIncludeIndex == args.LastIncludedIndex
	if !ok {
		rf.debug(dError, "ISS old [%v,%v,%v]<=[%v,%v,%v]", rf.state, rf.CurrentTerm, rf.lastIncludeIndex, Candidate, args.Term, args.LastIncludedIndex)
	}
	return
}

type InstallSnapShotReply struct {
	Term int
}

// 该服务表示，它已经创建了一个快照，其中包含到索引为止的所有信息。这意味着服务不再需要通过(包括)该索引的日志。
// Raft现在应该尽可能地减少它的日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 一旦服务器完成了快照的写入，它可能会删除从最后一个包含的索引开始的所有日志条目，以及任何先前的快照。
	// Raft必须以一种允许Go垃圾收集器释放和重用内存的方式丢弃旧的日志项;
	// 这要求对丢弃的日志条目没有可达的引用(指针)。
	rf.lastIncludeTerm = rf.getLogWithIndex(index).Term
	rf.DiscardLogsBeforeIndex(index)
	rf.lastIncludeIndex = index
	rf.date = snapshot
	rf.Logs[0] = Entry{Index: rf.lastIncludeIndex, Term: rf.lastIncludeTerm}
	rf.debug(dSnap, "TSS LI%d LT%d %v", rf.lastIncludeIndex, rf.lastIncludeTerm, rf.Logs)
	rf.persister.SaveStateAndSnapshot(rf.State(), rf.date)
}

// 发送leader所保存的快照
func (rf *Raft) sendSnapShot(server int) {
	rf.debug(dSnap, "-> S%d ISS", server)
	round := 0
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		args := InstallSnapShotArgs{
			Term:              rf.CurrentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludeIndex,
			LastIncludedTerm:  rf.lastIncludeTerm,
			Data:              rf.date,
		}
		rf.mu.Unlock()
		reply := InstallSnapShotReply{}
		rf.debug(dSnap, "-> S%d ISS-%d [T%d,%d,%d]", server, round, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
		if rf.callWithTimeLimit(snapShotTO, server, "Raft.InstallSnapShot", &args, &reply) {
			rf.debug(dSnap, "<- S%dT%d ISS [%d,%d]", server, reply.Term, args.LastIncludedIndex, args.LastIncludedTerm)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			//检查rpc返回值
			if !rf.checkInstallSnapShotArgs(&args) || rf.replyBigTerm(server, reply.Term) {
				return
			}
			// 更新next值
			rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.nextMatchIndex[server] = rf.nextIndex[server] - 1
			rf.debug(dSnap, "<- S%dT%d ISS success NI[%d]:%d", server, reply.Term, server, rf.nextIndex[server])
			return
		} else {
			rf.debug(dTo, "-> S%d ISS-%d", server, round)
			round++
		}
	}
}

//在一个InstallSnapshot RPC中发送整个快照。
//不要实现图13中分割快照的偏移机制。
// 1. 如果term < currentTerm .立即回复
// 5. 保存快照文件，丢弃索引较小的现有快照或部分快照
// 6. 如果现有日志条目与快照最后包含的条目具有相同的索引和任期，则保留紧随其后的日志条目并回复
// 7. 丢弃整个日志
// 8. 使用快照内容重置状态机(并加载快照的集群配置)

func (rf *Raft) InstallSnapShot(args *InstallSnapShotArgs, reply *InstallSnapShotReply) {
	persist := false
	rf.mu.Lock()
	defer func() {
		if persist {
			rf.persist()
		}
		rf.mu.Unlock()
	}()

	rf.debug(dSnap, "<- S%d ISS %v - [T%d,LI%d,LT%d] %v", args.LeaderId, args, rf.CurrentTerm, rf.lastIncludeIndex, rf.lastIncludeTerm, EntriesToString(rf.Logs))

	reply.Term = rf.CurrentTerm
	//本机任期较小才继续下去
	if rf.CurrentTerm < args.Term {
		rf.changeStateToFollower(args.Term)
		rf.resetElectionTimeOut()
		persist = true
	} else if rf.CurrentTerm > args.Term {
		// 如果leader的任期还没自己大
		return
	}

	// peer的快照更新
	if rf.lastIncludeIndex >= args.LastIncludedIndex {
		return
	}

	persist = false
	// 判断是否有日志需要保留，即从哪删
	if args.LastIncludedIndex <= rf.lastLog().Index && rf.getLogWithIndex(args.LastIncludedIndex).Term == args.LastIncludedTerm {
		// If existing log entry has same index and term as snapshot's last included entry, retain log entries following it and reply
		rf.DiscardLogsBeforeIndex(args.LastIncludedIndex)
	} else {
		// 删除所有条目
		rf.DiscardLogsBeforeIndex(rf.lastLog().Index)
	}

	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastApplied)
	rf.lastIncludeIndex = args.LastIncludedIndex
	rf.lastIncludeTerm = args.LastIncludedTerm
	rf.date = args.Data
	rf.Logs[0] = Entry{Index: rf.lastIncludeIndex, Term: rf.lastIncludeTerm}

	rf.debug(dSnap, "LI%d LT%d LC%v LA%v %v", rf.lastIncludeIndex, rf.lastIncludeTerm, rf.commitIndex, rf.lastApplied, rf.Logs)
	rf.persister.SaveStateAndSnapshot(rf.State(), args.Data)
	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		SnapshotIndex: rf.lastIncludeIndex,
		SnapshotTerm:  rf.lastIncludeTerm,
		Snapshot:      rf.date,
	}
}

// 发送心跳
func (rf *Raft) sendHeartBeats() {
	rf.heatCnt++
	//更新提交
	rf.updateCommitedIndexAndSignalApply()
	rf.debug(dTimer, "begin send heartbeats-%d T%d", rf.heatCnt, rf.CurrentTerm)
	cnt := rf.heatCnt
	// 对除自己外每个peer发送追加条目以实现心跳的同时更新状态，通过i个协程实现
	for i := range rf.peers {
		if i != rf.me {
			args := rf.makeAppendEntriesArg(i, true)
			go func(server int) {
				rf.debug(dTimer, "-> S%d %v-%d", server, args, cnt)
				reply := AppendEntriesReply{}
				if rf.callWithTimeLimit(heartbeatTO, server, "Raft.AppendEntries", args, &reply) {
					rf.debug(dTimer, "<- S%d %v-%d %v", server, args, rf.heatCnt, reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 检查返回的结果
					if !rf.checkAppendEntriesArg(server, args) || rf.replyBigTerm(server, reply.Term) {
						return
					}
					// 1. Reply false if term < currentTerm
					// 2. 如果日志在prevLogIndex中不包含术语与prevLogTerm匹配的条目，则返回false
					if reply.Success {
						// 如果成功则更新相应的next索引
						rf.nextIndex[server] = max(rf.nextIndex[server]-1, args.PrevLogIndex+1)
						rf.nextMatchIndex[server] = rf.nextIndex[server] - 1
					} else {
						// 否则找到最后一个能够匹配的条目，以此为起点同步
						rf.FindNextIndex(server, &reply)
					}
					// 如果leader的最后一个索引大于peer所要求的下一个，则对其进行追加

					if rf.lastLog().Index >= rf.nextIndex[server] && !rf.isInAppending[server] {
						rf.isInAppending[server] = true
						go rf.sendAppendEntries(server)
					}
				} else {
					rf.debug(dTo, "-> S%d HEAT-%d", server, cnt)
				}
			}(i)
		}
	}
}

// 发送追加日志
func (rf *Raft) sendAppendEntries(server int) {
	rf.debug(dAppend, "S%d START AP", server)
	round := 0
	var args *AppendEntriesArg
	rf.mu.Lock()
	// 检查状态
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	//如果想要的日志已经由于提交被放弃保存，则直接发送快照
	if rf.nextIndex[server] <= rf.lastIncludeIndex {
		rf.isInAppending[server] = false
		go rf.sendSnapShot(server)
		rf.mu.Unlock()
		return
	}
	//否则进行追加
	args = rf.makeAppendEntriesArg(server, false)
	rf.mu.Unlock()

	for !rf.killed() {
		rf.debug(dAppend, "-> S%d AP-%d %v", server, round, args)
		reply := AppendEntriesReply{}
		if rf.callWithTimeLimit(appendEntriesTO, server, "Raft.AppendEntries", args, &reply) {
			rf.debug(dAppend, "<- S%d AP-%d %v %v", server, round, args, reply)
			rf.mu.Lock()
			if !rf.checkAppendEntriesArg(server, args) || rf.replyBigTerm(server, reply.Term) {
				rf.isInAppending[server] = false
				rf.mu.Unlock()
				return
			}
			if reply.Success {
				// 这两个值都可能在发送RPC后更新。相反，正确的做法是将matchIndex更新为prevLogIndex+len（entries[]）。
				rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
				rf.nextMatchIndex[server] = rf.nextIndex[server] - 1
				if rf.nextIndex[server] == rf.lastLog().Index+1 {
					rf.debug(dAppend, "S%d END AP-%d", server, round)
					rf.isInAppending[server] = false
					rf.mu.Unlock()
					return
				}
			} else {
				rf.FindNextIndex(server, &reply)
			}
		} else {
			rf.debug(dTo, "-> S%d AP-%d", server, round)
			rf.mu.Lock()
			if !rf.checkAppendEntriesArg(server, args) {
				rf.isInAppending[server] = false
				rf.mu.Unlock()
				return
			}
		}
		// lock
		if rf.nextIndex[server] <= rf.lastIncludeIndex {
			rf.isInAppending[server] = false
			go rf.sendSnapShot(server)
			rf.mu.Unlock()
			return
		}
		args = rf.makeAppendEntriesArg(server, false)
		rf.mu.Unlock()
		round++
	}
}

/*
2A
 1. 要实现heartbeats，定义AppendEntries RPC结构体(尽管您可能还不需要所有的参数)，并让leader定期发送它们。编写AppendEntries RPC处理程序
    方法，该方法重置选举超时，以便当一个服务器已经当选时，其他服务器不会继续担任领导者。
 2. 如果RPC请求或响应包含term T > currentTerm: set currentTerm = T，转换为follower
    3.在等待投票时，候选人可能会从另一个声称自己是领导者的服务器收到AppendEntries RPC。
    如果领导者的任期(包括在其RPC中)至少与候选人的当前任期一样大，
    然后，候选人承认领导者是合法的，并返回到追随者状态。
    如果RPC中的项小于候选人的当前项，则候选人
    拒绝RPC并继续处于候选状态。

2B

	当发送AppendEntries RPC时，leader将新条目之前的索引和任期包含在其日志中。
	如果追随者没有在其日志中找到具有相同索引和任期的条目，那么它将拒绝新的条目。
	因此，只要AppendEntries成功返回，leader就知道follower的日志与它自己通过新条目记录的日志是相同的。
	为了使follower的日志与自己的一致，领导者必须找到两个日志一致的最新日志条目，然后删除
	在此点之后追随者日志中的任何条目，并在此点之后将领导者的所有条目发送给追随者。

	当拒绝AppendEntries请求时，follower可以包含冲突条目的项和它为该项存储的第一个索引。
	有了这些信息，leader可以递减nextIndex以绕过该项中所有冲突的条目;
	每个有冲突条目的术语都需要一个AppendEntries RPC，而不是每个条目都需要一个RPC。在实践中，我们怀疑这种优化是必要的，
	由于失败很少发生，因此不太可能有许多不一致的条目。
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArg, reply *AppendEntriesReply) {
	// Followers (§5.2):
	// 	• 回应候选人和领导的rpc
	//  • 如果选举超时而没有从当前领导者接收AppendEntries RPC或授予候选人投票:转换为候选人
	topic := dTimer
	if len(args.Entries) > 0 {
		topic = dFollow
	}
	rf.debug(topic, "<- S%d %v received [LI%d,LT%d]", args.LeaderID, args, rf.lastIncludeIndex, rf.lastIncludeTerm)

	persist := false
	rf.mu.Lock()
	defer func() {
		if persist {
			rf.persist()
		}
		rf.mu.Unlock()
	}()

	reply.Term = rf.CurrentTerm
	reply.Success = false
	// 判断此时状态能否追加
	if args.Term == rf.CurrentTerm {
		if rf.state == Leader {
			log.Panic("two leader in same term", rf.CurrentTerm)
		} else if rf.state != Follower {
			rf.changeStateToFollower(args.Term)
			persist = true
		}
	} else if args.Term < rf.CurrentTerm {
		// leader的任期比自己的小
		rf.debug(dTimer, "-> S%d T%d > T%d", args.LeaderID, rf.CurrentTerm, args.Term)
		return
	} else {
		// 发送方的任期大
		rf.changeStateToFollower(args.Term)
		persist = true
	}

	rf.resetElectionTimeOut()

	updateCommitIndex := func() {
		// If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
		if args.LeaderCommit > rf.commitIndex {
			old := rf.commitIndex
			// rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)	// wrong 发心跳包时会把 [prev+1: len)之间的全部提交，
			// 但是只有 prev前是一致的
			rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)) // index of last new entry
			if old != rf.commitIndex {
				rf.debug(dCommit, "commitIndex update %d", rf.commitIndex)
				rf.applyCond.Signal()
			}
		}
	}

	if args.PrevLogIndex < rf.lastIncludeIndex { // snapshot后 收到一个旧的心跳或者appendEntries
		reply.Success = true // 旧的appendEntries最后一条日志index不会超过log的最后一条，
		updateCommitIndex()
		return
	}

	// 缺日志
	if rf.lastLog().Index < args.PrevLogIndex {
		reply.XLen = rf.lastLog().Index + 1
		reply.XTerm = -1
		reply.XIndex = -1
		rf.debug(dFollow, "-> S%d %v", args.LeaderID, reply)
		return
	}

	// 如果现有条目与新条目冲突(相同的索引但不同的任期)，则删除现有条目及其后面的所有条目 (§5.3)
	prevLog := rf.getLogWithIndex(args.PrevLogIndex)
	if prevLog.Term != args.PrevLogTerm {
		reply.XTerm = prevLog.Term
		reply.XLen = -1
		idx := rf.lowerBound(1, len(rf.Logs), func(a int) bool {
			return rf.Logs[a].Term >= reply.XTerm
		})
		// 第一个条目的索引
		reply.XIndex = rf.Logs[idx].Index
		rf.deleteLogsStartFromIndex(args.PrevLogIndex)
		rf.debug(dFollow, "-> S%d %v", args.LeaderID, reply)
		persist = true
		return
	}

	reply.Success = true

	if len(args.Entries) > 0 {
		if rf.lastLog().Index > args.PrevLogIndex {
			// 添加没有的新条目
			nextIndex := args.PrevLogIndex + 1
			if rf.getLogWithIndex(nextIndex).Term == args.Entries[0].Term {
				if rf.lastLog().Index < args.Entries[len(args.Entries)-1].Index {
					rf.deleteLogsStartFromIndex(nextIndex)
					rf.appendLogs(args.Entries...)
				}
			} else {
				// 存在冲突
				rf.deleteLogsStartFromIndex(nextIndex)
				rf.appendLogs(args.Entries...)
			}
		} else {
			rf.appendLogs(args.Entries...)
		}
		persist = true
		rf.debug(dFollow, "-> S%d %v", args.LeaderID, reply)
	} else {
		rf.debug(dTimer, "-> S%d HEAT", args.LeaderID)
	}

	updateCommitIndex()
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

// 开始选举
func (rf *Raft) beginElection() {
	rf.debug(dVote, "begin election-T%d\n", rf.CurrentTerm)
	cnter := make(map[int]struct{}, len(rf.peers)) // 网络延迟有重复收到一个follower投票的风险 , 当然也可以给每一个request 一个序号
	cnter[rf.me] = struct{}{}
	for i := range rf.peers {
		if i != rf.me {
			args := RequestVoteArgs{
				Term:         rf.CurrentTerm,
				CandidatedId: rf.me,
				LastLogIndex: rf.lastLog().Index,
				LastLogTerm:  rf.lastLog().Term,
			}
			go func(server int) {
				round := 0
				for !rf.killed() {
					reply := RequestVoteReply{}
					rf.debug(dVote, "-> S%d RV-%d %v\n", server, round, args)
					if rf.callWithTimeLimit(electionTO, server, "Raft.RequestVote", &args, &reply) {
						rf.debug(dVote, "<- S%d RV-%d %v\n", server, round, reply)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						// 检查结果
						if !rf.checkRequestVoteArgs(server, &args) || rf.replyBigTerm(server, reply.Term) {
							return
						}
						//判断是否超过半数
						if reply.VoteGranted {
							cnter[server] = struct{}{}
							n := len(cnter)
							rf.debug(dVote, "<- S%d cnt:%d\n", server, n)
							if n > len(rf.peers)/2 {
								rf.changeStateToLeader()
							}
						}
						// 尽管可能 notUpdate ，还是有可能当选leader的
						return
					} else {
						rf.debug(dTo, "-> S%d RV-%d\n", server, round)
					}
					round++
					if round == 2 {
						return
					}
					rf.mu.Lock()
					if rf.state != Candidate {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
				}
			}(i)
		}
	}
}

// 接收者实现:
// 1 如果term < currentTerm(§5.1)则返回false
// 2 如果votedFor为空或candidateId，并且候选人的日志至少与接收者的日志一样最新，则授予投票
// 3 如果RPC请求或响应包含term T > currentTerm: set currentTerm = T，转换为follower(§5.1)
// 4 如果投票人自己的日志比候选人的日志更新，投票人就会拒绝投票。
// 如果日志的最后一个条目有不同的术语，那么后面的术语的日志是最新的。
// 如果日志以相同的期限结束，那么任何较长的日志都是最新的。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	persist := false
	rf.mu.Lock()
	defer func() {
		if persist {
			rf.persist()
		}
		rf.mu.Unlock()
	}()
	rf.debug(dVote, "<- S%d RV %v VF%d", args.CandidatedId, args, rf.VotedFor)
	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {
		rf.debug(dVote, "-> S%d X-RV T%d > T%d\n", args.CandidatedId, rf.CurrentTerm, args.Term)
		return
	} else if args.Term == rf.CurrentTerm && rf.VotedFor != -1 {
		if rf.VotedFor == args.CandidatedId { // 网络延迟造成第一条回复没有收到
			rf.debug(dVote, "-> S%d G RV\n", args.CandidatedId)
			reply.VoteGranted = true
			rf.resetElectionTimeOut()
		} else {
			rf.debug(dVote, "-> S%d X-RV VF = S%d\n", args.CandidatedId, rf.VotedFor)
		}
		return
	}

	// == votedFor=-1 or  >
	if args.Term > rf.CurrentTerm {
		rf.changeStateToFollower(args.Term)
		rf.VotedFor = -1
		persist = true
	}

	// check uptodate
	lastIndex := rf.lastLog().Index
	if rf.lastLog().Term > args.LastLogTerm ||
		(rf.lastLog().Term == args.LastLogTerm && lastIndex > args.LastLogIndex) {
		rf.debug(dVote, "-> S%d X-RV NU%d\n", args.CandidatedId, lastIndex)
	} else {
		rf.debug(dVote, "-> S%d G RV\n", args.CandidatedId)
		rf.VotedFor = args.CandidatedId
		reply.VoteGranted = true
		rf.resetElectionTimeOut()
		persist = true
	}
}

// 如果存在N使N > commitIndex，大部分matchIndex[i]≥N,即大多数peer已更新log[N]。term == currentTerm: set commitIndex = N
func (rf *Raft) updateCommitedIndexAndSignalApply() {
	checkState(rf, Leader)
	old := rf.commitIndex
	cnter := make(map[int]int, rf.lastLog().Index-rf.lastIncludeIndex)
	cnter[rf.lastLog().Index]++
	for i, nextMatch := range rf.nextMatchIndex {
		if i != rf.me && nextMatch > rf.commitIndex {
			cnter[nextMatch]++
		}
	}
	// 得到最大的且相同任期的日志索引
	for matchIndex, matchCnt := range cnter {
		if matchCnt > len(rf.peers)/2 && rf.getLogWithIndex(matchIndex).Term == rf.CurrentTerm {
			rf.commitIndex = max(rf.commitIndex, matchIndex)
		}
	}
	// 如果此索引存在
	if old != rf.commitIndex {
		rf.debug(dCommit, "update commitIndex %d", rf.commitIndex)
		//条件变量 sync.Cond 用来协调想要访问共享资源的 Goroutine。当共享资源的状态发生变化时，sync.Cond 可以用来通知被阻塞的 Goroutine。
		rf.applyCond.Signal()
	}
}

// 应用指令
func (rf *Raft) applyCommand() {
	for !rf.killed() {
		rf.mu.Lock()
		// 如果最后应用的大于已提交的
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		// 不用检查是否killed，不会影响persister
		lastApplied := rf.lastApplied
		commitIndex := rf.commitIndex
		entries := make([]Entry, 0, commitIndex-lastApplied)
		rf.debug(dCommit, "LA%d LC%d LI%d LT%d", lastApplied, commitIndex, rf.lastIncludeIndex, rf.lastIncludeTerm)
		i, j := rf.getRealIndexInLog(lastApplied+1), rf.getRealIndexInLog(commitIndex+1)
		entries = append(entries, rf.Logs[i:j]...)
		rf.mu.Unlock()
		rf.debug(dCommit, "begin apply command %d -> %d", lastApplied+1, commitIndex)
		rf.debug(dCommit, "apply %v", EntriesToString(entries))
		for i := range entries {
			e := entries[i]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
			}
			rf.applyCh <- applyMsg
		}
		rf.mu.Lock()
		rf.lastApplied = commitIndex
		rf.debug(dCommit, "lastApplied update to %d", rf.lastApplied)
		rf.mu.Unlock()
	}
}

// 如果这个peer最近没有收到心跳，那么就会开始新的选举。
/*
	•转换为候选人，开始选举:
		•增加currentTerm
		•为自己投票
		•重置选举计时器
		•发送RequestVote rpc到所有其他服务器
	•如果收到大多数服务器的投票:成为领导者
	如果AppendEntries RPC从新的leader收到:转换为follower
	•如果选举超时:开始新的选举
*/
func (rf *Raft) ticker() {
	for !rf.killed() {
		//时间到
		<-rf.tick.C
		rf.mu.Lock()
		if rf.state == Leader {
			//如果是leader发送心跳
			rf.sendHeartBeats()
		} else {
			// 两个Follower超时时间相近，S1,S2
			// S1 299 S2 300
			// S1 ticker(), tick超时发起投票
			// S2 RequestVote(), 收到投票获取了锁
			// S2 ticker()，tick超时等待获取锁
			// S2 RequestVote(), 投票后重置超时时间，清空channel，释放锁，但此时S2第一个tick已经被收取了
			// S2 ticker() 获取了锁，又会发起新一轮投票
			//如果未超时
			if time.Since(rf.lastHeatbeatTime) < rf.electionTO {
				rf.debug(dError, "begin election not time out")
				rf.mu.Unlock()
				continue
			}
			// 否则开始选举
			rf.changeStateToCandidate()
			rf.beginElection()
		}
		rf.mu.Unlock()
	}
}

// 当前一次的超时数据没有被读取，而设置了新的定时器，然后去通道读数据，
// 结果读到的是上次超时的超时事件，看似成功，实则失败，完全掉入陷阱。
// chan是缓冲区大小为，当已满时，会丢弃新的tick
// sendTime does a non-blocking send of the current time on c.
// func sendTime(c any, seq uintptr) {
// 	select {
// 	case c.(chan Time) <- Now():
// 	default:
// 	}
// }

//
// 使用Raft的服务(例如k/v服务器)希望在下一个命令开始协议，并追加到Raft的日志中。
// 如果此服务器不是leader，则返回false。
// 否则启动协议并立即返回。不能保证这个命令会被提交到Raft日志中，因为领导者可能会失败或在选举中失败。
// 即使Raft实例已经被杀死，这个函数也应该优雅地返回。

// 第一个返回值是命令在提交时被分配的索引。第二个返回值是当前任期。如果服务器认为自己是leader，第三个返回值为真。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return index, term, false
	}
	index, term, isLeader = rf.start(command)
	return index, term, isLeader
}

func (rf *Raft) start(command interface{}) (int, int, bool) {
	index := rf.lastLog().Index + 1
	term := rf.CurrentTerm
	entry := Entry{Index: index, Term: term, Command: command}
	rf.appendLogs(entry)
	rf.persist()
	rf.updateCommitedIndexAndSignalApply()
	for i := range rf.peers {
		if inAppending := rf.isInAppending[i]; i != rf.me && !inAppending {
			rf.isInAppending[i] = true
			go rf.sendAppendEntries(i)
		}
	}
	return index, term, true
}

// 服务或测试人员想要创建一个Raft服务器。所有Raft服务器(包括这个)的端口都在peers[]中。
// 此服务器的端口是peers[me]。所有服务器的peers[]顺序相同。
// Persister是服务器保存其持久状态的地方，最初还保存最近保存的状态(如果有的话)。
// applyCh是一个通道，测试人员或服务器期望Raft在该通道上发送ApplyMsg消息。
// Make()必须快速返回，因此它应该为任何长时间运行的工作启动goroutines。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0
	rf.disconnect = 0
	rf.VotedFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.Logs = make([]Entry, 1)
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.tick = time.NewTicker(999 * time.Second)
	rf.Logs[0] = Entry{Index: 0, Term: 0}
	rf.debug(dWarn, "make!\n")
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.randomElectionTimeOut()
	go rf.ticker()
	go rf.applyCommand()
	return rf
}

// 切换为leader
func (rf *Raft) changeStateToLeader() {
	rf.debug(dLeader, "Candidate => Leader T%d", rf.CurrentTerm)
	rf.state = Leader
	n := len(rf.peers)
	rf.nextIndex = make([]int, n)
	rf.heatCnt = 0
	//初始化nextIndex为此peer的最后一个日志+1
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.lastLog().Index + 1
	}
	// 初始化为0，单调递增
	rf.nextMatchIndex = make([]int, n)
	rf.isInAppending = make(map[int]bool, n)
	//初始化追加状态为false
	for i := 0; i < n; i++ {
		rf.isInAppending[i] = false
	}
	rf.setHeartBeatTimeOut()
	rf.sendHeartBeats()
	//Leader完整性属性保证Leader拥有所有已提交的条目，但在任期开始时，它可能不知道这些是什么。
	//为了找到答案，它需要这样做它的项中提交一个条目。
	//Raft通过让每个leader在任期开始时在日志中提交一个空白的无操作条目来处理这个问题。
	// Var no_op接口{}= nil
	// rf.start(no_op)
}

// 转换为候选者
func (rf *Raft) changeStateToCandidate() {
	//投票给自己，当前任期自增
	rf.VotedFor = rf.me
	rf.CurrentTerm++
	if rf.CurrentTerm > 500 {
		panic("term too large something go wrong ?")
	}
	if rf.state == Follower {
		rf.debug(dState, "Follower => Candidate T%d", rf.CurrentTerm)
		rf.state = Candidate
	} else {
		rf.debug(dState, "Candidate T%d", rf.CurrentTerm)
	}
	//重置超时时间
	rf.randomElectionTimeOut()
	rf.persist()
}

// 转换为follower
func (rf *Raft) changeStateToFollower(term int) {
	var str string
	if rf.state != Follower {
		str = fmt.Sprintf("%v => Follower ", rf.state)
	} else {
		str = "Follower "
	}
	rf.state = Follower
	if term > rf.CurrentTerm {
		str += fmt.Sprintf("T%d => T%d", rf.CurrentTerm, term)
		rf.CurrentTerm = term
	}
	rf.debug(dState, str)
}

// 计时器相关，如果是leader，设置心跳间隔，否则设置选举超时
// 设置心跳间隔
func (rf *Raft) setHeartBeatTimeOut() {
	rf.debug(dTimer, "set leader tick%v", heartbeatTO)
	rf.setNewTO(heartbeatTO)
}

// 设置选举超时时间并将此时记录为上次心跳时间
func (rf *Raft) randomElectionTimeOut() {
	rf.electionTO = RandomElectionTimeOut()
	rf.debug(dTimer, "set election tick%v", rf.electionTO)
	rf.setNewTO(rf.electionTO)
	rf.lastHeatbeatTime = time.Now()
}

// 重置选举超时
func (rf *Raft) resetElectionTimeOut() {
	rf.debug(dTimer, "reset election tick%v", rf.electionTO)
	rf.setNewTO(rf.electionTO)
	rf.lastHeatbeatTime = time.Now()
}

// 更新计时器，并将之前的记录清除
func (rf *Raft) setNewTO(time time.Duration) {
	rf.tick.Reset(time)
	for len(rf.tick.C) > 0 {
		rf.debug(dError, "<-rf.tick.C")
		<-rf.tick.C
	}
}

// 检查状态
func checkState(rf *Raft, state State) {
	if rf.state != state {
		str := fmt.Sprintf("rf state unequal expect %v got %v\n", state, rf.state)
		panic(str)
	}
}

/*
	Case 1 (leader doesn't have XTerm):
    nextIndex = XIndex
  Case 2 (leader has XTerm):
    nextIndex = leader's last entry for XTerm
  Case 3 (follower's log is too short):
    nextIndex = XLen
*/
// 根据appendEntries的返回值找到peer想要的下一个log的索引
func (rf *Raft) FindNextIndex(server int, reply *AppendEntriesReply) {
	if reply.XTerm != -1 {
		// 找到Xterm的最后一个条目
		left, right := 1, len(rf.Logs)
		for left < right {
			mid := left + (right-left)/2
			if rf.Logs[mid].Term <= reply.XTerm {
				left = mid + 1
			} else {
				right = mid
			}
		}
		if rf.Logs[right-1].Term == reply.XTerm {
			// Case 2 (leader has XTerm):
			rf.nextIndex[server] = rf.Logs[right-1].Index
		} else {
			// 	Case 1 (leader doesn't have XTerm):
			rf.nextIndex[server] = reply.XIndex
		}
	} else {
		// Case 3 (follower's log is too short):
		rf.nextIndex[server] = reply.XLen
	}
	rf.debug(dLog, "nextIndex[%d] %d", server, rf.nextIndex[server])
}

// 得到真实索引，因为日志提交后会丢弃掉已提交条目
func (rf *Raft) getRealIndexInLog(i int) int {
	j := i - rf.lastIncludeIndex
	if j < 0 {
		panic("j must > 0")
	}
	return j
}

// 根据索引找日志
func (rf *Raft) getLogWithIndex(index int) *Entry {
	if index-rf.lastIncludeIndex < 0 {
		return &rf.Logs[0]
	}
	return &rf.Logs[index-rf.lastIncludeIndex]
}

// 最后一条日志
func (rf *Raft) lastLog() *Entry {
	return &rf.Logs[len(rf.Logs)-1]
}

// 找到前一条日志
func (rf *Raft) prevLog(server int) *Entry {
	return rf.getLogWithIndex(rf.nextIndex[server] - 1)
}

// find the first entry use cmp
func (rf *Raft) lowerBound(left int, right int, cmp func(a int) bool) int {
	r := right
	for left < right {
		mid := left + (right-left)/2
		if cmp(mid) {
			right = mid
		} else {
			left = mid + 1
		}
	}
	if r == right {
		panic("not found")
	}
	return right
}

// 初始化参数
func (rf *Raft) makeAppendEntriesArg(server int, heartbeat bool) *AppendEntriesArg {
	var entries []Entry
	if !heartbeat {
		entries = rf.Logs[rf.getRealIndexInLog(rf.nextIndex[server]):]
	}
	return &AppendEntriesArg{
		LeaderID:     rf.me,
		Term:         rf.CurrentTerm,
		PrevLogIndex: rf.prevLog(server).Index,
		PrevLogTerm:  rf.prevLog(server).Term,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

//测试器不会在每次测试后停止由Raft创建的例程，但它会调用Kill()方法。你的代码可以使用killed()来检查Kill()是否被调用。
//atomic的使用避免了对锁的需要。

// 问题是长时间运行的goroutines例程使用内存，可能会占用CPU时间，可能会导致以后的测试失败并产生令人困惑的调试输出。任何具有长时间循环的goroutine都应该调用killed()来检查它是否应该停止。
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.debug(dWarn, "has been killed LC %d LA %d\n", rf.commitIndex, rf.lastApplied)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) Disconnect() { // for debug
	atomic.StoreInt32(&rf.disconnect, 1)
}

func (rf *Raft) Connect() {
	atomic.StoreInt32(&rf.disconnect, 0)
}

// 限定时间进行rpc
func (rf *Raft) callWithTimeLimit(limit time.Duration, server int, svcMeth string, args interface{}, reply interface{}) bool {
	var ok bool
	ch := make(chan bool)
	go func() {
		ch <- rf.peers[server].Call(svcMeth, args, reply)
	}()
	// 此处开启协程一直尝试rpc，如果在限定时间内未成功，false，否则true
	select {
	case <-time.After(limit):
		go func() {
			<-ch // 丢弃旧的request，不要阻塞 goroutine
		}()
		return false
	case ok = <-ch:
	}
	return ok
}

// 如果遇到了更大的任期，将自己转变为follower，并重设超时时间
func (rf *Raft) replyBigTerm(server int, term int) bool {
	if term > rf.CurrentTerm {
		rf.debug(dLog, "<- S%d  T%d > T%d", server, term, rf.CurrentTerm)
		rf.changeStateToFollower(term)
		rf.persist()
		rf.resetElectionTimeOut()
		return true
	}
	return false
}

// 放弃保存已提交日志
func (rf *Raft) DiscardLogsBeforeIndex(index int) {
	i := rf.getRealIndexInLog(index)
	logs := make([]Entry, len(rf.Logs)-i) // 1 + [i+1,len)
	if i+1 < len(rf.Logs) {
		copy(logs[1:], rf.Logs[i+1:])
	}
	rf.Logs = logs
	rf.debug(dSnap, "log trim (%d", index)
}

// 删除索引位置之后的所有日志
func (rf *Raft) deleteLogsStartFromIndex(index int) {
	i := rf.getRealIndexInLog(index)
	rf.Logs = rf.Logs[:i]
	rf.debug(dLog, "log trim %v]", EntriesToString(rf.Logs))
}

// 追加日志条目
func (rf *Raft) appendLogs(entries ...Entry) {
	str := fmt.Sprintf("%v => %v", EntriesToString(entries), EntriesToString(rf.Logs))
	rf.Logs = append(rf.Logs, entries...)
	rf.debug(dLog, "%v", str)
}

// 输出调试信息
func (rf *Raft) debug(topic logTopic, format string, a ...interface{}) {
	if atomic.LoadInt32(&rf.disconnect) == 1 || topic == dTo {
		return
	}
	prefix := fmt.Sprintf("S%d ", rf.me)
	format = prefix + format
	Debug(topic, format, a...)
}
