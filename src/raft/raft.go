package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new debug entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the debug, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive debug entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed debug entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// Follower :
// electiontimeout超时前未收到Leader AppendEntries或者Candidate RequestVote，
// 则转为Candidate进行选举
// Candidate:
// 变为candidate时，term++,投票投自己，重置electionTimer,
// 向所有节点发送RequestVote。如果获得半数投票，转为leader
// 或者electionTimer超时，开启新一轮选举
// Leader:
// 上任后立即发送一轮心跳
// 每个一段时间发送心跳 AppendEntries
type Role int

const (
	Follower = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	//所有RaftNode保存的持久状态 在回复Rpc请求前要持久化
	currentTerm int
	voteFor     int
	log         []LogEntry
	//服务器不稳定状态
	commitIndex int //已知被提交的最大日志索引值
	lastApplied int //被状态机执行的最大日志索引值
	//leader的不稳定状态 重新选举后初始化
	nextIndex  []int //每个节点下一个日志索引号 初始化为领导者最后一条日志索引值+1
	matchIndex []int //每个节点已经复制到该服务器的最大索引值 初始化为0

	//自定义参数，非论文中Figure2中field
	role              Role
	lastAppendTime    time.Time
	electionTimeout   time.Duration
	lastIncludedIndex int
	lastIncludedTerm  int
	snapshot          []byte
	needApplySnapshot bool

	applyCh   chan ApplyMsg
	applyCond *sync.Cond
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(rf.currentTerm); err != nil {
		panic("Failed to encode currentTerm: " + err.Error())
	}
	if err := e.Encode(rf.voteFor); err != nil {
		panic("Failed to encode votedFor: " + err.Error())
	}
	if err := e.Encode(rf.log); err != nil {
		panic("Failed to encode log: " + err.Error())
	}
	if err := e.Encode(rf.lastIncludedIndex); err != nil {
		panic("Failed to encode snapshot.LastIncludedIndex: " + err.Error())
	}
	if err := e.Encode(rf.lastIncludedTerm); err != nil {
		panic("Failed to encode snapshot.LastIncludedTerm: " + err.Error())
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if err := d.Decode(&currentTerm); err != nil {
		panic("Failed to decode currentTerm: " + err.Error())
	}
	if err := d.Decode(&voteFor); err != nil {
		panic("Failed to decode votedFor: " + err.Error())
	}
	if err := d.Decode(&log); err != nil {
		panic("Failed to decode log: " + err.Error())
	}
	if log == nil || len(log) == 0 {
		log = append(log, LogEntry{
			Term:    -1,
			Command: nil,
		})
	}
	if err := d.Decode(&lastIncludedIndex); err != nil {
		panic("Failed to decode lastIncludedIndex: " + err.Error())
	}
	if err := d.Decode(&lastIncludedTerm); err != nil {
		panic("Failed to decode lastIncludedTerm: " + err.Error())
	}
	rf.currentTerm = currentTerm
	rf.voteFor = voteFor
	rf.log = log
	rf.lastIncludedTerm = lastIncludedTerm
	rf.lastIncludedIndex = lastIncludedIndex
	rf.commitIndex = Max(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, lastIncludedIndex)
}

func (rf *Raft) recoverSnapshot() {
	snapshot := rf.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		Debug(dTrace, "S%d has no snapshot", rf.me)
		return
	}
	rf.snapshot = snapshot
	rf.needApplySnapshot = true

	Debug(dTrace, "S%d recover snapshot, lastIndex=%d lastTerm=%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

func (rf *Raft) heartbeat() {
	for rf.killed() == false {
		time.Sleep(HeartbeatInterval)
		rf.mu.Lock()
		if rf.role != Leader {
			//Debug(dClient, "S%d is not Leader,stop heartbeat", rf.me)
			rf.mu.Unlock()
			continue
		}
		Debug(dTimer, "S%d Leader, timer heartbeats", rf.me)
		rf.broadcastAppendEntries()
		rf.mu.Unlock()
	}
}

func (rf *Raft) electionTicker() {
	for rf.killed() == false {
		time.Sleep(rf.electionTimeout)
		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

		if time.Since(rf.lastAppendTime) >= rf.electionTimeout {
			Debug(dInfo, "S%d start election", rf.me)
			rf.convert2Candidate()
			rf.persist()
			rf.startElection()
		}
		rf.electionTimeout = genElectionDuration()
		rf.mu.Unlock()
	}
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's debug. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft debug, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 上层服务(kv service)调用，Leader指令日志追加到LogEntry进行同步和commit
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isLeader := rf.currentTerm, rf.role == Leader
	if !isLeader {
		return -1, -1, false
	}
	index := rf.getLastLog().Index + 1
	newLog := LogEntry{
		Term:    term,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, newLog)
	rf.persist()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	Debug(dTrace, "S%d new command", rf.me)
	go rf.broadcastAppendEntries()
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

func (rf *Raft) getLog(idx int) LogEntry {
	if idx < rf.getFirstLog().Index {
		return LogEntry{
			Term:  -1,
			Index: -1,
		}
	}
	subscript := idx - rf.getFirstLog().Index
	if len(rf.log) > subscript {
		return rf.log[subscript]
	} else {
		return LogEntry{
			Term:  -1,
			Index: -1,
		}
	}
}

func (rf *Raft) hasNewLogs(i int) bool {
	return rf.getLastLog().Index >= rf.nextIndex[i]
}

func (rf *Raft) applier(applyCh chan ApplyMsg) {
	for {
		rf.mu.Lock()
		for rf.commitIndex <= rf.lastApplied && !rf.needApplySnapshot {
			rf.applyCond.Wait()
			if rf.killed() {
				rf.mu.Unlock()
				return
			}
		}
		if rf.needApplySnapshot {
			if rf.lastIncludedIndex <= rf.lastApplied {
				rf.needApplySnapshot = false
				rf.mu.Unlock()
				continue
			}
			Debug(dCommit, "S%d apply snapshot, lastIndex=%d lastTerm=%d lastApplied=%d", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.lastApplied)
			msg := ApplyMsg{
				SnapshotValid: true,
				CommandValid:  false,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.mu.Unlock()
			applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = Max(rf.lastApplied, rf.lastIncludedIndex)
			rf.needApplySnapshot = false
			rf.mu.Unlock()
		} else {
			startIdx := rf.lastApplied + 1
			endIdx := rf.commitIndex
			entries := make([]LogEntry, rf.commitIndex-rf.lastApplied)
			copy(entries, rf.log[startIdx-rf.getFirstLog().Index:endIdx-rf.getFirstLog().Index+1])
			rf.mu.Unlock()
			for _, entry := range entries {
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: entry.Index,
				}
			}
			rf.mu.Lock()
			if rf.lastApplied < endIdx {
				rf.lastApplied = endIdx
			}
			Debug(dCommit, "S%d apply logs,commit[%d] lastApplied[%d]", rf.me, rf.commitIndex, rf.lastApplied)
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) convert2Leader() {
	if rf.role == Leader {
		return
	}
	rf.role = Leader
	Debug(dInfo, "S%d is leader now", rf.me)
}

func (rf *Raft) convert2Follower() {
	if rf.role == Follower {
		return
	}
	rf.role = Follower
	rf.voteFor = -1
	Debug(dInfo, "S%d is follower now", rf.me)
}

func (rf *Raft) convert2Candidate() {
	rf.role = Candidate
	rf.voteFor = rf.me
	rf.currentTerm++
	Debug(dInfo, "S%d is candidate now", rf.me)
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

	rf.mu = sync.Mutex{}
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.log = make([]LogEntry, 1)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	rf.role = Follower
	rf.electionTimeout = genElectionDuration()

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverSnapshot()

	for peer := range peers {
		rf.matchIndex[peer] = 0
		rf.nextIndex[peer] = rf.getLastLog().Index + 1
	}
	Debug(dInfo, "S%d start", rf.me)
	go rf.electionTicker()
	go rf.heartbeat()
	go rf.applier(rf.applyCh)

	return rf
}
