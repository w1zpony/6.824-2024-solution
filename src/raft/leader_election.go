package raft

import "time"

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int //leader任期
	CandidateId  int
	LastLogIndex int //candidate最新一条日志记录索引值
	LastLogTerm  int //candidate最新一条日志记录的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  //Raft节点当前任期号，为了让candidate更新term
	VoteGranted bool //当前节点投票给该candidate时返回true
}

// candidate调用，申请投票
// 5.1 term < currrentTerm返回false
// 5.2 term = currentTerm && votedFor != -1 && votedFor != candidateId 返回false
// 5.4 如果voteFor为空或者等于args.CandidateId，且candidate日志至少与当前节点日志一样新，则投票给该candidate
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	Debug(dVote, "S%d receive RequestVote from S%d", rf.me, args.CandidateId)
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//5.1 任期小于当前任期
	if args.Term < rf.currentTerm {
		Debug(dVote, "S%d,reject vote S%d: rf.term[%d] > args.term[%d]", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//5.2 相同任期，但投票给其他人
	if args.Term == rf.currentTerm && rf.voteFor != args.CandidateId && rf.voteFor != -1 {
		Debug(dVote, "S%d,reject vote S%d: already voted for S%d ", rf.me, args.CandidateId, rf.voteFor)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//5.1 收到更高任期
	if args.Term > rf.currentTerm {
		Debug(dVote, "S%d change to Follower: rf.term[%d] < S%d.term[%d] ", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.currentTerm = args.Term
		rf.convert2Follower()
		rf.persist()
	}

	//5.4 日志不够新
	if !rf.isLogUpToDate(args.LastLogIndex, args.LastLogTerm) {
		Debug(dVote, "S%d,reject vote S%d: isLogUpToDate false ", rf.me, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	//投票
	rf.voteFor = args.CandidateId
	rf.persist()
	rf.lastAppendTime = now
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	Debug(dVote, "S%d access vote S%d ", rf.me, args.CandidateId)
}

func (rf *Raft) isLogUpToDate(index, term int) bool {
	lastLog := rf.getLastLog()
	return term > lastLog.Term || (term == lastLog.Term && index >= lastLog.Index)
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

func (rf *Raft) startElection() {
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLog().Index,
		LastLogTerm:  rf.getLastLog().Term,
	}
	candidateVotes := 1
	Debug(dVote, "S%d become Candidate, start leader election", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			if rf.sendRequestVote(server, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				Debug(dVote, "S%d Candidate <-Vote S%d, req.term=%d req.Vote=%v", rf.me, server, reply.Term, reply.VoteGranted)
				if rf.role == Candidate && reply.Term == rf.currentTerm {
					if reply.VoteGranted {
						candidateVotes++
						if candidateVotes > len(rf.peers)/2 {
							Debug(dLeader, "S%d become leader", rf.me)
							rf.convert2Leader()
							rf.broadcastAppendEntries()
						}
					}
				} else if reply.Term > rf.currentTerm {
					Debug(dVote, "S%d reply.Term[%d] > candidate.Term[%d], become to follower", rf.me, reply.Term, rf.currentTerm)
					rf.convert2Follower()
					rf.currentTerm = reply.Term
					rf.persist()
				}

			}
		}(peer, args)
	}
}
