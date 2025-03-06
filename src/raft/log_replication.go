package raft

import "time"

// leader调用，用于同步日志，也用于心跳
type AppendEntriesArgs struct {
	Term         int //leader任期
	LeaderId     int
	PrevLogIndex int        //之前处理过的日志索引值
	PrevLogTerm  int        //prevLogIndex任期号
	Entries      []LogEntry //heartbeat时为空
	LeaderCommit int        //leader已经提交的日志索引值
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// term < currentTerm return false
// 日志不包含prevLogIndex 或prevLogTerm不匹配 return false
// prevLogIndex prevLogTerm冲突，删除此条以及之后的所有log
// 追加不包含的日志
// 如果leaderCommit > commitIndex,则设置commitIndex为leaderCommit和最新条目索引这两个中较小的一个
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//5.1 leader比自己的term小
	//AppendEntries中的Rpc任期小，不更新electionTicker
	if args.Term < rf.currentTerm {
		Debug(dClient, "S%d [Append] leader[%d] term is lower, reject", rf.me, args.LeaderId)
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}
	rf.lastAppendTime = now
	//Figure2
	//5.2 如果args.term >= rf.term，无论什么状态，都应退到follower状态，保证单一leader
	rf.convert2Follower()
	rf.currentTerm = args.Term
	rf.persist()

	//日志太旧了
	if args.PrevLogIndex < rf.getFirstLog().Index {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	if !rf.matchPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term = rf.currentTerm
		reply.Success = false
		//follower没有PrevLogIndex
		if rf.getLastLog().Index < args.PrevLogIndex {
			reply.ConflictIndex = rf.getLastLog().Index + 1
			reply.ConflictTerm = -1
		} else {
			//包含PrevLogIndex但是任期不对
			firstIdx := rf.getFirstLog().Index
			i := args.PrevLogIndex
			for ; i >= firstIdx && rf.log[i-firstIdx].Term == args.PrevLogTerm; i-- {
			}
			reply.ConflictIndex = i + 1
			reply.ConflictTerm = args.PrevLogTerm
		}
		return
	} else {
		//PrevLogIndex PrevLogTerm匹配，需要校验args.Entries和已存在的日志是否存在冲突
		//如果存在冲突，回退冲突的日志，将新日志覆盖到log中
		conflictIndex := -1
		for i, entry := range args.Entries {
			localIndex := args.PrevLogIndex + i + 1
			logOffset := args.PrevLogIndex - rf.getFirstLog().Index + i + 1
			if localIndex > rf.getLastLog().Index || rf.log[logOffset].Term != entry.Term {
				conflictIndex = localIndex
				break
			}
		}
		if conflictIndex != -1 {
			newLog := rf.log[:conflictIndex-rf.getFirstLog().Index]
			newLog = append(newLog, args.Entries[conflictIndex-args.PrevLogIndex-1:]...)
			rf.log = newLog
		}
		rf.persist()

		commitIndex := Min(args.LeaderCommit, rf.getLastLog().Index)
		if commitIndex > rf.commitIndex {
			rf.commitIndex = commitIndex
			rf.applyCond.Signal()
		}
		reply.Term, reply.Success = rf.currentTerm, true
	}
	Debug(dClient, "S%d received Log from S%d", rf.me, args.LeaderId)
}

func (rf *Raft) matchPrevLog(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.log[index-rf.getFirstLog().Index].Term
}

func (rf *Raft) broadcastAppendEntries() {
	//Debug(dLeader, "S%d leader broadcast heartbeat", rf.me)
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.doReplicate(peer)
	}
}

func (rf *Raft) doReplicate(peer int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	if rf.needSnapshot(peer) {
		Debug(dLeader, "S%d send Snapshot to C%d, nextId[%d]<=lastInclude[%d]", rf.me, peer, rf.nextIndex[peer], rf.getFirstLog().Index)

		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}

		rf.mu.Unlock()
		rf.handleInstallSnapshot(peer, args)
	} else {
		Debug(dLeader, "S%d send AppendEntries to C%d", rf.me, peer)
		firstIndex := rf.getFirstLog().Index
		prevIndex := rf.nextIndex[peer] - 1
		prevTerm := rf.log[prevIndex-firstIndex].Term
		entries := make([]LogEntry, len(rf.log[prevIndex-firstIndex+1:]))
		copy(entries, rf.log[prevIndex-firstIndex+1:])
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevTerm,
			Entries:      entries,
			LeaderCommit: rf.commitIndex,
		}

		rf.mu.Unlock()
		rf.handleAppendEntries(peer, args)
	}
}
func (rf *Raft) isLogMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.log[index-rf.getFirstLog().Index].Term
}
func (rf *Raft) handleAppendEntries(peer int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	if !rf.sendAppendEntries(peer, args, reply) {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		Debug(dInfo, "S%d AppendEntries rejected from S%d, u r not leader", rf.me, peer)
		return
	}
	if rf.currentTerm != args.Term {
		Debug(dInfo, "S%d AppendEntries rejected from S%d, term conflict", rf.me, peer)
		return
	}
	if reply.Term > rf.currentTerm {
		Debug(dInfo, "S%d AppendEntries rejected from S%d, reply.term is higher", rf.me, peer)
		rf.convert2Follower()
		rf.persist()
		return
	}
	if reply.Success {
		//防止leader的状态在发送Rpc期间发生变化，matchIndex倒退
		possibleMatchIndex := args.PrevLogIndex + len(args.Entries)
		if possibleMatchIndex > rf.matchIndex[peer] {
			rf.matchIndex[peer] = possibleMatchIndex
		}
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1

		//figure2 leader rule 4
		//If there exists an N such that N > commitIndex,
		//a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm,
		//set commitIndex = N
		for N := rf.getLastLog().Index; N > rf.commitIndex; N-- {
			count := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.matchIndex[i] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 && rf.isTermMatched(N, rf.currentTerm) {
				rf.commitIndex = N
				break
			}
		}
		if rf.commitIndex > rf.lastApplied {
			rf.applyCond.Signal()
		}
	} else {
		if reply.Term < rf.currentTerm {
			Debug(dInfo, "S%d AppendEntries rejected from S%d, reply.Term is older, ignore", rf.me, peer)
			return
		}
		//index not match, broadcast immediately
		if reply.Term == rf.currentTerm {
			possibleIdx := 0
			//5.3
			//leader发送的PrevLogIndex在follower不存在，日志太新，直接从follower最后一条日志重发
			if reply.ConflictTerm == -1 {
				possibleIdx = reply.ConflictIndex
			} else {
				//查找冲突term的最后一条日志
				firstIdx := rf.getFirstLog().Index
				l := firstIdx
				r := args.PrevLogIndex - 1
				for l < r {
					mid := (r + l + 1) >> 1
					if rf.log[mid-firstIdx].Term <= reply.ConflictTerm {
						l = mid
					} else {
						r = mid - 1
					}
				}
				if rf.log[l-firstIdx].Term == reply.ConflictTerm {
					possibleIdx = l
				} else {
					possibleIdx = reply.ConflictIndex
				}
			}
			if possibleIdx < rf.nextIndex[peer] && possibleIdx > rf.matchIndex[peer] {
				rf.nextIndex[peer] = possibleIdx
			}
		}
	}
}

func (rf *Raft) isTermMatched(index, term int) bool {
	return index <= rf.getLastLog().Index && term == rf.log[index-rf.getFirstLog().Index].Term
}

func (rf *Raft) needSnapshot(i int) bool {
	return rf.nextIndex[i]-1 < rf.getFirstLog().Index
}
