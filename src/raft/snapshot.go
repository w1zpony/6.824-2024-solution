package raft

import (
	"time"
)

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	//	Offset            int
	Data []byte
	//	Done              bool
}

type InstallSnapshotReply struct {
	Term int
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the debug through (and including)
// that index. Raft should now trim its debug as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.getFirstLog().Index || index > rf.commitIndex {
		Debug(dInfo, "S%d reject Snapshot,index=%d, commitIndex=%d, lastIncludedIndex=%d", rf.me, rf.commitIndex, rf.log[0].Index)
		return
	}

	snapshotIndex := rf.getFirstLog().Index
	var newLog = []LogEntry{{Term: rf.log[index-snapshotIndex].Term, Index: index}} // 裁剪后依然log索引0处用一个占位entry，不实际使用
	newLog = append(newLog, rf.log[index-snapshotIndex+1:]...)                      // 这样可以避免原log底层数组由于有部分在被引用而无法将剪掉的部分GC（真正释放）
	rf.log = newLog
	rf.lastApplied = Max(index, rf.lastApplied)
	rf.snapshot = snapshot
	rf.lastIncludedTerm = rf.getLog(index).Term
	rf.lastIncludedIndex = index
	rf.lastApplied = Max(index, rf.lastApplied)
	rf.persist()
	Debug(dSnap, "S%d new snapshot is created", rf.me)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	Debug(dSnap, "S%d receive Snapshot from S%d", rf.me, args.LeaderId)
	now := time.Now()
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//leader 落后，不安装快照
	// reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		Debug(dSnap, "S%d reject snapshot, args.Term[%d]<rf.Term[%d]", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.voteFor = args.Term, -1
		rf.persist()
	}
	rf.convert2Follower()

	rf.lastAppendTime = now

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		Debug(dSnap, "S%d reject snapshot, args.LastIndex[%d] <= rf.lastIncluded[%d]", rf.me, args.LastIncludedIndex, rf.getFirstLog().Index)
		reply.Term = rf.currentTerm
		return
	}
	//log suffix
	newLog := []LogEntry{{
		Term:  args.LastIncludedTerm,
		Index: args.LastIncludedIndex,
	}}
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].Index == args.LastIncludedIndex && rf.log[i].Term == args.LastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
	rf.snapshot = args.Data

	rf.commitIndex = Max(rf.commitIndex, args.LastIncludedIndex)
	rf.needApplySnapshot = true
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.persist()
	rf.applyCond.Broadcast()
	reply.Term = rf.currentTerm
	Debug(dSnap, "S%d has load Snapshot from S%d", rf.me, args.LeaderId)
}

func (rf *Raft) handleInstallSnapshot(peer int, args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{}
	if rf.sendInstallSnapshot(peer, args, reply) {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Leader || rf.currentTerm != args.Term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.convert2Follower()
			rf.currentTerm = reply.Term
			rf.persist()
		} else {
			nextIdx := Max(args.LastIncludedIndex+1, rf.nextIndex[peer])
			rf.nextIndex[peer] = nextIdx
			rf.matchIndex[peer] = nextIdx - 1
		}
	}

}
