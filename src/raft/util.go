package raft

import (
	"math/rand"
	"time"
)

const (
	// 根据任务要求心跳每秒不能超过10次
	HeartbeatInterval  = 80 * time.Millisecond
	ElectionTimeoutMax = 1500
	ElectionTimeoutMin = 1000
)

var rd = rand.New(rand.NewSource(time.Now().UnixNano()))

func genElectionDuration() time.Duration {
	// milliseconds.
	return time.Duration(rd.Int63n(ElectionTimeoutMax-ElectionTimeoutMin)+ElectionTimeoutMin) * time.Millisecond
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.log[len(rf.log)-1]
}

func (rf *Raft) getFirstLog() LogEntry {
	return rf.log[0]
}
