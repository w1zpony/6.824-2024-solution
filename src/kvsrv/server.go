package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	db map[string]string

	historyMap map[int64]History
}

type History struct {
	SeqNum int64
	Value  string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.historyMap[args.ClientId]; ok {
		delete(kv.historyMap, args.ClientId)
	}
	reply.Value = kv.db[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	history := kv.historyMap[args.ClientId]
	if args.SeqNum > history.SeqNum {
		delete(kv.historyMap, args.ClientId)
		kv.db[args.Key] = args.Value
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	history := kv.historyMap[args.ClientId]

	if args.SeqNum <= history.SeqNum {
		reply.Value = history.Value
	} else {
		oldValue := kv.db[args.Key]
		kv.db[args.Key] = oldValue + args.Value
		kv.historyMap[args.ClientId] = History{args.SeqNum, oldValue}
		reply.Value = oldValue
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.db = make(map[string]string)
	kv.historyMap = make(map[int64]History)

	return kv
}
