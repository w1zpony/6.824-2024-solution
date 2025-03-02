package mr

import (
	"fmt"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "io"
import "encoding/json"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := MessageArgs{}
		replay := MessageReply{}
		ok := call("Coordinator.AllocateTask", &args, &replay)
		if !ok {
			//如果Coordinator宕机，退出
			break
		}
		switch replay.TaskPhase {
		case MapPhase:
			doMap(mapf, &replay)
		case ReducePhase:
			doReduce(reducef, &replay)
		case Wait:
			time.Sleep(time.Second)
		case AllDone:
			return
		default:
			time.Sleep(time.Second)
		}
	}

}

//map 任务
//1.读取传入的文件
//2.调用mapf函数，返回键值对
//3.调用ihash(key),将相同key的值分配到不同的reduce中间文件中
//4.写入文件mr-X-Y
//5.调用Coordinator.ReportTask，报告任务完成
func doMap(mapf func(string, string) []KeyValue, reply *MessageReply) {
	file, err := os.Open(reply.TaskFile)
	if err != nil {
		//log.Printf("cannot open %v: %v", reply.TaskFile, err)
		sendReport(Failure, reply)
		return
	}
	content, err := io.ReadAll(file)
	if err != nil {
		//log.Printf("cannot read %v: %v", reply.TaskFile, err)
		sendReport(Failure, reply)
		return
	}
	file.Close()

	outFiles := make([]*os.File, reply.NReduce)
	jsonEncoders := make([]*json.Encoder, reply.NReduce)
	kva := mapf(reply.TaskFile, string(content))
	for _, kv := range kva {
		reduceID := ihash(kv.Key) % reply.NReduce
		if jsonEncoders[reduceID] == nil {
			outFiles[reduceID], err = os.CreateTemp("", "mr-map-temp")
			if err != nil {
				//	log.Printf("cannot create temp file %v: %v", outFiles[reduceID], err)
				sendReport(Failure, reply)
				return
			}
			jsonEncoders[reduceID] = json.NewEncoder(outFiles[reduceID])
		}
		if err := jsonEncoders[reduceID].Encode(&kv); err != nil {
			//log.Printf("cannot encode key-value pair %v: %v", kv, err)
			sendReport(Failure, reply)
			return
		}
	}

	for i, file := range outFiles {
		if file != nil {
			file.Close()
			outFileName := fmt.Sprintf("mr-%d-%d", reply.TaskID, i)
			if err := os.Rename(file.Name(), outFileName); err != nil {
				//	log.Printf("cannot rename temp file %v: %v", file.Name(), err)
				sendReport(Failure, reply)
				return
			}
		}
	}
	sendReport(Success, reply)
}

//reduce 任务
//1.读取mr-X-Y文件，解析键值对
//2.排序
//3.遍历键值对，将相同key的值收集到values中，调用reducef函数，返回结果
//4.写入文件mr-out-X
//5.调用Coordinator.ReportTask，报告任务完成
func doReduce(reducef func(string, []string) string, reply *MessageReply) {
	var kva []KeyValue
	var mapFiles []string
	for i := 0; i < reply.NMap; i++ {
		mapFiles = append(mapFiles, fmt.Sprintf("mr-%d-%d", i, reply.TaskID))
	}

	for _, filename := range mapFiles {
		file, err := os.Open(filename)
		if err != nil {
			//log.Printf("cannot open %v: %v", filename, err)
			continue
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				//	log.Printf("cannot decode key-value pair %v: %v", kv, err)
				sendReport(Failure, reply)
				return
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	outFile, err := os.CreateTemp("", "mr-reduce-temp")
	if err != nil {
		//log.Printf("cannot create temp file %v: %v", outFile, err)
		sendReport(Failure, reply)
		return
	}
	for i := 0; i < len(kva); {
		var values []string
		var j = i
		for j = i; j < len(kva) && kva[j].Key == kva[i].Key; j++ {
			values = append(values, kva[j].Value)
		}
		result := reducef(kva[i].Key, values)
		fmt.Fprintf(outFile, "%v %v\n", kva[i].Key, result)
		i = j
	}
	outFile.Close()
	outFileName := fmt.Sprintf("mr-out-%d", reply.TaskID)
	if err := os.Rename(outFile.Name(), outFileName); err != nil {
		//	log.Printf("cannot rename temp file %v: %v", outFile.Name(), err)
		sendReport(Failure, reply)
		return
	}
	sendReport(Success, reply)
}

func sendReport(status TaskResult, reply *MessageReply) {
	call("Coordinator.ReportTask", &MessageArgs{
		TaskID:     reply.TaskID,
		TaskStatus: status,
	}, &MessageReply{})
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
