package pipeline

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/serialx/hashring"
)

var errContinue = errors.New("task continuation in progress")

const chunkSize = 5 * 1024 * 1024 // 100 MB

type Worker struct {
	ID          int
	actions     []StreamProcessAction
	outputDir   string
	activeReply *MessageReply // set by invoke before calling Map/Reduce; safe — one task at a time per goroutine
	lastErr     error         // set by Map/Reduce when they report a failure to the coordinator
}

var _ TaskProcessor = (*Worker)(nil)
var _ StreamProcess = (*Worker)(nil)

func (w *Worker) CallForTask() *MessageReply {
	args := MessageSend{MsgType: AskForTask}
	reply := MessageReply{}
	if call("Coordinator.AskForTask", &args, &reply) {
		return &reply
	}
	return nil
}

func (w *Worker) CallForStatusReport(status MsgType, taskId int, taskName string, phaseIdx int) bool {
	args := MessageSend{
		MsgType:  status,
		TaskID:   taskId,
		TaskName: taskName,
		PhaseIdx: phaseIdx,
	}
	return call("Coordinator.NoticeResult", &args, &MessageReply{})
}

// StartWorker runs a worker goroutine. The coordinator tells it which action to
// call via ActionIndex — the worker has no hardcoded knowledge of any stage.
func StartWorker(id int, actions []StreamProcessAction, outputDir string) {
	w := &Worker{ID: id, actions: actions, outputDir: outputDir}

	for {
		reply := w.CallForTask()
		if reply == nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		switch reply.MsgType {
		case Wait:
			time.Sleep(200 * time.Millisecond)
		case Shutdown:
			fmt.Printf("[worker %d] shutting down\n", w.ID)
			return
		default:
			// Map/Reduce report TaskFailed directly on error; only send TaskSuccess on clean exit.
			if w.invoke(reply) == nil {
				w.CallForStatusReport(TaskSuccess, reply.TaskID, reply.TaskName, reply.PhaseIdx)
			}
		}
	}
}

// invoke sets the active task context and dispatches to Map or Reduce.
// Each method reports TaskFailed to the coordinator on error and sets w.lastErr.
// invoke returns w.lastErr so StartWorker knows whether to send TaskSuccess.
func (w *Worker) invoke(reply *MessageReply) error {
	w.activeReply = reply
	w.lastErr = nil
	defer func() { w.activeReply = nil }()

	task := w.actions[reply.ActionIndex]
	switch task.ActionType {
	case MapTask:
		w.Map(task)
	case ReduceTask:
		w.Reduce(task)
	}
	return w.lastErr
}

// Map implements StreamProcess.
// On error it notifies the coordinator of TaskFailed and sets w.lastErr.
// When a chunk is complete but more file data remains, it sends TaskContinue and
// sets w.lastErr = errContinue so invoke skips the TaskSuccess call.
func (w *Worker) Map(mapFunc StreamProcessAction) []KeyValue {
	kvs, err := w.mapErr(mapFunc)
	if err != nil && !errors.Is(err, errContinue) {
		w.lastErr = err
		reply := w.activeReply
		fmt.Printf("[worker %d] map task %d failed: %v\n", w.ID, reply.TaskID, err)
		w.CallForStatusReport(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
	} else if errors.Is(err, errContinue) {
		w.lastErr = errContinue
	}
	return kvs
}

func (w *Worker) mapErr(mapFunc StreamProcessAction) ([]KeyValue, error) {
	reply := w.activeReply

	f, err := os.Open(reply.TaskName)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if reply.ChunkOffset > 0 {
		if _, err := f.Seek(reply.ChunkOffset, io.SeekStart); err != nil {
			return nil, err
		}
	}

	buf := make([]byte, chunkSize)
	n, readErr := io.ReadFull(f, buf)
	if readErr != nil && readErr != io.ErrUnexpectedEOF && readErr != io.EOF {
		return nil, readErr
	}
	data := buf[:n]
	// ReadFull returns nil only when it filled the buffer — meaning there may be more.
	hasMore := readErr == nil

	ring := buildRing(reply.NReduce)
	taskBase := filepath.Base(reply.TaskName)
	offset := reply.ChunkOffset

	checkpointGlob := filepath.Join(w.outputDir, fmt.Sprintf("mr-%s-%d-*", taskBase, offset))
	existing, _ := filepath.Glob(checkpointGlob)
	if len(existing) > 0 {
		fmt.Printf("[worker %d] map task %s offset %d: checkpoint found, skipping\n",
			w.ID, taskBase, offset)
		if hasMore {
			w.callForContinuation(offset+int64(n), reply.TaskID, reply.TaskName, reply.PhaseIdx)
			return nil, errContinue
		}
		return nil, nil
	}

	result := mapFunc.Action(reply.TaskName, string(data))
	kvs, ok := result.([]KeyValue)
	if !ok {
		return nil, fmt.Errorf("map action must return []KeyValue, got %T", result)
	}
	bucketStr, _ := ring.GetNode(strconv.FormatInt(offset, 10))
	path := filepath.Join(w.outputDir, fmt.Sprintf("mr-%s-%d-%s", taskBase, offset, bucketStr))
	out, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	enc := json.NewEncoder(out)
	for _, kv := range kvs {
		if err := enc.Encode(kv); err != nil {
			out.Close()
			return nil, err
		}
	}
	out.Close()
	fmt.Printf("[worker %d] map task %s offset %d → %s (%d kv pairs)\n",
		w.ID, taskBase, offset, path, len(kvs))

	if hasMore {
		w.callForContinuation(offset+int64(n), reply.TaskID, reply.TaskName, reply.PhaseIdx)
		return kvs, errContinue
	}
	return kvs, nil
}

func (w *Worker) callForContinuation(nextOffset int64, taskId int, taskName string, phaseIdx int) bool {
	args := MessageSend{
		MsgType:    TaskContinue,
		TaskID:     taskId,
		TaskName:   taskName,
		PhaseIdx:   phaseIdx,
		NextOffset: nextOffset,
	}
	return call("Coordinator.NoticeResult", &args, &MessageReply{})
}

// Reduce implements StreamProcess.
// It reads all mr-*-*-{BucketID} files, sorts and groups by key, applies reduceFunc
// to each group, and writes the output to mr-out-{BucketID}.
// On error it notifies the coordinator of TaskFailed and sets w.lastErr.
func (w *Worker) Reduce(reduceFunc StreamProcessAction) any {
	out, err := w.reduceErr(reduceFunc)
	if err != nil {
		w.lastErr = err
		reply := w.activeReply
		fmt.Printf("[worker %d] reduce bucket %d failed: %v\n", w.ID, reply.BucketID, err)
		w.CallForStatusReport(TaskFailed, reply.TaskID, reply.TaskName, reply.PhaseIdx)
	}
	return out
}

func (w *Worker) reduceErr(reduceFunc StreamProcessAction) ([]KeyValue, error) {
	reply := w.activeReply
	taskBase := filepath.Base(reply.TaskName)
	outPath := filepath.Join(w.outputDir, fmt.Sprintf("mr-out-%s", taskBase))

	// Checkpoint: output file already exists means this task completed.
	if _, err := os.Stat(outPath); err == nil {
		fmt.Printf("[worker %d] reduce task %s: checkpoint found, skipping\n", w.ID, taskBase)
		return nil, nil
	}

	// Stream-decode all intermediate KV pairs produced by this map task.
	pattern := filepath.Join(w.outputDir, fmt.Sprintf("mr-%s-*-*", taskBase))
	files, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}

	var intermediate []KeyValue
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return nil, err
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Slice(intermediate, func(i, j int) bool { return intermediate[i].Key < intermediate[j].Key })

	ofile, err := os.Create(outPath)
	if err != nil {
		return nil, err
	}
	defer ofile.Close()

	var out []KeyValue
	for i := 0; i < len(intermediate); {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := make([]string, j-i)
		for k := i; k < j; k++ {
			values[k-i], _ = intermediate[k].Value.(string)
		}
		reduced := reduceFunc.Action(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, reduced)
		out = append(out, KeyValue{Key: intermediate[i].Key, Value: reduced})
		i = j
	}

	fmt.Printf("[worker %d] reduce task %s → %s (%d results)\n", w.ID, taskBase, outPath, len(out))
	return out, nil
}

// SelectKey implements StreamProcess.
func (w *Worker) SelectKey(groupFunc StreamProcessAction) any {
	panic("unimplemented")
}

// Sink implements StreamProcess.
func (w *Worker) Sink(sinkFunc StreamProcessAction) error {
	panic("unimplemented")
}

// buildRing creates a consistent hash ring over nReduce bucket node names.
func buildRing(nReduce int) *hashring.HashRing {
	nodes := make([]string, nReduce)
	for i := range nodes {
		nodes[i] = strconv.Itoa(i)
	}
	return hashring.New(nodes)
}

func call(rpcname string, args interface{}, reply interface{}) bool {
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
