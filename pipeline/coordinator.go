package pipeline

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"

	pq "github.com/emirpasic/gods/queues/priorityqueue"
	"github.com/emirpasic/gods/utils"
)

const (
	maxRetries     = 3
	taskTimeout    = 30 * time.Second // re-enqueue in-flight tasks that exceed this
	sweepInterval  = 5 * time.Second  // how often the timeout sweeper runs
)

type TaskInfo struct {
	FilePath     string
	Status       TaskStatus
	TaskId       int
	PhaseIdx     int       // which phase this task belongs to
	Retries      int       // how many times this task has been retried
	DispatchedAt time.Time // when it was last handed to a worker (zero = not dispatched)
	ChunkOffset  int64     // byte offset where the next map chunk begins (0 = start of file)
}

type Coordinator struct {
	mu sync.Mutex

	ProcessAction []StreamProcessAction

	// phaseIdx is the cursor into ProcessAction.
	// Phase 0 = map (tasks from data source).
	// Phase 1+ = subsequent stages (NReduce partitioned tasks each).
	// phaseIdx == len(ProcessAction) means all phases complete.
	phaseIdx int

	NReduce  int
	NumTasks int // total map tasks ever enqueued (grows as source streams)

	// phaseDone counts completed tasks in the current phase.
	phaseDone int

	// failedTasks counts tasks that exhausted their retries this phase.
	failedTasks int

	// sourceDone is set when the data source channel closes.
	sourceDone bool

	JobStatus *pq.Queue

	// inFlight tracks dispatched tasks that haven't reported back yet.
	// Key = TaskId. Used to detect crashed workers via the timeout sweeper.
	inFlight map[int]*TaskInfo

	// taskFiles maps TaskId → source FilePath, populated during the map phase
	// so reduce tasks can carry the same TaskName.
	taskFiles map[int]string

	Intermediates *IntermediateStore
}

func byPriority(a, b interface{}) int {
	priorityA := StatusPriority[a.(*TaskInfo).Status]
	priorityB := StatusPriority[b.(*TaskInfo).Status]
	return -utils.IntComparator(priorityA, priorityB)
}

var _ TaskScheduler = (*Coordinator)(nil)

func NewCoordinator(nReduce int, actions []StreamProcessAction) *Coordinator {
	return &Coordinator{
		ProcessAction: actions,
		phaseIdx:      0,
		NReduce:       nReduce,
		JobStatus:     pq.NewWith(byPriority),
		inFlight:      make(map[int]*TaskInfo),
		taskFiles:     make(map[int]string),
		Intermediates: NewIntermediateStore(nReduce),
	}
}

// AskForTask implements TaskScheduler.
func (c *Coordinator) AskForTask(req *MessageSend, reply *MessageReply) error {
	if req.MsgType != AskForTask {
		return fmt.Errorf("bad message type")
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.done() {
		reply.MsgType = Shutdown
		return nil
	}

	task, empty := c.nextJob()
	if empty {
		if c.currentPhaseComplete() {
			c.transitionToNextPhase()
			if c.done() {
				reply.MsgType = Shutdown
				return nil
			}
		}
		reply.MsgType = Wait
		return nil
	}

	// Mark in-flight so the timeout sweeper can recover it on worker crash.
	task.DispatchedAt = time.Now()
	task.PhaseIdx = c.phaseIdx
	c.inFlight[task.TaskId] = task

	reply.MsgType = TaskAlloc
	reply.TaskID = task.TaskId
	reply.TaskName = task.FilePath
	reply.BucketID = task.TaskId
	reply.NReduce = c.NReduce
	reply.ActionIndex = c.phaseIdx
	reply.PhaseIdx = c.phaseIdx
	reply.ChunkOffset = task.ChunkOffset
	return nil
}

// NoticeResult implements TaskScheduler.
func (c *Coordinator) NoticeResult(req *MessageSend, reply *MessageReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Ignore reports from a stale phase (coordinator already moved on).
	if req.PhaseIdx != c.phaseIdx {
		fmt.Printf("[coordinator] stale report for phase %d (current %d), ignoring\n",
			req.PhaseIdx, c.phaseIdx)
		return nil
	}

	task, inflight := c.inFlight[req.TaskID]
	if inflight {
		delete(c.inFlight, req.TaskID)
	}

	switch req.MsgType {
	case TaskSuccess:
		c.phaseDone++
		fmt.Printf("[coordinator] phase %d: task %d succeeded (%d/%d done)\n",
			req.PhaseIdx, req.TaskID, c.phaseDone, c.phaseTotal())

	case TaskContinue:
		if !inflight {
			return nil
		}
		task.ChunkOffset = req.NextOffset
		task.Status = UnAssigned
		task.DispatchedAt = time.Time{}
		c.JobStatus.Enqueue(task)
		fmt.Printf("[coordinator] phase %d: task %d continuing at offset %d\n",
			req.PhaseIdx, req.TaskID, req.NextOffset)

	case TaskFailed:
		if !inflight {
			// No in-flight record — duplicate or phantom report; ignore.
			return nil
		}
		task.Retries++
		if task.Retries >= maxRetries {
			c.failedTasks++
			fmt.Printf("[coordinator] phase %d: task %d exhausted %d retries — giving up\n",
				req.PhaseIdx, req.TaskID, maxRetries)
			// Count as done so the phase can still complete (with partial results).
			c.phaseDone++
		} else {
			task.Status = UnAssigned
			task.DispatchedAt = time.Time{}
			c.JobStatus.Enqueue(task)
			fmt.Printf("[coordinator] phase %d: task %d failed (retry %d/%d), re-enqueued\n",
				req.PhaseIdx, req.TaskID, task.Retries, maxRetries)
		}
	}

	return nil
}

// sweepTimedOutTasks re-enqueues any in-flight task that has exceeded taskTimeout.
// Called periodically by the background sweeper goroutine — not holding mu on entry.
func (c *Coordinator) sweepTimedOutTasks() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for id, task := range c.inFlight {
		if !task.DispatchedAt.IsZero() && now.Sub(task.DispatchedAt) > taskTimeout {
			delete(c.inFlight, id)
			task.Retries++
			if task.Retries >= maxRetries {
				c.failedTasks++
				c.phaseDone++
				fmt.Printf("[coordinator] task %d timed out and exhausted retries — giving up\n", id)
			} else {
				task.Status = UnAssigned
				task.DispatchedAt = time.Time{}
				c.JobStatus.Enqueue(task)
				fmt.Printf("[coordinator] task %d timed out (retry %d/%d), re-enqueued\n",
					id, task.Retries, maxRetries)
			}
		}
	}
}

// phaseTotal returns the expected number of completions for the current phase.
func (c *Coordinator) phaseTotal() int {
	return c.NumTasks
}

// transitionToNextPhase advances phaseIdx and enqueues tasks for the new phase.
func (c *Coordinator) transitionToNextPhase() {
	c.phaseIdx++
	c.phaseDone = 0
	c.failedTasks = 0
	c.inFlight = make(map[int]*TaskInfo)

	if c.done() {
		fmt.Println("[coordinator] all phases complete")
		return
	}

	action := c.ProcessAction[c.phaseIdx]
	fmt.Printf("[coordinator] transitioning to phase %d (%v)\n", c.phaseIdx, action.ActionType)

	switch action.ActionType {
	case MapTask:
		// tasks arrive dynamically from the data source — nothing to enqueue here
	default:
		// One reduce task per map task: workers glob mr-{TaskName}-*-* to find all chunks.
		for i := 0; i < c.NumTasks; i++ {
			c.JobStatus.Enqueue(&TaskInfo{
				TaskId:   i,
				FilePath: c.taskFiles[i],
				Status:   UnAssigned,
				PhaseIdx: c.phaseIdx,
			})
		}
	}
}

// currentPhaseComplete returns true when all tasks for the active phase have finished
// (either succeeded or exhausted retries).
func (c *Coordinator) currentPhaseComplete() bool {
	allReported := c.phaseDone == c.phaseTotal()
	noneInFlight := len(c.inFlight) == 0
	switch c.ProcessAction[c.phaseIdx].ActionType {
	case MapTask:
		return c.sourceDone && allReported && noneInFlight
	default:
		return allReported && noneInFlight
	}
}

func (c *Coordinator) done() bool {
	return c.phaseIdx >= len(c.ProcessAction)
}

func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done()
}

func (c *Coordinator) nextJob() (*TaskInfo, bool) {
	item, ok := c.JobStatus.Dequeue()
	if !ok {
		return nil, true
	}
	return item.(*TaskInfo), false
}

func (c *Coordinator) Start(msgChan <-chan string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	time.Sleep(30 * time.Millisecond)

	go c.listenFromDataSource(msgChan)
	go c.runSweeper()
}

// runSweeper periodically re-enqueues tasks whose workers have gone silent.
func (c *Coordinator) runSweeper() {
	ticker := time.NewTicker(sweepInterval)
	defer ticker.Stop()
	for range ticker.C {
		if c.Done() {
			return
		}
		c.sweepTimedOutTasks()
	}
}

func (c *Coordinator) listenFromDataSource(msgCh <-chan string) {
	fmt.Println("[coordinator] listening from data source")
	idx := 0
	for msg := range msgCh {
		c.mu.Lock()
		c.JobStatus.Enqueue(&TaskInfo{
			FilePath: msg,
			Status:   UnAssigned,
			TaskId:   idx,
			PhaseIdx: 0,
		})
		c.taskFiles[idx] = msg
		idx++
		c.NumTasks++
		c.mu.Unlock()
		fmt.Printf("[coordinator] enqueued map task %d: %s\n", idx-1, msg)
	}
	c.mu.Lock()
	c.sourceDone = true
	c.mu.Unlock()
	fmt.Println("[coordinator] data source exhausted")
}
