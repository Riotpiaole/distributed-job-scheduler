package pipeline

import (
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// pipeSink implements raft.SnapshotSink over an io.Pipe so snapshot bytes
// can be round-tripped through Restore in tests without touching disk.
type pipeSink struct{ *io.PipeWriter }

func (p *pipeSink) ID() string      { return "test-snapshot" }
func (p *pipeSink) Cancel() error   { return p.PipeWriter.CloseWithError(fmt.Errorf("cancelled")) }

// newPipeReadWriter returns a ReadCloser (for Restore) and a SnapshotSink (for Persist).
func newPipeReadWriter() (io.ReadCloser, raft.SnapshotSink) {
	pr, pw := io.Pipe()
	return pr, &pipeSink{pw}
}

// applyCmd is a test helper that wraps a RaftCommand into a raft.Log and
// calls Apply directly on the FSM (no real Raft cluster needed).
func applyCmd(t *testing.T, c *Coordinator, cmd RaftCommand) {
	t.Helper()
	data, err := json.Marshal(cmd)
	if err != nil {
		t.Fatalf("marshal command: %v", err)
	}
	result := c.Apply(&raft.Log{Data: data})
	if result != nil {
		t.Fatalf("Apply returned error: %v", result)
	}
}

func TestApply_EnqueueTask(t *testing.T) {
	c := NewCoordinator(4, []StreamProcessAction{{Name: "wc", ActionType: MapTask}})

	task := &TaskInfo{
		TaskId:     0,
		ChunkID:    "chunk-abc",
		FileName:   "test.txt",
		Status:     UnAssigned,
		PluginName: "wc",
	}
	applyCmd(t, c, RaftCommand{Type: CmdEnqueueTask, Task: task})

	if c.NumTasks != 1 {
		t.Fatalf("expected NumTasks=1, got %d", c.NumTasks)
	}
	if c.taskFiles[0] != "chunk-abc" {
		t.Fatalf("expected taskFiles[0]=chunk-abc, got %q", c.taskFiles[0])
	}
	if _, inQueue := c.JobStatus.Dequeue(); !inQueue {
		t.Fatal("expected task in JobStatus queue")
	}
}

func TestApply_CompleteTask(t *testing.T) {
	c := NewCoordinator(4, []StreamProcessAction{{Name: "wc", ActionType: MapTask}})

	// Put a task in-flight manually.
	c.mu.Lock()
	c.inFlight[7] = &TaskInfo{TaskId: 7, ChunkID: "chunk-xyz"}
	c.mu.Unlock()

	applyCmd(t, c, RaftCommand{Type: CmdCompleteTask, TaskID: 7, ChunkID: "chunk-xyz"})

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inFlight[7]; ok {
		t.Fatal("task 7 should have been removed from inFlight")
	}
	if c.phaseDone != 1 {
		t.Fatalf("expected phaseDone=1, got %d", c.phaseDone)
	}
	if _, ok := c.chunkStore["chunk-xyz"]; ok {
		t.Fatal("chunk-xyz should have been evicted from chunkStore")
	}
}

func TestApply_FailTask_Retry(t *testing.T) {
	c := NewCoordinator(4, []StreamProcessAction{{Name: "wc", ActionType: MapTask}})

	c.mu.Lock()
	c.inFlight[3] = &TaskInfo{TaskId: 3, ChunkID: "chunk-3", Retries: 0}
	c.mu.Unlock()

	applyCmd(t, c, RaftCommand{Type: CmdFailTask, TaskID: 3})

	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.inFlight[3]; ok {
		t.Fatal("task 3 should have left inFlight after failure")
	}
	// Should be re-queued (retry < maxRetries).
	if c.JobStatus.Empty() {
		t.Fatal("expected failed task to be re-enqueued for retry")
	}
	if c.phaseDone != 0 {
		t.Fatal("phaseDone should not increment on retryable failure")
	}
}

func TestApply_FailTask_Exhausted(t *testing.T) {
	c := NewCoordinator(4, []StreamProcessAction{{Name: "wc", ActionType: MapTask}})

	c.mu.Lock()
	// Retries already at maxRetries-1 so next failure exhausts it.
	c.inFlight[5] = &TaskInfo{TaskId: 5, ChunkID: "chunk-5", Retries: maxRetries - 1}
	c.mu.Unlock()

	applyCmd(t, c, RaftCommand{Type: CmdFailTask, TaskID: 5})

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.failedTasks != 1 {
		t.Fatalf("expected failedTasks=1, got %d", c.failedTasks)
	}
	if c.phaseDone != 1 {
		t.Fatalf("expected phaseDone=1, got %d", c.phaseDone)
	}
}

func TestApply_AdvancePhase(t *testing.T) {
	c := NewCoordinator(4, []StreamProcessAction{
		{Name: "wc", ActionType: MapTask},
		{Name: "wc", ActionType: ReduceTask},
	})

	c.mu.Lock()
	c.phaseDone = 3
	c.inFlight[1] = &TaskInfo{TaskId: 1}
	c.mu.Unlock()

	applyCmd(t, c, RaftCommand{Type: CmdAdvancePhase})

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.phaseIdx != 1 {
		t.Fatalf("expected phaseIdx=1, got %d", c.phaseIdx)
	}
	if c.phaseDone != 0 {
		t.Fatalf("expected phaseDone reset to 0, got %d", c.phaseDone)
	}
	if len(c.inFlight) != 0 {
		t.Fatal("expected inFlight cleared after AdvancePhase")
	}
}

func TestSnapshotRestore(t *testing.T) {
	c := NewCoordinator(2, []StreamProcessAction{{Name: "wc", ActionType: MapTask}})

	// Prime some state.
	c.mu.Lock()
	c.phaseIdx = 1
	c.phaseDone = 2
	c.NumTasks = 3
	c.sourceDone = true
	c.taskFiles[0] = "chunk-0"
	c.taskFiles[1] = "chunk-1"
	c.taskFileNames[0] = "file0.txt"
	c.taskFileNames[1] = "file1.txt"
	c.inFlight[0] = &TaskInfo{TaskId: 0, ChunkID: "chunk-0", DispatchedAt: time.Now()}
	c.JobStatus.Enqueue(&TaskInfo{TaskId: 2, ChunkID: "chunk-2", Status: UnAssigned})
	c.mu.Unlock()

	// Take snapshot.
	snap, err := c.Snapshot()
	if err != nil {
		t.Fatalf("Snapshot: %v", err)
	}

	// Restore into a fresh coordinator.
	c2 := NewCoordinator(2, []StreamProcessAction{{Name: "wc", ActionType: MapTask}})
	pr, pw := newPipeReadWriter()
	go func() {
		snap.Persist(pw)
	}()
	if err := c2.Restore(pr); err != nil {
		t.Fatalf("Restore: %v", err)
	}

	c2.mu.Lock()
	defer c2.mu.Unlock()
	if c2.phaseIdx != 1 {
		t.Fatalf("phaseIdx: want 1, got %d", c2.phaseIdx)
	}
	if c2.phaseDone != 2 {
		t.Fatalf("phaseDone: want 2, got %d", c2.phaseDone)
	}
	if c2.NumTasks != 3 {
		t.Fatalf("NumTasks: want 3, got %d", c2.NumTasks)
	}
	if !c2.sourceDone {
		t.Fatal("sourceDone should be true")
	}
	if c2.taskFiles[0] != "chunk-0" {
		t.Fatalf("taskFiles[0]: want chunk-0, got %q", c2.taskFiles[0])
	}
	if _, ok := c2.inFlight[0]; !ok {
		t.Fatal("inFlight[0] should be restored")
	}
	if c2.JobStatus.Empty() {
		t.Fatal("queue should have task 2 after restore")
	}
}
