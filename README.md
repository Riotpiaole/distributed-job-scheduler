# go-flink

A distributed MapReduce pipeline engine written in Go. 

## How it works

The coordinator reads input files, splits them into 10 MB chunks, and distributes map and reduce tasks to workers over a Unix-domain RPC socket. Workers are independent processes — you can start one or a hundred on the same machine. Each loads the same plugin and registers with the coordinator automatically.

## Quick start

### 1. Build the coordinator/worker binary

```bash
go build -o go-flink .
```

### 2. Build a plugin

```bash
cd plugin
go build -buildmode=plugin -o wc.so wc.go
```

### 3. Start the coordinator

```bash
./go-flink <input-dir> wc.so -o mr-out
```

### 4. Start workers (in separate terminals or processes)

```bash
# Each worker process picks up its own PID as --id by default
./go-flink worker --plugin wc.so
./go-flink worker --plugin wc.so
./go-flink worker --plugin wc.so
```

Spin up as many workers as you need. The coordinator hands out tasks as fast as workers ask for them. When all tasks are done the coordinator sends a shutdown signal and all workers exit cleanly.

## Scaling

Throughput scales with the number of worker processes. Workers are stateless — they fetch chunk content from the coordinator via RPC (`GetChunk`), run the plugin's `Map` or `Reduce` function locally, and write intermediate files to the shared output directory. To scale:

- Add more `./go-flink worker --plugin <your>.so` processes.
- Point them all at the same output directory (`-o`).
- No other configuration is needed.

There is no upper limit enforced by the framework. The coordinator's priority queue and timeout sweeper handle stragglers and crashed workers automatically (up to 3 retries per task).

## Why ChunkQueue instead of a channel

The datasource originally streamed chunks over a Go channel. This caused a deadlock: the coordinator's `listenFromDataSource` goroutine would block on a channel receive waiting for the next chunk, while simultaneously needing to serve incoming `AskForTask` and `GetChunk` RPC calls from workers. Because the RPC server and the chunk listener shared the same goroutine scheduling path, the coordinator could not concurrently pull messages from the channel and respond to workers — one side always starved the other.

`ChunkQueue` solves this by decoupling production from consumption. The datasource goroutine pushes chunks into the queue without blocking the coordinator. The coordinator's listener goroutine polls with a short sleep when the queue is empty, leaving the RPC server free to handle worker requests at all times. The queue is closed by the producer when all chunks have been pushed; consumers check `Done()` to know when to stop polling.

## Coordinator ↔ Worker call sequence

### Map phase

```
Worker                                      Coordinator
  │                                              │
  │  ── AskForTask(MsgType=AskForTask) ────────► │
  │                                              │  dequeue next TaskInfo from priority queue
  │                                              │  mark task in-flight (DispatchedAt = now)
  │ ◄─ TaskAlloc(ChunkID, ActionIndex=0, ──────  │
  │              PhaseIdx, NReduce)              │
  │                                              │
  │  ── GetChunk(ChunkID) ─────────────────────► │
  │ ◄─ ChunkReply(Content []byte) ─────────────  │  raw bytes served from chunkStore
  │                                              │
  │  [run plugin.Map(filename, content)]         │
  │  [write mr-<chunkID>-<bucket> to disk]       │
  │                                              │
  │  ── NoticeResult(TaskSuccess, TaskID, ─────► │
  │                  PhaseIdx)                   │  phaseDone++; delete chunk from chunkStore
  │                                              │  if all map tasks done → transitionToNextPhase()
```

### Reduce phase

```
Worker                                      Coordinator
  │                                              │
  │  ── AskForTask(MsgType=AskForTask) ────────► │
  │                                              │  dequeue reduce TaskInfo (one per ChunkID)
  │ ◄─ TaskAlloc(TaskName=ChunkID, ────────────  │
  │              ActionIndex=1, PhaseIdx)        │
  │                                              │
  │  [glob mr-<ChunkID>-* from disk]             │
  │  [sort + group by key]                       │
  │  [run plugin.Reduce(key, values)]            │
  │  [write mr-out-<ChunkID> to disk]            │
  │                                              │
  │  ── NoticeResult(TaskSuccess, TaskID, ─────► │
  │                  PhaseIdx)                   │  phaseDone++
  │                                              │  if all reduce tasks done → Done() = true
```

### Task failure and retry

```
Worker                                      Coordinator
  │                                              │
  │  [task fails mid-execution]                  │
  │  ── NoticeResult(TaskFailed, TaskID, ──────► │
  │                  PhaseIdx)                   │  task.Retries++
  │                                              │  if Retries < 3 → re-enqueue task
  │                                              │  if Retries >= 3 → give up, phaseDone++
```

### Worker stall / crash (sweeper path)

```
Worker                                      Coordinator (sweeper goroutine, every 5s)
  │                                              │
  │  [worker process hangs or dies]              │
  │  [no NoticeResult arrives]                   │
  │                                              │  now - task.DispatchedAt > 30s
  │                                              │  task.Retries++
  │                                              │  re-enqueue task (or give up after 3 retries)
```

### Shutdown

```
Worker                                      Coordinator
  │                                              │
  │  ── AskForTask ────────────────────────────► │  Done() == true
  │ ◄─ Shutdown ───────────────────────────────  │
  │                                              │
  │  [worker exits]                              │
```

## Writing a plugin

A plugin is a regular Go file compiled with `-buildmode=plugin`. It must export exactly two functions:

```go
func Map(filename string, contents string) []pipeline.KeyValue
func Reduce(key string, values []string) string
```

See [plugin/wc.go](plugin/wc.go) for a complete word-count example.

Build it:
```bash
go build -buildmode=plugin -o myplugin.so myplugin.go
```

## CLI reference

```
go-flink <input-dir> <plugin.so> [-o output-dir]
    Start the coordinator and begin streaming input files.

go-flink worker --plugin <plugin.so> [--id <int>] [-o output-dir]
    Start a worker process. --id defaults to the process PID.
    Run this command in parallel across as many processes as desired.
```

## Output

Intermediate files are written as `mr-<chunkID>-<bucket>` and final results as `mr-out-<chunkID>` inside the output directory (default: `mr-out/`).

## Dependencies

- [cobra](https://github.com/spf13/cobra) — CLI
- [gods](https://github.com/emirpasic/gods) — priority queue for task scheduling
- [hashring](https://github.com/serialx/hashring) — consistent hashing for reduce partitioning
- [uuid](https://github.com/google/uuid) — chunk identity
