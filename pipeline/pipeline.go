package pipeline

import (
	"context"
	"time"

	"riotpiaole.com/vec_db_pipeline/pipeline/datasource"
)

type InpuType int

const DEFAULT_WINDOW_SIZE = 1 * 1024 * 1024 // 1 MB
const (
	FILE_DIR InpuType = iota
	FILE_LIST
	DB_URL
	EVETN_STREAM
)

var _ StreamProcess = (*Pipeline)(nil)
var _ StreamListener = (*Pipeline)(nil)

// FileDataSource handles directory ingestion
type Pipeline struct {
	Sources       datasource.DataSource
	Actions       []StreamProcessAction
	WindowSize    int
	PartitionFunc func(string) string
}

// Listen implements StreamListener.
func (p *Pipeline) Listen(source <-chan string) {
	panic("unimplemented")
}

// ListenRawBytes implements StreamListener.
func (p *Pipeline) ListenRawBytes(source <-chan []byte) {
	panic("unimplemented")
}

// Filter implements StreamProcess.
func (p *Pipeline) Filter(validateFunc StreamProcessAction) StreamProcess {
	panic("unimplemented")
}

// GroupBy implements StreamProcess.
func (p *Pipeline) GroupBy(groupFunc StreamProcessAction) StreamProcess {
	panic("unimplemented")
}

// Map implements StreamProcess.
func (p *Pipeline) Map(mapFunc StreamProcessAction) StreamProcess {
	panic("unimplemented")
}

// Reduce implements StreamProcess.
func (p *Pipeline) Reduce(reduceFunc StreamProcessAction) StreamProcess {
	panic("unimplemented")
}

// Sequential implements StreamProcess.
func (p *Pipeline) Sequential(aggergateFunc StreamProcessAction) StreamProcess {
	panic("unimplemented")
}

// Sink implements StreamProcess.
func (p *Pipeline) Sink(sinkFunc StreamProcessAction) error {
	panic("unimplemented")
}

// Sequential implements StreamProcess.

// NewPipeline creates a new instance
func NewPipeline(source datasource.DataSource, windowSize int, partitionFunc func(string) string) *Pipeline {
	return &Pipeline{
		Sources:       source,
		Actions:       []StreamProcessAction{}, // This can be populated with actual cluster addresses
		WindowSize:    windowSize,
		PartitionFunc: partitionFunc,
	}
}

func (p *Pipeline) Start() {

	ctx := context.Background()

	msgCh := p.Sources.Stream(ctx)
	coordinator := NewCoordinator(p.Actions)

	go coordinator.StartsServer(msgCh)

	for coordinator.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
