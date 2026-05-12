package pipeline

type InpuType int

const DEFAULT_WINDOW_SIZE = 1 * 1024 * 1024 // 1 MB
const (
	FILE_DIR InpuType = iota
	FILE_LIST
	DB_URL
	EVETN_STREAM
)

// FileDataSource handles directory ingestion
type Pipeline struct {
	Clusters      []string
	WindowSize    int
	PartitionFunc func(string) string
}

// NewPipeline creates a new instance
func NewPipeline(windowSize int, partitionFunc func(string) string) *Pipeline {
	return &Pipeline{
		Clusters:      []string{}, // This can be populated with actual cluster addresses
		WindowSize:    windowSize,
		PartitionFunc: partitionFunc,
	}
}
