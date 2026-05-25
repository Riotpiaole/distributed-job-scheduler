package pipeline

// StageSpec describes one stage in a pipeline execution graph.
type StageSpec struct {
	Type       TaskType
	PluginName string
}

// SourceConfig describes a data source generically so it can be serialized
// into a JobSpec and sent to a remote cluster.
type SourceConfig struct {
	Type   string            // "file" | "s3" | "kafka"
	Config map[string]string // source-specific params (e.g. {"path": "./datasets"})
}

// JobSpec is the complete description of a pipeline job submitted to a cluster.
type JobSpec struct {
	JobID     string
	Source    SourceConfig
	Stages    []StageSpec
	OutputDir string
	NReduce   int
}

// JobReply is the coordinator's response to a SubmitJob RPC.
type JobReply struct {
	JobID  string
	Status string // "accepted" | "running" | "done" | "error"
	Error  string
}
