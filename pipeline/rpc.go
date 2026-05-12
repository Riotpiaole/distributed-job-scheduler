package pipeline

type MicroBatchMsg struct {
	BatchID int
	Data    string
}

type AskForTask struct {
}

type ReportTaskCompleted struct {
}
