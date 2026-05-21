package pipeline

type TaskStatus int

const (
	Idle TaskStatus = iota
	UnAssigned

	MapAlloc
	Mapping
	MapComplete
	MapFailed

	ReduceAlloc
	Reducing
	ReduceComplete
	ReduceFailed

	FilterAlloc
	Filtering
	Filtered
	FilterFailed

	GroupByAlloc
	Grouping
	Grouped
	GroupByFailed

	SinkAlloc
	Sinking
	SinkComplete
	SinkFailed
)

var StatusPriority = map[TaskStatus]int{
	UnAssigned: 0,

	MapAlloc:    1,
	Mapping:     2,
	MapComplete: 3,
	MapFailed:   4,

	ReduceAlloc:    1,
	Reducing:       2,
	ReduceComplete: 3,
	ReduceFailed:   4,

	FilterAlloc:  1,
	Filtering:    2,
	Filtered:     3,
	FilterFailed: 4,

	GroupByAlloc:  1,
	Grouping:      2,
	Grouped:       3,
	GroupByFailed: 4,

	SinkAlloc:    1,
	Sinking:      2,
	SinkComplete: 3,
	SinkFailed:   4,
}
