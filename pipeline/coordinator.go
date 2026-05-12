package pipeline

type CoordinatorPhase int

const (
	MAPPING CoordinatorPhase = iota
	REDUCING
	GROUPING
	SINKING
	WAITING
)

type Coordinator struct {
	Phase    CoordinatorPhase
	listener <-chan MicroBatchMsg
}
