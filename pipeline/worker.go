package pipeline

type workerRole int

const (
	MAPPER workerRole = iota
	REDUCER
)

type Worker struct {
	ID   int
	Role workerRole
}
