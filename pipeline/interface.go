package pipeline

/**

func main() {
	// Create the list of functions
	funcList := []StreamProcessAction{
		// Function 1: Expects (int, int), returns (sum int, product int)
		func(args ...any) []any {
			if len(args) < 2 {
				return []any{0, 0}
			}
			a, ok1 := args[0].(int)
			b, ok2 := args[1].(int)
			if !ok1 || !ok2 {
				return []any{0, 0}
			}
			return []any{a + b, a * b}
		},

		// Function 2: Expects (string, int), returns (repeated string)
		func(args ...any) []any {
			if len(args) < 2 {
				return []any{""}
			}
			str, ok1 := args[0].(string)
			count, ok2 := args[1].(int)
			if !ok1 || !ok2 {
				return []any{""}
			}
			result := ""
			for i := 0; i < count; i++ {
				result += str
			}
			return []any{result}
		},
	}

	// Execute the math function
	mathResults := funcList[0](10, 5)
	fmt.Printf("Math Outputs: Sum = %v, Product = %v\n", mathResults[0], mathResults[1])

	// Execute the string function
	strResults := funcList[1]("Go", 3)
	fmt.Printf("String Output: %v\n", strResults[0])
}
**/

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	SelectKeyTask
	FilterTask
	GroupByTask
	SinkTask
)

// StreamProcessAction describes one stage in a pipeline.
// Name is the plugin filename stem (e.g. "wc"); ActionType determines
// how the coordinator partitions and routes tasks for this stage.
type StreamProcessAction struct {
	Name       string
	ActionType TaskType
}

