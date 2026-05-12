package pipeline

// apache flink similar API to process streaming data,
// we can implement a simple version of it for our log processing system
type StreamProcess interface {
	Filter(validateFunc func(string) bool) StreamProcess
	Map(mapFunc func(string) string) StreamProcess
	Reduce(reduceFunc func(string, string) string) StreamProcess
	GroupBy(groupFunc func(string) string) StreamProcess
	Sink(sinkFunc func(string) error) error
}

type StreamListener interface {
	Listen(source <-chan string)
	ListenRawBytes(source <-chan []byte)

	// implementing more interface IE s3 or more
}

// ds = DataPipeline(SOURCE, WINDOW_SIZE, PARTITION_FUNC)
// ds = pipeline.Filter()
// So this define what the worker have to do each stage, and coordinater assign them to it
// This is a high level API
// And from there we proceed to lower level API, which is more close to the worker implementation,
// and coordinater will assign them to it
// Pipeline -> Coordinator -> Worker
//  Message received upon channel
// > ds.Listen(SOME_URL_SOURCE)
