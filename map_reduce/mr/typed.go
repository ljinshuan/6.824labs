package mr

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapReduce interface {
	Map(filename string, contents string) []KeyValue
	Reduce(key string, values []string) string
}
