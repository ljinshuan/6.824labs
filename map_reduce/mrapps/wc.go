package mrapps

//
// a word-count application "plugin" for MapReduce.
//
// go build -buildmode=plugin wc.go
//

import (
	"6.824labs/map_reduce/mr"
	"unicode"
)
import "strings"
import "strconv"

type WcMapReduce struct {
}

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//

func (W *WcMapReduce) Map(filename string, contents string) []mr.KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	var kva []mr.KeyValue
	for _, w := range words {
		kv := mr.KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func (W *WcMapReduce) Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
