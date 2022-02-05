package raft

import (
	"encoding/json"
	"fmt"
)

func Println(format string, a ...interface{}) {

	args := make([]interface{}, len(a))
	for i, d := range a {
		m, _ := json.Marshal(d)
		args[i] = string(m)
	}
	fmt.Printf(format, args...)
}
