package raft

import (
	"encoding/json"
	"fmt"
	"time"
)

func Println(format string, a ...interface{}) {

	args := make([]interface{}, len(a))
	for i, d := range a {
		m, _ := json.Marshal(d)
		args[i] = string(m)
	}
	fmt.Printf(time.Now().Format("2006-01-02 15:04:05.000 ")+format, args...)
}
