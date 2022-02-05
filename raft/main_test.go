package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestName(t *testing.T) {

	timeout, _ := context.WithTimeout(context.Background(), 100*time.Millisecond)
	ch := make(chan string)
	go func() {
		time.Sleep(time.Millisecond * 1)
		ch <- "jinshuan.li"
	}()
	select {
	case name := <-ch:
		fmt.Println(name)
	case <-timeout.Done():
		fmt.Println("time out")
	}

	go func() {
		time.Sleep(time.Millisecond * 1000)
		ch <- "jinshuan.li"
	}()
	select {
	case name := <-ch:
		fmt.Println(name)
	case <-timeout.Done():
		fmt.Println("time out")
	}
}
