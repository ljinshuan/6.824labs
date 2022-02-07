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
func TestSubSlic(t *testing.T) {

	abc := []int{1, 2, 3, 4, 5, 6}
	fmt.Println(abc)
	a := abc[0:2]
	fmt.Println(a)
}
