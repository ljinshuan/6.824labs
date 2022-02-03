package main

import (
	"6.824labs/map_reduce/main_test/server"
	"fmt"
	"net/rpc"
)

func main() {

	http, err := rpc.DialHTTP("unix", "zhisu_test")
	if err != nil {
		return
	}
	defer http.Close()

	request := &server.Request{Msg: "jinshuan.li"}
	response := &server.Response{}
	http.Call("RpcServer.Hello", request, response)
	fmt.Printf("%v\n", response)

	http.Call("RpcServer.Hello", request, response)
	fmt.Printf("%v\n", response)
}
